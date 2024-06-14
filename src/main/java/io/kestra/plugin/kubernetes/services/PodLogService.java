package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PodLogService implements AutoCloseable {
    private final ThreadMainFactoryBuilder threadFactoryBuilder;
    private List<LogWatch> podLogs = new ArrayList<>();
    private ScheduledExecutorService scheduledExecutor;
    @Getter
    private LoggingOutputStream outputStream;
    private Thread thread;

    public PodLogService(ThreadMainFactoryBuilder threadFactoryBuilder) {
        this.threadFactoryBuilder = threadFactoryBuilder;
    }

    public final void watch(KubernetesClient client, Pod pod, AbstractLogConsumer logConsumer, RunContext runContext) {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threadFactoryBuilder.build("k8s-log"));
        outputStream = new LoggingOutputStream(logConsumer);
        AtomicBoolean started = new AtomicBoolean(false);

        ScheduledFuture<?> scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
            () -> {
                Instant lastTimestamp = outputStream.getLastTimestamp() == null ? null : Instant.from(outputStream.getLastTimestamp());

                if (!started.get() || lastTimestamp == null || lastTimestamp.isBefore(Instant.now().minus(Duration.ofMinutes(10)))) {
                    if (!started.get()) {
                        started.set(true);
                    } else {
                        runContext.logger().trace("No log since '{}', reconnecting", lastTimestamp == null ? "unknown" : lastTimestamp.toString());
                    }

                    if (podLogs != null) {
                        podLogs.forEach(LogWatch::close);
                        podLogs = new ArrayList<>();
                    }

                    PodResource podResource = PodService.podRef(client, pod);

                    pod
                        .getSpec()
                        .getContainers()
                        .forEach(container -> {
                            podLogs.add(podResource
                                .inContainer(container.getName())
                                .usingTimestamps()
                                .sinceTime(lastTimestamp != null ?
                                    lastTimestamp.plusSeconds(1).toString() :
                                    null
                                )
                                .watchLog(outputStream)
                            );
                        });
                }
            },
            0,
            30,
            TimeUnit.SECONDS
        );

        // look at exception on the main thread
        thread = Thread.ofVirtual().name("k8s-listener").start(
            () -> {
                try {
                    Await.until(scheduledFuture::isDone);
                } catch (RuntimeException e) {
                    if (!e.getMessage().contains("Can't sleep")) {
                        log.error(this.getClass().getName() + " exception", e);
                    } else {
                        log.debug(this.getClass().getName() + " exception", e);
                    }
                }

                try {
                    scheduledFuture.get();
                } catch (ExecutionException | InterruptedException e) {
                    log.error(this.getClass().getName() + " exception", e);
                }
            }
        );
    }

    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }

        if (thread != null) {
            thread.interrupt();
            thread = null;
        }

        if (podLogs != null) {
            podLogs.forEach(LogWatch::close);
        }

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }
}
