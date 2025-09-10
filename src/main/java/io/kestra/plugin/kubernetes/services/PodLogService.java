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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PodLogService implements AutoCloseable {
    private final ThreadMainFactoryBuilder threadFactoryBuilder;
    private List<LogWatch> podLogs = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledFuture;
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

        scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
            () -> {
                Instant lastTimestamp = outputStream.getLastTimestamp() == null ? null : Instant.from(outputStream.getLastTimestamp());

                if (!started.get() || lastTimestamp == null || lastTimestamp.isBefore(Instant.now().minus(Duration.ofMinutes(10)))) {
                    if (!started.get()) {
                        started.set(true);
                    } else {
                        runContext.logger().trace("No log since '{}', reconnecting", lastTimestamp == null ? "unknown" : lastTimestamp.toString());
                    }

                    podLogs.forEach(LogWatch::close);
                    podLogs.clear();

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
                        log.error("{} exception", this.getClass().getName(), e);
                    } else {
                        log.debug("{} exception", this.getClass().getName(), e);
                    }
                }

                try {
                    scheduledFuture.get();
                } catch (CancellationException e) {
                    log.debug("{} cancelled", this.getClass().getName(), e);
                } catch (ExecutionException | InterruptedException e) {
                    log.error("{} exception", this.getClass().getName(), e);
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

        // Ensure the scheduled task reaches a terminal state to avoid blocking on future.get() in the listener
        if (scheduledFuture != null) {
            try {
                scheduledFuture.cancel(true);
            } catch (Exception ignore) {
                // best-effort cancellation
            }
        }

        if (thread != null) {
            thread.interrupt();
            thread = null;
        }

        podLogs.forEach(LogWatch::close);
        podLogs.clear();

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }
}
