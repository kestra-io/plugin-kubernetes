package io.kestra.plugin.kubernetes.services;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PodLogService implements AutoCloseable {
    private final ThreadMainFactoryBuilder threadFactoryBuilder;
    private List<LogWatch> podLogs = new ArrayList<>();
    private ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledFuture;
    @Getter
    private LoggingOutputStream outputStream;
    private Thread thread;

    public PodLogService(ThreadMainFactoryBuilder threadFactoryBuilder) {
        this.threadFactoryBuilder = threadFactoryBuilder;
    }

    public void setLogConsumer(AbstractLogConsumer logConsumer) {
        if (outputStream == null) {
            outputStream = new LoggingOutputStream(logConsumer);
        }
    }

    public final void watch(KubernetesClient client, Pod pod, AbstractLogConsumer logConsumer, RunContext runContext) {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threadFactoryBuilder.build("k8s-log"));
        setLogConsumer(logConsumer);
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

                    if (podLogs != null) {
                        podLogs.forEach(LogWatch::close);
                        podLogs = new ArrayList<>();
                    }

                    PodResource podResource = PodService.podRef(client, pod);

                    try {
                        pod
                            .getSpec()
                            .getContainers()
                            .forEach(container -> {
                                try {
                                    podLogs.add(podResource
                                        .inContainer(container.getName())
                                        .usingTimestamps()
                                        .sinceTime(lastTimestamp != null ?
                                            lastTimestamp.plusNanos(1).toString() :
                                            null
                                        )
                                        .watchLog(outputStream)
                                    );
                                } catch (KubernetesClientException e) {
                                    if (e.getCode() == 404) {
                                        runContext.logger().info("Pod no longer exists, stopping log collection");
                                        scheduledFuture.cancel(false);
                                    } else {
                                        throw e;
                                    }
                                }
                            });
                    } catch (KubernetesClientException e) {
                        if (e.getCode() == 404) {
                            runContext.logger().info("Pod no longer exists, stopping log collection");
                            scheduledFuture.cancel(false);
                        } else {
                            throw e;
                        }
                    }
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

    public void fetchFinalLogs(KubernetesClient client, Pod pod, RunContext runContext) throws IOException {
        if (outputStream == null) {
            return;
        }

        // Fetch logs from last 60 seconds to catch any missed by watchLog()
        // For short containers: fetches all logs (K8s returns everything if sinceSeconds > pod lifetime)
        // For long containers: 60s lookback catches queued/delayed logs
        runContext.logger().debug(
            "Fetching final logs from last 60 seconds, lastTimestamp={}",
            outputStream.getLastTimestamp()
        );

        PodResource podResource = PodService.podRef(client, pod);

        pod.getSpec().getContainers().forEach(container -> {
            try {
                String logs = podResource
                    .inContainer(container.getName())
                    .usingTimestamps()
                    .sinceSeconds(60)
                    .getLog();

                if (logs != null && !logs.isEmpty()) {
                    // Write all logs - hash-based deduplication automatically filters duplicates
                    outputStream.write(logs.getBytes());
                    outputStream.flush();
                } else {
                    runContext.logger().debug("No logs returned for container '{}'", container.getName());
                }
            } catch (IOException e) {
                runContext.logger().error("Failed to fetch final logs for container '{}'", container.getName(), e);
            }
        });
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

        if (podLogs != null) {
            podLogs.forEach(LogWatch::close);
        }

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }
}
