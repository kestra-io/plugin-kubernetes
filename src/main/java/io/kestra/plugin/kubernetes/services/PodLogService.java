package io.kestra.plugin.kubernetes.services;

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
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PodLogService implements AutoCloseable {
    private List<LogWatch> podLogs = new ArrayList<>();
    private ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledFuture;
    @Getter
    private LoggingOutputStream outputStream;
    private Thread thread;

    public void setLogConsumer(AbstractLogConsumer logConsumer) {
        if (outputStream == null) {
            outputStream = new LoggingOutputStream(logConsumer);
        }
    }

    public final void watch(KubernetesClient client, Pod pod, AbstractLogConsumer logConsumer, RunContext runContext) {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(ThreadMainFactoryBuilder.build("k8s-log"));
        setLogConsumer(logConsumer);
        AtomicBoolean started = new AtomicBoolean(false);
        AtomicReference<Instant> lastReconnection = new AtomicReference<>(Instant.now());
        Logger logger = runContext.logger();

        scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
            () -> {
                Instant lastTimestamp = outputStream.getLastTimestamp() == null ? null : Instant.from(outputStream.getLastTimestamp());
                boolean forceReconnect = Instant.now().isAfter(lastReconnection.get().plus(Duration.ofHours(3)));

                if (!started.get() || forceReconnect || lastTimestamp == null || lastTimestamp.isBefore(Instant.now().minus(Duration.ofMinutes(10)))) {
                    if (!started.get()) {
                        started.set(true);
                    } else {
                        if (forceReconnect) {
                            logger.trace("Connection is over 3 hours old, forcing reconnect to prevent kubelet disconnect.");
                        } else {
                            logger.trace("No log since '{}', reconnecting", lastTimestamp == null ? "unknown" : lastTimestamp.toString());
                        }
                    }

                    lastReconnection.set(Instant.now());

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
                                        logger.info("Pod no longer exists, stopping log collection");
                                        scheduledFuture.cancel(false);
                                    } else {
                                        throw e;
                                    }
                                }
                            });
                    } catch (KubernetesClientException e) {
                        if (e.getCode() == 404) {
                            logger.info("Pod no longer exists, stopping log collection");
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
                        logger.error("{} exception", this.getClass().getName(), e);
                    } else {
                        logger.debug("{} exception", this.getClass().getName(), e);
                    }
                }

                try {
                    scheduledFuture.get();
                } catch (CancellationException e) {
                    logger.debug("{} cancelled", this.getClass().getName(), e);
                } catch (ExecutionException | InterruptedException e) {
                    logger.error("{} exception", this.getClass().getName(), e);
                }
            }
        );
    }

    public void fetchFinalLogs(KubernetesClient client, Pod pod, RunContext runContext) throws IOException {
        if (outputStream == null) {
            return;
        }

        Instant lastTimestamp = outputStream.getLastTimestamp();
        Instant lookbackTime = lastTimestamp != null ? lastTimestamp.minus(Duration.ofSeconds(60)) : null;
        Logger logger = runContext.logger();

        // Hybrid approach: fetch logs since (lastTimestamp - 60s) to catch any missed by watchLog()
        // - Provides 60s safety buffer for multi-container out-of-order logs and K8s API delays
        // - Uses lastTimestamp as anchor for unbounded lookback when watchLog() failures are prolonged
        // - Hash-based deduplication efficiently handles the increased overlap
        logger.debug(
            "Fetching final logs since lookbackTime={} (lastTimestamp={} minus 60s)",
            lookbackTime,
            lastTimestamp
        );

        PodResource podResource = PodService.podRef(client, pod);

        pod.getSpec().getContainers().forEach(container -> {
            try {
                String logs = podResource
                    .inContainer(container.getName())
                    .usingTimestamps()
                    .sinceTime(lookbackTime != null ? lookbackTime.toString() : null)
                    .getLog();

                if (logs != null && !logs.isEmpty()) {
                    // Write all logs - hash-based deduplication automatically filters duplicates
                    outputStream.write(logs.getBytes());
                    outputStream.flush();
                } else {
                    logger.debug("No logs returned for container '{}'", container.getName());
                }
            } catch (IOException e) {
                logger.error("Failed to fetch final logs for container '{}'", container.getName(), e);
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
