package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PodLogService implements AutoCloseable {
    private final ThreadMainFactoryBuilder threadFactoryBuilder;
    private LogWatch podLogs;
    private ScheduledExecutorService scheduledExecutor;
    private LoggingOutputStream outputStream;

    public PodLogService(ThreadMainFactoryBuilder threadFactoryBuilder) {
        this.threadFactoryBuilder = threadFactoryBuilder;
    }

    public final void watch(PodResource<Pod> pod, Logger logger) {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threadFactoryBuilder.build("k8s-log"));
        outputStream = new LoggingOutputStream(logger, Level.INFO, null);
        AtomicBoolean started = new AtomicBoolean(false);

        ScheduledFuture<?> scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
            () -> {
                if (!started.get() || outputStream.getLastTimestamp().isBefore(Instant.now().minusSeconds(60))) {
                    if (!started.get()) {
                        started.set(true);
                    } else {
                        logger.trace("No log for since '{}', reconnecting", outputStream.getLastTimestamp().toString());
                    }

                    if (podLogs != null) {
                        podLogs.close();
                    }

                    podLogs = pod
                        .usingTimestamps()
                        .sinceTime(outputStream.getLastTimestamp() != null ?
                            outputStream.getLastTimestamp().plusSeconds(1).toString() :
                            null
                        )
                        .watchLog(outputStream);
                }
            },
            0,
            30,
            TimeUnit.SECONDS
        );

        // look at exception on the main thread
        Thread thread = new Thread(
            () -> {
                Await.until(scheduledFuture::isDone);

                try {
                    scheduledFuture.get();
                } catch (ExecutionException | InterruptedException e) {
                    log.error(this.getClass().getName() + " exception", e);
                }
            },
            "k8s-listener"
        );
        thread.start();
    }

    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }

        if (podLogs != null) {
            podLogs.close();
        }

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }
}
