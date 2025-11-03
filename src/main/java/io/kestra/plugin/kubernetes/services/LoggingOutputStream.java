package io.kestra.plugin.kubernetes.services;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Output stream for Kubernetes pod logs with hash-based deduplication.
 * <p>
 * Prevents duplicate logs when both watchLog() streaming and fetchFinalLogs() batch retrieval
 * deliver the same log lines. Uses hash of complete line (timestamp + content) for memory
 * efficiency (~4 bytes per log). Thread-safe for multi-container pods.
 * <p>
 * <b>Timestamp Handling:</b> Original Kubernetes log timestamps (RFC3339 format) are parsed and
 * tracked internally in {@link #lastTimestamp} for reconnection logic, but are stripped from
 * messages before passing to the log consumer. This means {@link io.kestra.core.models.executions.LogEntry}
 * timestamps will reflect queue processing time, not the original log generation time from the pod.
 * For timing analysis, use {@link #getLastTimestamp()} which returns the original Kubernetes timestamp.
 */
public class LoggingOutputStream extends java.io.OutputStream {
    private final AbstractLogConsumer logConsumer;
    private volatile Instant lastTimestamp;
    private final Set<Integer> writtenLogs = Collections.synchronizedSet(new HashSet<>());
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Rate limiting: track last emission time to prevent overwhelming async queue
    private volatile long lastEmissionNanos = 0;
    private static final long MIN_DELAY_BETWEEN_EMISSIONS_NANOS = 2_000_000; // 2ms

    /**
     * Creates a new logging output stream.
     *
     * @param logConsumer the consumer that receives deduplicated log lines
     */
    public LoggingOutputStream(AbstractLogConsumer logConsumer) {
        this.logConsumer = logConsumer;
    }

    /**
     * Returns the most recent timestamp seen in log lines.
     *
     * @return the most recent timestamp, or null if no timestamped logs have been written
     */
    public synchronized Instant getLastTimestamp() {
        return lastTimestamp;
    }

    @Override
    public synchronized void write(int b) {
        if (b == '\n') {
            this.send();
        } else {
            baos.write(b);
        }
    }

    /**
     * Writes a portion of a byte array to the stream.
     * Ensures atomic line processing to prevent log corruption from concurrent container streams.
     *
     * @param b the byte array
     * @param off the start offset in the data
     * @param len the number of bytes to write
     * @throws IOException if an I/O error occurs
     */
    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        for (int i = 0; i < len; i++) {
            write(b[off + i]);
        }
    }

    private synchronized void send() {
        if (baos.size() == 0) {
            return;
        }

        String lineWithTimestamp = baos.toString().trim();
        baos.reset();

        if (lineWithTimestamp.isEmpty()) {
            return;
        }

        // Hash-based deduplication: use hash of complete line (timestamp + content)
        // Memory efficient: ~4 bytes per log instead of ~100 bytes
        if (!writtenLogs.add(lineWithTimestamp.hashCode())) {
            // This line was already written - skip duplicate
            return;
        }

        // Split by any whitespace to safely extract a potential ISO timestamp prefix injected by k8s
        ArrayList<String> logs = new ArrayList<>(Arrays.asList(lineWithTimestamp.split("\\s+")));
        String message = lineWithTimestamp;

        if (!logs.isEmpty()) {
            try {
                Instant newTimestamp = Instant.parse(logs.getFirst());
                // Only update lastTimestamp if the new timestamp is newer (handles out-of-order log arrivals)
                if (lastTimestamp == null || newTimestamp.isAfter(lastTimestamp)) {
                    lastTimestamp = newTimestamp;
                }
                logs.remove(0);
                message = String.join(" ", logs);
            } catch (DateTimeParseException ignored) {
                // No valid timestamp, use line as-is
            }
        }

        // Rate limiting: add small delay between emissions to prevent overwhelming async queue
        long now = System.nanoTime();
        long timeSinceLastEmission = now - lastEmissionNanos;

        if (lastEmissionNanos > 0 && timeSinceLastEmission < MIN_DELAY_BETWEEN_EMISSIONS_NANOS) {
            long sleepNanos = MIN_DELAY_BETWEEN_EMISSIONS_NANOS - timeSinceLastEmission;
            try {
                Thread.sleep(sleepNanos / 1_000_000, (int)(sleepNanos % 1_000_000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        lastEmissionNanos = System.nanoTime();

        // we have no way to know that a log is from stdErr so with Kubernetes all logs will always be INFO
        logConsumer.accept(message, false);
    }

    @Override
    public void flush() throws IOException {
        this.send();
        super.flush();
    }

    /**
     * Closes the stream and releases resources.
     * Flushes any remaining buffered content and clears the deduplication set.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        this.send();
        writtenLogs.clear();
        super.close();
    }
}