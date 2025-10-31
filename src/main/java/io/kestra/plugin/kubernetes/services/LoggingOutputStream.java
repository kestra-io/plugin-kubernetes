package io.kestra.plugin.kubernetes.services;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;

public class LoggingOutputStream extends java.io.OutputStream {
    private final AbstractLogConsumer logConsumer;
    private volatile Instant lastTimestamp;

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public LoggingOutputStream(AbstractLogConsumer logConsumer) {
        this.logConsumer = logConsumer;
    }

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

        String line = baos.toString();
        baos.reset();

        // Trim to remove accidental leading/trailing spaces that can break output marker parsing
        line = line.trim();

        // Split by any whitespace to safely extract a potential ISO timestamp prefix injected by k8s
        ArrayList<String> logs = new ArrayList<>(Arrays.asList(line.split("\\s+")));
        if (!logs.isEmpty()) {
            try {
                Instant newTimestamp = Instant.parse(logs.get(0));
                // Only update lastTimestamp if the new timestamp is newer (handles out-of-order log arrivals)
                if (lastTimestamp == null || newTimestamp.isAfter(lastTimestamp)) {
                    lastTimestamp = newTimestamp;
                }
                logs.remove(0);
            } catch (DateTimeParseException ignored) {
            }

            line = String.join(" ", logs);
        }

        // we have no way to know that a log is from stdErr so with Kubernetes all logs will always be INFO
        
        logConsumer.accept(line, false);
    }

    @Override
    public void close() throws IOException {
        this.send();
        super.close();
    }
}