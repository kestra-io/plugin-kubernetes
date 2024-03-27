package io.kestra.plugin.kubernetes.services;

import io.kestra.core.models.script.AbstractLogConsumer;
import lombok.Getter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;

public class LoggingOutputStream extends java.io.OutputStream {
    private final AbstractLogConsumer logConsumer;
    @Getter
    private Instant lastTimestamp;

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public LoggingOutputStream(AbstractLogConsumer logConsumer) {
        this.logConsumer = logConsumer;
    }

    @Override
    public void write(int b) {
        if (b == '\n') {
            this.send();
        } else {
            baos.write(b);
        }
    }

    private void send() {
        if (baos.size() == 0) {
            return;
        }

        String line = baos.toString();
        baos.reset();

        ArrayList<String> logs = new ArrayList<>(Arrays.asList(line.split("[ ]")));
        if (!logs.isEmpty()) {
            try {
                lastTimestamp = Instant.parse(logs.get(0));
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
