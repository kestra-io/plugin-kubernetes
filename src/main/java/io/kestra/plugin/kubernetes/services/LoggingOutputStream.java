package io.kestra.plugin.kubernetes.services;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoggingOutputStream extends java.io.OutputStream {
    private final Logger logger;
    private final Level level;
    private final String format;
    @Getter
    private Instant lastTimestamp;

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public LoggingOutputStream(Logger logger, Level level, String prefix) {
        this.logger = logger;
        this.level = level;
        this.format = (prefix != null ? prefix + " {}" : "{}");
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
        if (logs.size() > 0) {
            try {
                lastTimestamp = Instant.parse(logs.get(0));
                logs.remove(0);
            } catch (DateTimeParseException ignored) {
            }

            line = String.join(" ", logs);
        }

        switch (level) {
            case TRACE:
                logger.trace(format, line);
                break;
            case DEBUG:
                logger.debug(format, line);
                break;
            case ERROR:
                logger.error(format, line);
                break;
            case INFO:
                logger.info(format, line);
                break;
            case WARN:
                logger.warn(format, line);
                break;
        }
    }

    @Override
    public void close() throws IOException {
        this.send();
        super.close();
    }
}
