package io.kestra.plugin.kubernetes.services;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class LoggingOutputStream extends java.io.OutputStream {
    private final Logger logger;
    private final Level level;
    private final String format;

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);

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
