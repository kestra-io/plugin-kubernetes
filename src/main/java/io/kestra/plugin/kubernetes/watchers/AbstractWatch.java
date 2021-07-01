package io.kestra.plugin.kubernetes.watchers;

import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;

abstract public class AbstractWatch<T> implements io.fabric8.kubernetes.client.Watcher<T> {
    protected Logger logger;

    public AbstractWatch(Logger logger) {
        this.logger = logger;
    }

    public void eventReceived(Action action, T resource) {
        logger.debug("Received action '{}' on [{}]", action, this.logContext(resource));
    }

    public void onClose() {
        logger.debug("Received close on [Type: {}]", this.getClass().getSimpleName());
    }

    public void onClose(WatcherException e) {
        logger.debug("Received close on [Type: {}] with exception", this.getClass().getSimpleName());

        if (e != null) {
            logger.error(e.getMessage(), e);
        }
    }

    abstract protected String logContext(T resource);
}
