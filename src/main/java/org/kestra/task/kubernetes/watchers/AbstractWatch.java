package org.kestra.task.kubernetes.watchers;

import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;

abstract public class AbstractWatch<T> implements io.fabric8.kubernetes.client.Watcher<T> {
    protected Logger logger;

    public AbstractWatch(Logger logger) {
        this.logger = logger;
    }

    public void eventReceived(Action action, T resource) {
        logger.debug("Received action '{}' on [{}]", action, this.logContext(resource));
    }

    public void onClose(KubernetesClientException e) {
        logger.debug("Received close on [Type: {}]", this.getClass().getSimpleName());

        if (e != null) {
            logger.error(e.getMessage(), e);
        }
    }

    abstract protected String logContext(T resource);
}
