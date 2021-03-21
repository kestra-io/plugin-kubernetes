package io.kestra.plugin.kubernetes.watchers;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;

public class PodWatcher extends AbstractWatch<Pod> {
    public PodWatcher(Logger logger) {
        super(logger);
    }

    protected String logContext(Pod resource) {
        return String.join(
            ", ",
            "Type: " + resource.getClass().getSimpleName(),
            "Namespace: " + resource.getMetadata().getNamespace(),
            "Name: " + resource.getMetadata().getName(),
            "Uid: " + resource.getMetadata().getUid(),
            "Phase: " + resource.getStatus().getPhase()
        );
    }

    @Override
    public void onClose() {

    }

    @Override
    public void onClose(WatcherException cause) {

    }
}
