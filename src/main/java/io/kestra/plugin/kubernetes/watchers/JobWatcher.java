package io.kestra.plugin.kubernetes.watchers;

import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;

public class JobWatcher extends AbstractWatch<Job> {
    public JobWatcher(Logger logger) {
        super(logger);
    }

    protected String logContext(Job resource) {
        return String.join(
            ", ",
            "Type: " + resource.getClass().getSimpleName(),
            "Namespace: " + resource.getMetadata().getNamespace(),
            "Name: " + resource.getMetadata().getName(),
            "Uid: " + resource.getMetadata().getUid(),
            "Status: " + resource.getStatus()
        );
    }

    @Override
    public void onClose(WatcherException cause) {

    }
}
