package org.kestra.task.kubernetes.watchers;

import io.fabric8.kubernetes.api.model.batch.Job;
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
            "Uid: " + resource.getMetadata().getUid()
        );
    }
}
