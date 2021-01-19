package org.kestra.task.kubernetes.services;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import org.kestra.core.utils.Await;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

abstract public class JobService {
    public static void waitForPodCreated(KubernetesClient client, String namespace, Job job, Duration waitUntilRunning) throws TimeoutException {
        Await.until(
            () -> client
                .pods()
                .inNamespace(namespace)
                .withLabel("controller-uid", job.getMetadata().getUid())
                .list()
                .getItems()
                .size() > 0,
            Duration.ofMillis(500),
            waitUntilRunning
        );
    }

    public static Job waitForCompletion(KubernetesClient client, String namespace, Job job, Duration waitRunning) throws InterruptedException {
        return jobRef(client, namespace, job)
            .waitUntilCondition(
                j -> j == null || j.getStatus() == null || j.getStatus().getCompletionTime() != null,
                waitRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static Pod findPod(KubernetesClient client, String namespace, Job job) {
        return client
            .pods()
            .inNamespace(namespace)
            .withLabel("controller-uid", job.getMetadata().getUid())
            .list()
            .getItems()
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "Can't find pod for job '" + Objects.requireNonNull(job.getMetadata()).getName() + "'"
            ));
    }

    public static ScalableResource<Job> jobRef(KubernetesClient client, String namespace, Job job) {
        return client
            .batch()
            .jobs()
            .inNamespace(namespace)
            .withName(job.getMetadata().getName());
    }
}
