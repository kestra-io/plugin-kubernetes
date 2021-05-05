package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

abstract public class PodService {
    public static Pod waitForPodReady(KubernetesClient client, String namespace, Pod pod, Duration waitUntilRunning) throws InterruptedException {
        return PodService.podRef(client, namespace, pod)
            .waitUntilCondition(
                j -> j.getStatus() == null ||
                    j.getStatus().getPhase().equals("Failed") ||
                    j.getStatus()
                        .getConditions()
                        .stream()
                        .filter(podCondition -> podCondition.getStatus().equalsIgnoreCase("True"))
                        .anyMatch(podCondition -> podCondition.getType().equals("ContainersReady") ||
                            (podCondition.getReason() != null && podCondition.getReason()
                                .equals("PodCompleted"))
                        ),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static void handleEnd(Pod ended) throws InterruptedException {
        // let some time to gather the logs before delete
        Thread.sleep(1000);

        if (ended.getStatus() != null && ended.getStatus().getPhase().equals("Failed")) {
            throw PodService.failedMessage(ended);
        }
    }

    public static Pod waitForCompletion(KubernetesClient client, Logger logger, String namespace, Pod job, Duration waitRunning) throws InterruptedException {
        try {
            return podRef(client, namespace, job)
                .waitUntilCondition(
                    j -> j == null || j.getStatus() == null || (!j.getStatus().getPhase().equals("Running") && !j.getStatus().getPhase().equals("Pending")),
                    waitRunning.toSeconds(),
                    TimeUnit.SECONDS
                );
        } catch (KubernetesClientException e) {
            if (e.getCause() instanceof WatcherException && e.getCause().getMessage().contains("too old resource version")) {
                logger.warn("Receive old version, trying to wait more", e);
                return PodService.waitForCompletion(client, logger, namespace, job, waitRunning);
            }

            throw e;
        }
    }

    public static IllegalStateException failedMessage(Pod pod) throws IllegalStateException {
        if (pod.getStatus() == null) {
            return new IllegalStateException("Pods terminated without any status !");
        }

        return (pod.getStatus().getContainerStatuses() == null ? new ArrayList<ContainerStatus>() : pod.getStatus().getContainerStatuses())
            .stream()
            .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState()
                .getTerminated() != null)
            .map(containerStatus -> containerStatus.getState().getTerminated())
            .findFirst()
            .map(containerStateTerminated -> new IllegalStateException(
                "Pods terminated with status '" + pod.getStatus().getPhase() + "', " +
                    "exitcode '" + containerStateTerminated.getExitCode() + "' & " +
                    "message '" + containerStateTerminated.getMessage() + "'"
            ))
            .orElse(new IllegalStateException("Pods terminated without any containers status !"));
    }

    public static PodResource<Pod> podRef(KubernetesClient client, String namespace, Pod pod) {
        return client.pods()
            .inNamespace(namespace)
            .withName(pod.getMetadata().getName());
    }
}
