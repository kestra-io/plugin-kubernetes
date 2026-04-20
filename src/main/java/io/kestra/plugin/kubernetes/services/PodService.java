package io.kestra.plugin.kubernetes.services;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
import io.kestra.plugin.kubernetes.models.Connection;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.PodResource;

abstract public class PodService {
    private static final List<String> COMPLETED_PHASES = List.of("Succeeded", "Failed", "Unknown"); // see https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
    private static final String SIDECAR_FILES_CONTAINER_NAME = "out-files";
    // 0 means "don't wait at all"; 30s gives the pod time to become ready before exec connections are established
    public static final int EXEC_READY_WAIT_TIMEOUT_MS = 30_000;

    public static KubernetesClient client(RunContext runContext, Connection connection) throws IllegalVariableEvaluationException {
        return connection != null ? client(connection.toConfig(runContext)) : client(null);
    }

    public static KubernetesClient client(Config config) {
        return config != null ? ClientService.of(config) : ClientService.of();
    }

    public static Pod waitForInitContainerRunning(KubernetesClient client, Pod pod, String container, Duration waitUntilRunning) {
        return PodService.podRef(client, pod)
            .waitUntilCondition(
                j -> j != null &&
                    j.getStatus() != null &&
                    j.getStatus().getInitContainerStatuses() != null &&
                    j.getStatus()
                        .getInitContainerStatuses()
                        .stream()
                        .filter(containerStatus -> containerStatus.getName().equals(container))
                        .anyMatch(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getRunning() != null),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static Pod waitForPodReady(KubernetesClient client, Pod pod, Duration waitUntilRunning) {
        boolean hasSidecar = pod.getSpec().getContainers().stream()
            .anyMatch(c -> SIDECAR_FILES_CONTAINER_NAME.equals(c.getName()));

        return PodService.podRef(client, pod)
            .waitUntilCondition(
                j -> j != null &&
                    j.getStatus() != null && (PodPhase.FAILED.value().equals(j.getStatus().getPhase()) ||
                        (j.getStatus().getContainerStatuses() != null &&
                            j.getStatus().getContainerStatuses().stream()
                                .anyMatch(
                                    cs -> cs.getState() != null &&
                                        cs.getState().getWaiting() != null &&
                                        cs.getState().getWaiting().getReason() != null &&
                                        !TransientWaitingReason.contains(cs.getState().getWaiting().getReason())
                                ))
                        ||
                        j.getStatus()
                            .getConditions()
                            .stream()
                            .anyMatch(
                                podCondition -> ("ContainersReady".equals(podCondition.getType()) &&
                                    (hasSidecar || "True".equals(podCondition.getStatus()))) ||
                                    ("PodCompleted".equals(podCondition.getReason()))
                            )),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static Pod waitForContainersStartedOrCompleted(KubernetesClient client, Pod pod, Duration waitUntilRunning) {
        var podResource = podRef(client, pod);
        var remaining = waitUntilRunning;
        var chunk = Duration.ofMinutes(5);

        while (remaining.toSeconds() > 0) {
            var waitTime = remaining.compareTo(chunk) < 0 ? remaining : chunk;
            remaining = remaining.minus(waitTime);

            try {
                var result = podResource.waitUntilCondition(
                    j -> j != null &&
                        j.getStatus() != null && ((PodPhase.RUNNING.value().equals(j.getStatus().getPhase()) &&
                            j.getStatus().getContainerStatuses() != null &&
                            j.getStatus().getContainerStatuses().stream()
                                .anyMatch(c -> c.getState().getRunning() != null))
                            ||
                            COMPLETED_PHASES.contains(j.getStatus().getPhase())),
                    waitTime.toSeconds(),
                    TimeUnit.SECONDS
                );
                if (result != null) {
                    return result;
                }
            } catch (KubernetesClientTimeoutException e) {
                // chunk expired — fall through to GET check
            } catch (KubernetesClientException e) {
                // watch error — fall through to GET check
            }

            podResource = podRef(client, pod);
            if (podResource.get() == null) {
                throw new KubernetesClientException("Pod was deleted while waiting for containers to start: " + pod.getMetadata().getName());
            }
        }

        throw new KubernetesClientTimeoutException(pod, waitUntilRunning.toSeconds(), TimeUnit.SECONDS);
    }

    public static Pod waitForCompletionExcept(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning, String except) {
        return waitForCompletion(
            client,
            logger,
            pod,
            waitRunning,
            j -> j != null &&
                j.getStatus() != null &&
                j.getStatus().getContainerStatuses() != null &&
                j.getStatus()
                    .getContainerStatuses()
                    .stream()
                    .anyMatch(containerStatus -> !containerStatus.getName().equals(except)) &&
                j.getStatus()
                    .getContainerStatuses()
                    .stream()
                    .filter(containerStatus -> !containerStatus.getName().equals(except))
                    .allMatch(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null)
        );
    }

    public static Pod waitForCompletion(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning) {
        return waitForCompletion(
            client,
            logger,
            pod,
            waitRunning,
            j -> j != null &&
                j.getStatus() != null &&
                COMPLETED_PHASES.contains(j.getStatus().getPhase())
        );
    }

    public static Pod waitForCompletion(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning, Predicate<Pod> condition) {
        var podResource = podRef(client, pod);
        var remaining = waitRunning;
        var chunk = Duration.ofMinutes(5);

        while (remaining.toSeconds() > 0) {
            var waitTime = remaining.compareTo(chunk) < 0 ? remaining : chunk;
            remaining = remaining.minus(waitTime);

            try {
                var ended = podResource.waitUntilCondition(condition, waitTime.toSeconds(), TimeUnit.SECONDS);
                if (ended != null) {
                    return ended;
                }
            } catch (KubernetesClientTimeoutException e) {
                // chunk expired — fall through to GET check below
            } catch (KubernetesClientException e) {
                // watch error — fall through to GET check below
                logger.debug("Watch error while waiting for pod completion, checking pod state", e);
            }

            podResource = podRef(client, pod);
            var current = podResource.get();
            if (current == null) {
                var podName = pod.getMetadata().getName();
                logger.warn("Pod '{}' was deleted before reaching a terminal phase", podName);
                throw new KubernetesClientException("Pod was deleted before reaching a terminal phase: " + podName);
            }
            if (condition.test(current)) {
                return current;
            }
        }

        throw new KubernetesClientTimeoutException(pod, waitRunning.toSeconds(), TimeUnit.SECONDS);
    }

    public static IllegalStateException failedMessage(Pod pod) throws IllegalStateException {
        if (pod.getStatus() == null) {
            return new IllegalStateException("Pods terminated without any status !");
        }

        return (pod.getStatus().getContainerStatuses() == null ? new ArrayList<ContainerStatus>() : pod.getStatus().getContainerStatuses())
            .stream()
            .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null)
            .map(containerStatus -> containerStatus.getState().getTerminated())
            .findFirst()
            .map(
                containerStateTerminated -> new IllegalStateException(
                    "Pods terminated with status '" + pod.getStatus().getPhase() + "', " +
                        "exitcode '" + containerStateTerminated.getExitCode() + "' & " +
                        "message '" + containerStateTerminated.getMessage() + "'"
                )
            )
            .orElseGet(() ->
            {
                if (pod.getStatus().getContainerStatuses() != null) {
                    Optional<String> waitingReason = pod.getStatus().getContainerStatuses().stream()
                        .filter(cs -> cs.getState() != null && cs.getState().getWaiting() != null)
                        .map(cs -> cs.getState().getWaiting().getReason())
                        .filter(reason -> reason != null && !TransientWaitingReason.contains(reason))
                        .findFirst();
                    if (waitingReason.isPresent()) {
                        return new IllegalStateException("Pod failed before container start: " + waitingReason.get());
                    }
                }
                return new IllegalStateException("Pod failed with phase '" + pod.getStatus().getPhase() + "'");
            });
    }

    public static void checkContainerFailures(Pod pod, String exceptContainer, Logger logger) throws IllegalStateException {
        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return;
        }

        pod.getStatus().getContainerStatuses().stream()
            .filter(containerStatus -> !containerStatus.getName().equals(exceptContainer))
            .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null)
            .filter(containerStatus -> containerStatus.getState().getTerminated().getExitCode() != 0)
            .findFirst()
            .ifPresent(containerStatus ->
            {
                ContainerStateTerminated terminated = containerStatus.getState().getTerminated();
                String errorMsg = "Container '" + containerStatus.getName() + "' failed with exit code " +
                    terminated.getExitCode() +
                    (terminated.getReason() != null ? ", reason: " + terminated.getReason() : "") +
                    (terminated.getMessage() != null ? ", message: " + terminated.getMessage() : "");

                logger.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            });
    }

    public static PodResource podRef(KubernetesClient client, Pod pod) {
        return client.pods()
            .inNamespace(pod.getMetadata().getNamespace())
            .withName(pod.getMetadata().getName());
    }

    /**
     * Retry file operations with exponential backoff optimized for freshly provisioned nodes.
     */
    public static Boolean withRetries(Logger logger, String where, RetryUtils.CheckedSupplier<Boolean> call) throws IOException {
        var retryPolicy = Exponential.builder()
            .type("exponential")
            .interval(Duration.ofSeconds(1))
            .maxInterval(Duration.ofSeconds(10))
            .maxDuration(Duration.ofSeconds(60))
            .delayFactor(2.0)
            .build();

        Boolean upload = RetryUtils.<Boolean, IOException> of(retryPolicy, logger).run(
            object -> !object,
            () ->
            {
                var bool = call.get();

                if (!bool) {
                    logger.debug("Failed to call '{}'", where);
                }

                return bool;
            }
        );

        if (!upload) {
            throw new IOException("Failed to call '" + where + "'");
        }

        return upload;
    }

    public static void uploadMarker(RunContext runContext, PodResource podResource, Logger logger, String marker, String container) throws IOException {
        File markerFile = tempDir(runContext).resolve(marker).toFile();
        if (!markerFile.createNewFile()) {
            throw new IOException("Unable to create the marker file: " + markerFile.getAbsolutePath());
        }

        withRetries(
            logger,
            "uploadMarker",
            () -> podResource
                .inContainer(container)
                .withReadyWaitTimeout(EXEC_READY_WAIT_TIMEOUT_MS)
                .file("/kestra/" + marker)
                .upload(markerFile.toPath())
        );

        if (!markerFile.delete()) {
            logger.debug("Unable to delete the marker file: {}", markerFile.getAbsolutePath());
        }
        logger.debug(marker + " marker uploaded");
    }

    /**
     * Fetch and log Kubernetes pod events (e.g. FailedScheduling, Evicted, ImagePullBackOff).
     */
    public static void logPodEvents(KubernetesClient client, Pod pod, Logger logger, AbstractLogConsumer logConsumer) {
        if (pod == null || pod.getMetadata() == null) {
            return;
        }

        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();

        try {
            client.v1().events()
                .inNamespace(namespace)
                .withField("involvedObject.name", podName)
                .list()
                .getItems()
                .stream()
                .filter(event -> "Warning".equals(event.getType()))
                .sorted(
                    Comparator.comparing(
                        Event::getLastTimestamp,
                        Comparator.nullsLast(Comparator.naturalOrder())
                    )
                )
                .forEach(event ->
                {
                    String reason = event.getReason() == null ? "" : event.getReason();
                    String message = event.getMessage() == null ? "" : event.getMessage();

                    logConsumer.accept(
                        "[pod-event] " + reason + " - " + message,
                        true
                    );
                });

        } catch (Exception e) {
            logger.warn("Failed to fetch events for pod '{}'", podName, e);
        }
    }

    public static boolean hasAnyContainerStarted(Pod pod) {
        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return false;
        }
        return pod.getStatus().getContainerStatuses().stream()
            .anyMatch(
                cs -> cs.getState() != null &&
                    (cs.getState().getRunning() != null || cs.getState().getTerminated() != null)
            );
    }

    public static boolean hasNonTransientWaitingContainer(Pod pod) {
        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return false;
        }
        return pod.getStatus().getContainerStatuses().stream()
            .anyMatch(
                cs -> cs.getState() != null &&
                    cs.getState().getWaiting() != null &&
                    cs.getState().getWaiting().getReason() != null &&
                    !TransientWaitingReason.contains(cs.getState().getWaiting().getReason())
            );
    }

    public static Path tempDir(RunContext runContext) {
        return runContext.workingDir().path().resolve("working-dir");
    }

    public enum TransientWaitingReason {
        CONTAINER_CREATING("ContainerCreating"),
        POD_INITIALIZING("PodInitializing");

        private final String reason;

        TransientWaitingReason(String reason) {
            this.reason = reason;
        }

        public static boolean contains(String reason) {
            for (TransientWaitingReason r : values()) {
                if (r.reason.equals(reason)) {
                    return true;
                }
            }
            return false;
        }
    }

    public enum PodPhase {
        PENDING("Pending"),
        RUNNING("Running"),
        SUCCEEDED("Succeeded"),
        FAILED("Failed"),
        UNKNOWN("Unknown");

        private final String value;

        PodPhase(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }
}
