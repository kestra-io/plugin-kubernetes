package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
import io.kestra.plugin.kubernetes.models.Connection;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

abstract public class PodService {
    private static final List<String> COMPLETED_PHASES = List.of("Succeeded", "Failed", "Unknown"); // see https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase

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
                    j.getStatus()
                        .getInitContainerStatuses()
                        .stream()
                        .filter(containerStatus -> containerStatus.getName().equals(container))
                        .anyMatch(containerStatus -> containerStatus.getState().getRunning() != null),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static Pod waitForPodReady(KubernetesClient client, Pod pod, Duration waitUntilRunning) {
        return PodService.podRef(client, pod)
            .waitUntilCondition(
                j -> j != null &&
                    j.getStatus() != null && (
                        "Failed".equals(j.getStatus().getPhase()) ||
                        j.getStatus()
                            .getConditions()
                            .stream()
                            .anyMatch(podCondition ->
                                ("ContainersReady".equals(podCondition.getType()) &&
                                    "True".equals(podCondition.getStatus())) ||
                                (podCondition.getReason() != null &&
                                    "PodCompleted".equals(podCondition.getReason()))
                            )
                    ),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static Pod waitForContainersStartedOrCompleted(KubernetesClient client, Pod pod, Duration waitUntilRunning) {
        return PodService.podRef(client, pod)
            .waitUntilCondition(
                j -> j != null &&
                    j.getStatus() != null && (
                        ("Running".equals(j.getStatus().getPhase()) &&
                         j.getStatus().getContainerStatuses() != null &&
                         j.getStatus().getContainerStatuses().stream()
                             .anyMatch(c -> c.getState().getRunning() != null))
                        ||
                        COMPLETED_PHASES.contains(j.getStatus().getPhase())
                    ),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static Pod waitForCompletionExcept(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning, String except) {
        return waitForCompletion(
            client,
            logger,
            pod,
            waitRunning,
            j -> j != null &&
                j.getStatus() != null &&
                j.getStatus()
                    .getContainerStatuses()
                    .stream()
                    .filter(containerStatus -> !containerStatus.getName().equals(except))
                    .allMatch(containerStatus -> containerStatus.getState().getTerminated() != null)
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
        Pod ended = null;
        PodResource podResource = podRef(client, pod);
        long startTime = System.currentTimeMillis();
        long maxWaitMillis = waitRunning.toMillis();

        while (ended == null) {
            // Calculate elapsed and remaining time
            long elapsed = System.currentTimeMillis() - startTime;
            long remaining = maxWaitMillis - elapsed;

            // Fail if maximum duration exceeded
            if (remaining <= 0) {
                throw new IllegalStateException(
                    String.format("Pod did not complete within waitRunning duration of %s", waitRunning)
                );
            }

            try {
                // Wait for REMAINING time, not full duration
                ended = podResource
                    .waitUntilCondition(
                        condition,
                        remaining,
                        TimeUnit.MILLISECONDS
                    );
            } catch (KubernetesClientException e) {
                // Check if we've exceeded the maximum duration after the failed wait
                elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= maxWaitMillis) {
                    throw new IllegalStateException(
                        String.format("Pod did not complete within waitRunning duration of %s", waitRunning),
                        e
                    );
                }

                // Retry with remaining time
                podResource = podRef(client, pod);
                if (podResource.get() != null) {
                    long remainingSeconds = (maxWaitMillis - elapsed) / 1000;
                    logger.debug("Pod is still alive, waiting for remaining {}s", remainingSeconds);
                } else {
                    logger.warn("Unable to refresh pods, no pods were found!", e);
                    throw e;
                }
            }
        }

        return ended;
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
            .map(containerStateTerminated -> new IllegalStateException(
                "Pods terminated with status '" + pod.getStatus().getPhase() + "', " +
                    "exitcode '" + containerStateTerminated.getExitCode() + "' & " +
                    "message '" + containerStateTerminated.getMessage() + "'"
            ))
            .orElse(new IllegalStateException("Pods terminated without any containers status !"));
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
            .ifPresent(containerStatus -> {
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

        Boolean upload = RetryUtils.<Boolean, IOException>of(retryPolicy, logger).run(
            object -> !object,
            () -> {
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
                .withReadyWaitTimeout(0)
                .file("/kestra/" + marker)
                .upload(markerFile.toPath())
        );

        if (!markerFile.delete()) {
            logger.debug("Unable to delete the marker file: {}", markerFile.getAbsolutePath());
        }
        logger.debug(marker + " marker uploaded");
    }

    public static Path tempDir(RunContext runContext) {
        return runContext.workingDir().path().resolve("working-dir");
    }
}
