package io.kestra.plugin.kubernetes.services;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.models.script.ScriptException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
import io.kestra.core.utils.Slugify;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

abstract public class PodService {
    public static Pod waitForInitContainerRunning(KubernetesClient client, Pod pod, String container, Duration waitUntilRunning) {
        return PodService.podRef(client, pod)
            .waitUntilCondition(
                j -> j == null ||
                    j.getStatus() == null ||
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
                j -> j == null ||
                    j.getStatus() == null ||
                    j.getStatus().getPhase().equals("Failed") ||
                    j.getStatus()
                        .getConditions()
                        .stream()
                        .anyMatch(podCondition -> podCondition.getType().equals("ContainersReady") ||
                                (podCondition.getReason() != null && podCondition.getReason().equals("PodCompleted"))
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
            j -> j == null ||
                j.getStatus() == null ||
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
            j -> j == null ||
                j.getStatus() == null ||
                j.getStatus().getContainerStatuses().stream().allMatch(containerStatus -> containerStatus.getState().getTerminated() != null)
        );
    }

    public static Pod waitForCompletion(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning, Predicate<Pod> condition) {
        Pod ended = null;
        PodResource podResource = podRef(client, pod);

        while (ended == null) {
            try {
                ended = podResource
                    .waitUntilCondition(
                        condition,
                        waitRunning.toSeconds(),
                        TimeUnit.SECONDS
                    );
            } catch (KubernetesClientException e) {
                podResource = podRef(client, pod);

                if (podResource.get() != null) {
                    logger.debug("Pod is still alive, refreshing and trying to wait more", e);
                } else {
                    logger.warn("Unable to refresh pods, no pods was found!", e);
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

    public static PodResource podRef(KubernetesClient client, Pod pod) {
        return client.pods()
            .inNamespace(pod.getMetadata().getNamespace())
            .withName(pod.getMetadata().getName());
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> labels(RunContext runContext) {
        Map<String, String> flow = (Map<String, String>) runContext.getVariables().get("flow");
        Map<String, String> task = (Map<String, String>) runContext.getVariables().get("task");
        Map<String, String> execution = (Map<String, String>) runContext.getVariables().get("execution");
        Map<String, String> taskrun = (Map<String, String>) runContext.getVariables().get("taskrun");

        return ImmutableMap.of(
            "kestra.io/namespace", normalizedValue(flow.get("namespace")),
            "kestra.io/flow-id", normalizedValue(flow.get("id")),
            "kestra.io/task-id", normalizedValue(task.get("id")),
            "kestra.io/execution-id", normalizedValue(execution.get("id")),
            "kestra.io/taskrun-id", normalizedValue(taskrun.get("id")),
            "kestra.io/taskrun-attempt", normalizedValue(String.valueOf(taskrun.get("attemptsCount")))
        );
    }

    @SuppressWarnings("unchecked")
    public static String podName(RunContext runContext) {
        Map<String, String> flow = (Map<String, String>) runContext.getVariables().get("flow");
        Map<String, String> task = (Map<String, String>) runContext.getVariables().get("task");

        String name = Slugify.of(String.join(
            "-",
            flow.get("namespace"),
            flow.get("id"),
            task.get("id")
        ));
        String normalized = normalizedValue(name);
        if (normalized.length() > 58) {
            normalized = normalized.substring(0, 57);
        }

        // we add a suffix of 5 chars, this should be enough as it's the standard k8s way
        String suffix = RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        return normalized + "-" + suffix;
    }

    public static String normalizedValue(String name) {
        if (name.length() > 63) {
            name = name.substring(0, 63);
        }

        name = StringUtils.stripEnd(name, "-");
        name = StringUtils.stripEnd(name, ".");
        name = StringUtils.stripEnd(name, "_");

        return name;
    }

    public static Boolean withRetries(Logger logger, String where, RetryUtils.CheckedSupplier<Boolean> call) throws IOException {
        Boolean upload = new RetryUtils().<Boolean, IOException>of().run(
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
        return runContext.tempDir().resolve("working-dir");
    }
}
