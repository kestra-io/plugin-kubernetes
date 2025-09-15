package io.kestra.plugin.kubernetes;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.FilesService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import io.kestra.plugin.kubernetes.models.Connection;
import io.kestra.plugin.kubernetes.models.Metadata;
import io.kestra.plugin.kubernetes.models.PodStatus;
import io.kestra.plugin.kubernetes.services.InstanceService;
import io.kestra.plugin.kubernetes.services.PodLogService;
import io.kestra.plugin.kubernetes.services.PodService;
import io.kestra.plugin.kubernetes.watchers.PodWatcher;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.plugin.kubernetes.services.PodService.waitForCompletion;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a pod on a Kubernetes cluster, wait until the pod stops and collect its logs."
)
@Plugin(
    examples = {
        @Example(
            title = "Launch a Pod",
            full = true,
            code = """
                id: kubernetes_pod_create
                namespace: company.team

                tasks:
                  - id: pod_create
                    type: io.kestra.plugin.kubernetes.PodCreate
                    namespace: default
                    metadata:
                      labels:
                        my-label: my-value
                    spec:
                      containers:
                      - name: unittest
                        image: debian:stable-slim
                        command:
                          - 'bash'
                          - '-c'
                          - 'for i in {1..10}; do echo $i; sleep 0.1; done'
                      restartPolicy: Never
                """
        ),
        @Example(
            title = "Launch a Pod with input files and gather its output files.",
            full = true,
            code = """
                id: kubernetes_pod_create
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: pod_create
                    type: io.kestra.plugin.kubernetes.PodCreate
                    spec:
                      containers:
                      - name: unittest
                        image: centos
                        command:
                          - cp
                          - "{{workingDir}}/data.txt"
                          - "{{workingDir}}/out.txt"
                      restartPolicy: Never
                    waitUntilRunning: PT3M
                    inputFiles:
                      data.txt: "{{inputs.file}}"
                    outputFiles:
                      - out.txt
                """
        ),
        @Example(
            title = "Launch a Pod with input files and gather its output files limiting resources for the init and sidecar containers.",
            full = true,
            code = """
                id: kubernetes_pod_create
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: pod_create
                    type: io.kestra.plugin.kubernetes.PodCreate
                    fileSidecar:
                      resources:
                        limits:
                          cpu: "300m"
                          memory: "512Mi"
                    spec:
                      containers:
                      - name: unittest
                        image: centos
                        command:
                          - cp
                          - "{{workingDir}}/data.txt"
                          - "{{workingDir}}/out.txt"
                      restartPolicy: Never
                    waitUntilRunning: PT3M
                    inputFiles:
                      data.txt: "{{inputs.file}}"
                    outputFiles:
                      - out.txt
                """
        )
    }
)
@Slf4j
public class PodCreate extends AbstractPod implements RunnableTask<PodCreate.Output> {
    @Schema(
        title = "The namespace where the pod will be created"
    )
    @NotNull
    @Builder.Default
    private Property<String> namespace = Property.ofValue("default");

    @Schema(
        title = "The YAML metadata of the pod."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> metadata;

    @Schema(
        title = "The YAML spec of the pod."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Map<String, Object> spec;

    @Schema(
        title = "Whether the pod should be deleted upon completion."
    )
    @NotNull
    @Builder.Default
    private final Property<Boolean> delete = Property.ofValue(true);

    @Schema(
        title = "Whether to reconnect to the current pod if it already exists."
    )
    @NotNull
    @Builder.Default
    private final Property<Boolean> resume = Property.ofValue(true);

    @Schema(
        title = "Additional time after the pod ends to wait for late logs."
    )
    @Builder.Default
    private Property<Duration> waitForLogInterval = Property.ofValue(Duration.ofSeconds(2));

    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicReference<String> currentPodName = new AtomicReference<>();
    private volatile String currentNamespace;
    private volatile Connection currentConnection;

    @Override
    public PodCreate.Output run(RunContext runContext) throws Exception {
        try {
            this.currentConnection = this.getConnection();

            super.init(runContext);
            Map<String, Object> additionalVars = new HashMap<>();
            additionalVars.put("workingDir", "/kestra/working-dir");
            var outputFilesList = runContext.render(this.outputFiles).asList(String.class);
            if (!outputFilesList.isEmpty()) {
                Map<String, Object> outputFileVariables = new HashMap<>();
                outputFilesList.forEach(file -> outputFileVariables.put(file, "/kestra/working-dir/" + file));
                additionalVars.put("outputFiles", outputFileVariables);
            }

            String namespace = runContext.render(this.namespace).as(String.class).orElseThrow();
            Logger logger = runContext.logger();

            this.currentNamespace = namespace;

            try (KubernetesClient client = PodService.client(runContext, this.getConnection());
                 PodLogService podLogService = new PodLogService(((DefaultRunContext) runContext).getApplicationContext().getBean(ThreadMainFactoryBuilder.class))) {
                Pod pod = null;

                if (runContext.render(this.resume).as(Boolean.class).orElseThrow()) {
                    // try to locate an existing pod for this taskrun and attempt
                    Map<String, String> taskrun = (Map<String, String>) runContext.getVariables().get("taskrun");
                    String taskrunId = ScriptService.normalize(taskrun.get("id"));
                    String attempt = ScriptService.normalize(String.valueOf(taskrun.get("attemptsCount")));
                    String labelSelector = "kestra.io/taskrun-id=" + taskrunId + "," + "kestra.io/taskrun-attempt=" + attempt;
                    var existingPods = client.pods().inNamespace(namespace).list(new ListOptionsBuilder().withLabelSelector(labelSelector).build());
                    if (existingPods.getItems().size() == 1) {
                        pod = existingPods.getItems().get(0);
                        logger.info("Pod '{}' is resumed from an already running pod ", pod.getMetadata().getName());

                    } else if (!existingPods.getItems().isEmpty()) {
                        logger.warn("More than one pod exist for the label selector {}, no pods will be resumed.", labelSelector);
                    }
                }

                if (pod == null) {
                    pod = createPod(runContext, client, namespace, additionalVars);
                    logger.info("Pod '{}' is created ", pod.getMetadata().getName());
                }

                currentPodName.set(pod.getMetadata().getName());

                try (Watch ignored = PodService.podRef(client, pod).watch(listOptions(runContext), new PodWatcher(logger))) {
                    // in case of resuming an already running pod, the status will be running
                    if (!"Running".equals(pod.getStatus().getPhase())) {
                        // wait for init container
                        if (this.inputFiles != null) {
                            Map<String, String> finalInputFiles = PluginUtilsService.transformInputFiles(runContext, additionalVars, this.inputFiles);

                            PluginUtilsService.createInputFiles(
                                runContext,
                                PodService.tempDir(runContext),
                                finalInputFiles,
                                additionalVars
                            );

                            pod = PodService.waitForInitContainerRunning(client, pod, INIT_FILES_CONTAINER_NAME, runContext.render(this.waitUntilRunning).as(Duration.class).orElseThrow());
                            this.uploadInputFiles(runContext, PodService.podRef(client, pod), logger, finalInputFiles.keySet());
                        }

                        // wait for pods ready
                        pod = PodService.waitForPodReady(client, pod, runContext.render(this.waitUntilRunning).as(Duration.class).orElseThrow());
                    }

                    if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed")) {
                        throw PodService.failedMessage(pod);
                    }

                    // watch log
                    AbstractLogConsumer logConsumer = new DefaultLogConsumer(runContext);
                    podLogService.watch(client, pod, logConsumer, runContext);

                    // wait until completion of the pods
                    Pod ended;
                    var waitRunningValue = runContext.render(this.waitRunning).as(Duration.class).orElseThrow();
                    if (this.outputFiles != null) {
                        ended = PodService.waitForCompletionExcept(client, logger, pod, waitRunningValue, SIDECAR_FILES_CONTAINER_NAME);
                    } else {
                        ended = waitForCompletion(client, logger, pod, waitRunningValue);
                    }

                    handleEnd(ended, runContext);

                    PodStatus podStatus = PodStatus.from(ended.getStatus());
                    Output.OutputBuilder output = Output.builder()
                        .metadata(Metadata.from(ended.getMetadata()))
                        .status(podStatus)
                        .vars(logConsumer.getOutputs());

                    if (isFailed(podStatus)) {
                        podStatus.getConditions().stream()
                            .filter(podCondition -> Objects.equals(podCondition.getReason(), "PodFailed"))
                            .forEach(podCondition -> logger.error(podCondition.getMessage()));

                        podStatus.getContainerStatuses().stream()
                            .filter(containerStatus -> containerStatus.getState().getTerminated() != null && Objects.equals(containerStatus.getState().getTerminated().getReason(), "ContainerCannotRun"))
                            .forEach(containerStatus -> logger.error(containerStatus.getState().getTerminated().getMessage()));
                    } else if (this.outputFiles != null) {
                        Map<Path, Path> pathMap = this.downloadOutputFiles(runContext, PodService.podRef(client, pod), logger, additionalVars);
                        output.outputFiles(outputFiles(runContext, runContext.render(this.outputFiles).asList(String.class), pathMap));
                    }

                    delete(client, logger, pod, runContext);

                    return output
                        .build();
                } catch (InterruptedException | InterruptedIOException e) {
                    logger.info("Task was interrupted.");
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        } finally {
            killed.set(false);
            currentPodName.set(null);
            currentNamespace = null;
            currentConnection = null;
        }
    }

    // This and the following method were copied from FilesService and adapted. Cleaner solution might be to move them
    // to FilesService with an optional pathMap parameter as any consumer of the method might have the same issues with
    // spaces in file paths.
    public static Map<String, URI> outputFiles(RunContext runContext, List<String> outputs, Map<Path, Path> pathMap) throws Exception {
        List<String> renderedOutputs = outputs != null ? runContext.render(outputs) : null;
        List<Path> allFilesMatching = runContext.workingDir().findAllFilesMatching(renderedOutputs);
        var outputFiles = allFilesMatching.stream()
            .map(throwFunction(path -> {
                Path unsanitizedRelative = pathMap.get(path);
                if (unsanitizedRelative == null) {
                    throw new IllegalStateException("No unsanitized relative file path found for " + path);
                }
                return new AbstractMap.SimpleEntry<>(
                    unsanitizedRelative.toString(),
                    runContext.storage().putFile(path.toFile(), resolveUniqueNameForFile(path))
                );
            }))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (runContext.logger().isTraceEnabled()) {
            runContext.logger().trace("Captured {} output file(s).", allFilesMatching.size());
        }

        return outputFiles;
    }

    private static String resolveUniqueNameForFile(final Path path) {
        return IdUtils.from(path.toString()) + "-" + path.toFile().getName();
    }

    private Pod createPod(RunContext runContext, KubernetesClient client, String namespace, Map<String, Object> additionalVars) throws java.io.IOException, io.kestra.core.exceptions.IllegalVariableEvaluationException {
        ObjectMeta metadata = InstanceService.fromMap(
            ObjectMeta.class,
            runContext,
            additionalVars,
            this.metadata,
            ImmutableMap.of("labels", ScriptService.labels(runContext, "kestra.io/"))
        );

        if (metadata.getName() == null) {
            metadata.setName(ScriptService.jobName(runContext));
        }

        PodSpec spec = InstanceService.fromMap(
            PodSpec.class,
            runContext,
            additionalVars,
            this.spec
        );


        this.handleFiles(runContext, spec);

        return client.pods()
            .inNamespace(namespace)
            .resource(new PodBuilder()
                .withMetadata(metadata)
                .withSpec(spec)
                .build()
            )
            .create();
    }

    private void delete(KubernetesClient client, Logger logger, Pod pod, RunContext runContext) throws IllegalVariableEvaluationException {
        if (runContext.render(delete).as(Boolean.class).orElseThrow()) {
            try {
                PodService.podRef(client, pod).delete();
                logger.info("Pod '{}' is deleted ", pod.getMetadata().getName());
            } catch (Throwable e) {
                logger.warn("Unable to delete pod {}", pod.getFullResourceName(), e);
            }
        }
    }

    private void handleEnd(Pod ended, RunContext runContext) throws InterruptedException, IllegalVariableEvaluationException {
        // let some time to gather the logs before delete
        Thread.sleep(runContext.render(this.waitForLogInterval).as(Duration.class).orElseThrow().toMillis());

        if (ended.getStatus() != null && ended.getStatus().getPhase().equals("Failed")) {
            throw PodService.failedMessage(ended);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The pod metadata."
        )
        private final Metadata metadata;

        @Schema(
            title = "The pod status."
        )
        private final PodStatus status;

        @Schema(
            title = "The output files URI in Kestra's internal storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> outputFiles;

        @Schema(
            title = "The output variables extracted from the logs of the commands"
        )
        private final Map<String, Object> vars;

        @Override
        public Optional<State.Type> finalState() {
            return status != null ? stateFromStatus(status) : io.kestra.core.models.tasks.Output.super.finalState();
        }

        private Optional<State.Type> stateFromStatus(PodStatus status) {
            if (isFailed(status)) {
                return Optional.of(State.Type.FAILED);
            }
            return Optional.empty();
        }
    }

    private static boolean isFailed(PodStatus podStatus) {
        return podStatus.getConditions().stream().anyMatch(podCondition -> Objects.equals(podCondition.getReason(), "PodFailed")) ||
            podStatus.getContainerStatuses().stream().anyMatch(containerStatus -> containerStatus.getState().getTerminated() != null && Objects.equals(containerStatus.getState().getTerminated().getReason(), "ContainerCannotRun"));
    }

    @Override
    public void kill() {
        if (!killed.compareAndSet(false, true)) {
            return;
        }

        if (currentPodName.get() == null || currentNamespace == null) {
            log.debug("Kill called but pod context not available.");
            return;
        }

        log.warn("Task was killed, deleting the pod '{}' in namespace '{}'.", currentPodName.get(), namespace);

        try (KubernetesClient client = PodService.client(null, currentConnection)) {
            client.pods()
                .inNamespace(currentNamespace)
                .withName(currentPodName.get())
                .withGracePeriod(0L)
                .delete();
            log.info("Pod '{}' from killed task has been deleted.", currentPodName.get());
        } catch (Exception e) {
            log.warn("Failed to delete pod '{}' on kill: {}", currentPodName.get(), e.getMessage());
        }
    }
}
