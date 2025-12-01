package io.kestra.plugin.kubernetes.core;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.plugin.kubernetes.services.PodService.waitForCompletion;

import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import io.kestra.plugin.kubernetes.AbstractPod;
import io.kestra.plugin.kubernetes.models.Connection;
import io.kestra.plugin.kubernetes.models.Metadata;
import io.kestra.plugin.kubernetes.models.PodStatus;
import io.kestra.plugin.kubernetes.services.InstanceService;
import io.kestra.plugin.kubernetes.services.PodLogService;
import io.kestra.plugin.kubernetes.services.PodService;
import io.kestra.plugin.kubernetes.watchers.PodWatcher;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates and manages a Kubernetes pod, handling its complete lifecycle from creation to cleanup.
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li>Creates pods from YAML specifications with template variable support</li>
 *   <li>Supports input file upload and output file download via init containers and sidecars</li>
 *   <li>Streams logs in real-time during pod execution</li>
 *   <li>Can resume execution of existing pods when task is restarted</li>
 *   <li>Handles graceful cleanup and pod deletion after completion or failure</li>
 * </ul>
 *
 * <h2>File Transfer Mechanism</h2>
 * When inputFiles or outputFiles are specified, the pod is configured with additional containers:
 * <ul>
 *   <li><b>Init container (init-files):</b> Waits for input files to be uploaded before starting main containers.
 *       Files are uploaded to {@code /kestra/working-dir} via Kubernetes exec API.</li>
 *   <li><b>Sidecar container (out-files):</b> Waits for an "ended" marker file before allowing pod deletion,
 *       ensuring output files are collected. The sidecar exits gracefully when signaled.</li>
 *   <li><b>Shared volume:</b> A volume named "kestra-files" is mounted at {@code /kestra} in all containers
 *       to enable file sharing between Kestra and the pod.</li>
 * </ul>
 *
 * <h2>Resume Behavior</h2>
 * When {@code resume=true} (default), the task attempts to reconnect to an existing pod with matching
 * {@code kestra.io/taskrun-id} and {@code kestra.io/taskrun-attempt} labels. This allows recovery from
 * interrupted executions without restarting the pod. If no matching pod exists, a new one is created.
 *
 * <h2>Pod Lifecycle</h2>
 * <ol>
 *   <li>Pod is created (or existing pod is found if resuming)</li>
 *   <li>Wait for pod to reach Running state (timeout: waitUntilRunning)</li>
 *   <li>Upload input files if specified</li>
 *   <li>Stream logs from all containers</li>
 *   <li>Wait for pod completion (timeout: waitRunning)</li>
 *   <li>Download output files if specified</li>
 *   <li>Delete pod if delete=true</li>
 * </ol>
 *
 * @see io.kestra.plugin.kubernetes.AbstractPod for file transfer implementation details
 * @see io.kestra.plugin.kubernetes.services.PodService for pod lifecycle utilities
 * @see io.kestra.plugin.kubernetes.services.PodLogService for log streaming
 */
@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a pod on a Kubernetes cluster, wait until the pod stops and collect its logs."
)
@Plugin(
    aliases = {"io.kestra.plugin.kubernetes.PodCreate"},
    examples = {
        @Example(
            title = "Launch a Pod",
            full = true,
            code = """
                id: kubernetes_pod_create
                namespace: company.team

                tasks:
                  - id: pod_create
                    type: io.kestra.plugin.kubernetes.core.PodCreate
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
                    type: io.kestra.plugin.kubernetes.core.PodCreate
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
                    type: io.kestra.plugin.kubernetes.core.PodCreate
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
        ),
        @Example(
            title = "Launch a Pod with security context on init/sidecar containers for restrictive environments.",
            full = true,
            code = """
                id: kubernetes_pod_create_secure
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: pod_create
                    type: io.kestra.plugin.kubernetes.core.PodCreate
                    fileSidecar:
                      securityContext:
                        allowPrivilegeEscalation: false
                        capabilities:
                          drop:
                            - ALL
                        readOnlyRootFilesystem: true
                        seccompProfile:
                          type: RuntimeDefault
                    spec:
                      containers:
                      - name: main
                        image: centos
                        command:
                          - cp
                          - "{{workingDir}}/data.txt"
                          - "{{workingDir}}/out.txt"
                        securityContext:
                          allowPrivilegeEscalation: false
                          capabilities:
                            drop:
                              - ALL
                          readOnlyRootFilesystem: true
                          seccompProfile:
                            type: RuntimeDefault
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
        title = "The pod metadata configuration",
        description = "Kubernetes metadata for the pod, including labels, annotations, and name.\n" +
            "If name is not specified, it will be auto-generated based on the task execution context.\n" +
            "Supports dynamic template expressions."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> metadata;

    @Schema(
        title = "The pod specification",
        description = "Kubernetes pod specification defining containers, volumes, restart policy, and other pod settings.\n" +
            "Must include at least one container. Supports dynamic template expressions including the special\n" +
            "{{workingDir}} variable which resolves to '/kestra/working-dir' when inputFiles or outputFiles are used."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Map<String, Object> spec;

    @Schema(
        title = "Whether to delete the pod after task completion",
        description = "When true (default), the pod is automatically deleted after successful completion or failure.\n" +
            "Set to false to keep the pod for debugging purposes. Note that pods are always deleted when the task is killed."
    )
    @NotNull
    @Builder.Default
    private final Property<Boolean> delete = Property.ofValue(true);

    @Schema(
        title = "Whether to resume execution of an existing pod",
        description = "When true (default), attempts to reconnect to an existing pod with matching taskrun ID and attempt count\n" +
            "instead of creating a new pod. This enables recovery from interrupted executions.\n" +
            "If no matching pod exists or multiple matching pods are found, a new pod is created."
    )
    @NotNull
    @Builder.Default
    private final Property<Boolean> resume = Property.ofValue(true);

    @Schema(
        title = "Additional time to wait for late-arriving logs after pod completion",
        description = "After the pod completes and initial log collection finishes, wait this duration to capture any\n" +
            "remaining logs that may still be in transit. Defaults to 30 seconds.\n" +
            "Useful as a safety net for high-throughput scenarios where logs may arrive slightly delayed."
    )
    @NotNull
    @Builder.Default
    private Property<Duration> waitForLogInterval = Property.ofValue(Duration.ofSeconds(30));

    // Constants for file paths and working directory
    private static final String KESTRA_WORKING_DIR = "/kestra/working-dir";
    private static final String WORKING_DIR_VAR = "workingDir";
    private static final String OUTPUT_FILES_VAR = "outputFiles";

    // Kill handling state
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicReference<String> currentPodName = new AtomicReference<>();
    private volatile String currentNamespace;
    private volatile Connection currentConnection;

    /**
     * Executes the pod creation task and manages its complete lifecycle.
     *
     * <p>This method orchestrates the following workflow:
     * <ol>
     *   <li>Validates and prepares input files (if specified)</li>
     *   <li>Creates a new pod or resumes an existing one (if resume=true)</li>
     *   <li>Waits for pod initialization and uploads input files</li>
     *   <li>Starts log streaming from all containers</li>
     *   <li>Waits for pod completion with timeout enforcement</li>
     *   <li>Downloads output files (if specified)</li>
     *   <li>Cleans up and deletes the pod (if delete=true)</li>
     * </ol>
     *
     * <p><b>Thread Safety:</b> This method can be interrupted via {@link #kill()}, which will attempt
     * to delete the pod. The interrupt flag is temporarily cleared during cleanup to ensure
     * pod deletion succeeds even when the thread is interrupted.
     *
     * <p><b>Error Handling:</b> If the pod fails or times out, this method throws an exception
     * after attempting cleanup. The pod is deleted (if delete=true) regardless of success or failure.
     *
     * @param runContext the execution context providing access to variables, storage, and logging
     * @return Output containing pod metadata, status, output files, and any variables extracted from logs
     * @throws Exception if pod creation fails, times out, or terminates with a non-zero exit code
     * @throws IllegalStateException if input files are invalid or if pod fails to reach Running state
     * @throws InterruptedException if the task is interrupted and returns null
     * @see #kill() for handling task interruption
     */
    @Override
    public PodCreate.Output run(RunContext runContext) throws Exception {

        Duration rWaitUntilRunning = runContext.render(this.waitUntilRunning).as(Duration.class).orElseThrow();

        try {
            this.currentConnection = this.getConnection();

            super.init(runContext);
            Map<String, Object> additionalVars = new HashMap<>();
            additionalVars.put(WORKING_DIR_VAR, KESTRA_WORKING_DIR);
            var outputFilesList = runContext.render(this.outputFiles).asList(String.class);
            if (!outputFilesList.isEmpty()) {
                Map<String, Object> outputFileVariables = new HashMap<>();
                outputFilesList.forEach(file -> outputFileVariables.put(file, KESTRA_WORKING_DIR + "/" + file));
                additionalVars.put(OUTPUT_FILES_VAR, outputFileVariables);
            }

            String namespace = runContext.render(this.namespace).as(String.class).orElseThrow();
            Logger logger = runContext.logger();

            this.currentNamespace = namespace;

            // Validate and prepare inputFiles BEFORE creating any Kubernetes resources
            // This provides fast failure with clear error messages instead of waiting for pod timeout
            Map<String, String> validatedInputFiles = null;
            if (this.inputFiles != null) {
                validatedInputFiles = PluginUtilsService.transformInputFiles(runContext, additionalVars, this.inputFiles);

                // Check for null/empty values that would cause pod to hang indefinitely
                for (Map.Entry<String, String> entry : validatedInputFiles.entrySet()) {
                    if (entry.getValue() == null || entry.getValue().isEmpty()) {
                        throw new IllegalStateException(
                            "Input file '" + entry.getKey() + "' references a null or empty value. " +
                            "Please verify that the upstream task output exists.");
                    }
                }

                // Pre-create files to validate accessibility before pod creation
                PluginUtilsService.createInputFiles(
                    runContext,
                    PodService.tempDir(runContext),
                    validatedInputFiles,
                    additionalVars
                );
            }

            try (KubernetesClient client = PodService.client(runContext, this.getConnection());
                 PodLogService podLogService = new PodLogService(((DefaultRunContext) runContext).getApplicationContext().getBean(ThreadMainFactoryBuilder.class))) {

                Pod pod = findOrCreatePod(runContext, client, namespace, additionalVars, logger);
                currentPodName.set(pod.getMetadata().getName());

                try (Watch ignored = PodService.podRef(client, pod).watch(listOptions(runContext), new PodWatcher(logger))) {
                    try {
                        // in case of resuming an already running pod, the status will be running
                        if (!"Running".equals(pod.getStatus().getPhase())) {
                            // wait for init container and upload files
                            if (validatedInputFiles != null) {
                                // Files already validated and created before pod creation
                                // Just need to wait for init container and upload them
                                pod = PodService.waitForInitContainerRunning(client, pod, INIT_FILES_CONTAINER_NAME, rWaitUntilRunning);
                                this.uploadInputFiles(runContext, PodService.podRef(client, pod), logger, validatedInputFiles.keySet());
                            }

                            // wait for pods ready
                            pod = PodService.waitForPodReady(client, pod, rWaitUntilRunning);
                        }

                        if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed")) {
                            throw PodService.failedMessage(pod);
                        }

                        // Wait for containers to start (Running) or pod to reach terminal state (Succeeded/Failed/Unknown)
                        // This ensures we proceed with log collection regardless of pod outcome
                        pod = PodService.waitForContainersStartedOrCompleted(client, pod, rWaitUntilRunning);

                        // Set up log consumer for output parsing (used by both watch and fetchFinalLogs)
                        AbstractLogConsumer logConsumer = new DefaultLogConsumer(runContext);
                        podLogService.setLogConsumer(logConsumer);

                        // Only start log streaming if pod is actually running
                        // For pods that complete quickly, fetchFinalLogs will handle log collection
                        if (pod.getStatus() != null && "Running".equals(pod.getStatus().getPhase())) {
                            podLogService.watch(client, pod, logConsumer, runContext);
                        }

                        // wait until completion of the pods
                        Pod ended;
                        var waitRunningValue = runContext.render(this.waitRunning).as(Duration.class).orElseThrow();

                        if (this.outputFiles != null) {
                            ended = PodService.waitForCompletionExcept(client, logger, pod, waitRunningValue, SIDECAR_FILES_CONTAINER_NAME);
                        } else {
                            ended = waitForCompletion(client, logger, pod, waitRunningValue);
                        }

                        // Collect late logs and check for failures (throws if container failed)
                        handleEnd(ended, runContext, this.outputFiles != null, client, podLogService);

                        PodStatus podStatus = PodStatus.from(ended.getStatus());
                        Output.OutputBuilder output = Output.builder()
                            .metadata(Metadata.from(ended.getMetadata()))
                            .status(podStatus)
                            .vars(logConsumer.getOutputs());

                        // Download output files if configured and task succeeded
                        if (this.outputFiles != null) {
                            Map<Path, Path> pathMap = this.downloadOutputFiles(runContext, PodService.podRef(client, pod), logger, additionalVars);
                            output.outputFiles(outputFiles(runContext, runContext.render(this.outputFiles).asList(String.class), pathMap));
                        }

                        return output
                            .build();
                    } finally {
                        // Signal sidecar to exit gracefully before pod deletion
                        // The sidecar container only exists when outputFiles is configured
                        boolean hasSidecar = pod.getSpec().getContainers().stream()
                            .anyMatch(container -> SIDECAR_FILES_CONTAINER_NAME.equals(container.getName()));

                        if (hasSidecar) {
                            try {
                                PodService.uploadMarker(runContext, PodService.podRef(client, pod),
                                                       logger, ENDED_MARKER, SIDECAR_FILES_CONTAINER_NAME);
                                logger.debug("Signaled sidecar to exit gracefully before pod deletion");
                            } catch (Exception markerException) {
                                // Failure is acceptable - pod might already be terminating
                                logger.debug("Could not signal sidecar (pod may be terminating): {}", markerException.getMessage());
                            }
                        }

                        // Delete the pod if not already killed externally (kill() deletes the pod itself)
                        if (!killed.get()) {
                            // Clear interrupt status before deletion to ensure the delete operation can complete
                            // When a task timeout occurs, the thread is interrupted, but we still need to clean up resources
                            boolean wasInterrupted = Thread.interrupted();
                            try {
                                delete(client, logger, pod, runContext);
                            } finally {
                                // Restore interrupt status after cleanup
                                if (wasInterrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    }
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

    /**
     * Finds an existing pod to resume or creates a new pod.
     *
     * <p>When {@code resume=true}, this method searches for an existing pod matching the current
     * taskrun ID and attempt count. If exactly one matching pod is found, it is returned for resumption.
     * If no pod or multiple pods are found, a new pod is created.
     *
     * @param runContext execution context for rendering properties and accessing variables
     * @param client Kubernetes client for pod operations
     * @param namespace namespace to search for or create the pod in
     * @param additionalVars additional variables to use for pod template rendering
     * @param logger logger for diagnostic messages
     * @return the pod to use (either found for resumption or newly created)
     * @throws Exception if pod creation fails or label selector construction fails
     */
    private Pod findOrCreatePod(
        RunContext runContext,
        KubernetesClient client,
        String namespace,
        Map<String, Object> additionalVars,
        Logger logger
    ) throws Exception {
        if (runContext.render(this.resume).as(Boolean.class).orElseThrow()) {
            // Try to locate an existing pod for this taskrun and attempt
            // Safe cast: runContext.getVariables() returns Map<String, Object> where "taskrun" is always a Map
            @SuppressWarnings("unchecked")
            Map<String, Object> taskrun = (Map<String, Object>) runContext.getVariables().get("taskrun");
            String taskrunId = ScriptService.normalize(String.valueOf(taskrun.get("id")));
            String attempt = ScriptService.normalize(String.valueOf(taskrun.get("attemptsCount")));
            String labelSelector = "kestra.io/taskrun-id=" + taskrunId + "," + "kestra.io/taskrun-attempt=" + attempt;

            var existingPods = client.pods()
                .inNamespace(namespace)
                .list(new ListOptionsBuilder().withLabelSelector(labelSelector).build());

            if (existingPods.getItems().size() == 1) {
                Pod pod = existingPods.getItems().get(0);
                logger.info("Pod '{}' is resumed from an already running pod", pod.getMetadata().getName());
                return pod;
            } else if (!existingPods.getItems().isEmpty()) {
                logger.warn("More than one pod exists for the label selector {}, no pods will be resumed.", labelSelector);
            }
        }

        // Create new pod if resume is disabled or no suitable pod was found
        Pod pod = createPod(runContext, client, namespace, additionalVars);
        logger.info("Pod '{}' is created", pod.getMetadata().getName());
        return pod;
    }

    /**
     * Maps output files from the pod's working directory to Kestra internal storage.
     *
     * <p>This method is adapted from {@code FilesService.outputFiles()} to handle file paths
     * containing special characters (spaces, unicode) that are URL-encoded during transfer
     * from Kubernetes pods. The {@code pathMap} parameter preserves the mapping between
     * sanitized paths (used internally) and original unsanitized paths (for user visibility).
     *
     * <p><b>Why this exists:</b> When files are downloaded from Kubernetes pods via the exec API,
     * paths containing spaces or special characters are URL-encoded. This method uses the pathMap
     * to restore original file names when storing files in Kestra, ensuring users see the correct
     * file names in task outputs.
     *
     * @param runContext execution context for storage access and template rendering
     * @param outputs list of glob patterns matching files to capture (supports wildcards like {@code *.txt})
     * @param pathMap mapping from sanitized (encoded) paths to original unsanitized relative paths
     * @return map of original file paths to their storage URIs in Kestra's internal storage
     * @throws Exception if file matching fails, storage upload fails, or pathMap is missing entries
     * @throws IllegalStateException if a file path has no corresponding entry in pathMap
     */
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
                logger.info("Pod '{}' is deleted", pod.getMetadata().getName());
            } catch (Throwable e) {
                logger.warn("Unable to delete pod '{}'", pod.getMetadata().getName(), e);
            }
        }
    }

    private void handleEnd(Pod ended, RunContext runContext, boolean hasOutputFiles, KubernetesClient client, PodLogService podLogService) throws Exception {
        Logger logger = runContext.logger();

        // Wait for async log stream (watchLog) to finish processing
        Thread.sleep(runContext.render(this.waitForLogInterval).as(Duration.class).orElseThrow().toMillis());

        // Fetch any remaining logs that the watch stream may have missed
        // Check if pod still exists after sleep (may have been deleted during sleep window)
        Pod currentPod = PodService.podRef(client, ended).get();
        if (currentPod != null) {
            podLogService.fetchFinalLogs(client, ended, runContext);
        } else {
            logger.debug("Pod '{}' was already deleted, skipping fetchFinalLogs", ended.getMetadata().getName());
        }

        // Check for failures based on whether outputFiles are configured
        if (hasOutputFiles) {
            // For pods with outputFiles, check container exit codes
            // (pod phase stays "Running" due to sidecar container)
            PodService.checkContainerFailures(ended, SIDECAR_FILES_CONTAINER_NAME, logger);
        } else if (ended.getStatus() != null && ended.getStatus().getPhase().equals("Failed")) {
            // For pods without outputFiles, check pod phase
            throw PodService.failedMessage(ended);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The pod metadata"
        )
        private final Metadata metadata;

        @Schema(
            title = "The pod status"
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

    /**
     * Terminates the running pod immediately when the task is killed.
     *
     * <p>This method is called by the Kestra framework when a task needs to be terminated,
     * typically due to:
     * <ul>
     *   <li>Task-level timeout configured in the flow</li>
     *   <li>Manual task cancellation by a user</li>
     *   <li>Execution cancellation</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> This method uses {@link AtomicBoolean#compareAndSet(boolean, boolean)}
     * to ensure the pod is deleted exactly once, even if kill() is called multiple times or
     * concurrently from different threads.
     *
     * <p><b>Grace Period:</b> The pod is deleted with a grace period of 0 seconds, which forces
     * immediate termination. This is intentional for kill operations to stop the pod as quickly
     * as possible without waiting for graceful shutdown.
     *
     * <p><b>Error Handling:</b> If pod deletion fails (e.g., pod already deleted, API error),
     * the error is logged but not thrown, allowing the kill operation to complete without
     * disrupting the task cancellation process.
     *
     * @see #run(RunContext) for the main execution flow that this method interrupts
     */
    @Override
    public void kill() {
        if (!killed.compareAndSet(false, true)) {
            return;
        }

        if (currentPodName.get() == null || currentNamespace == null) {
            log.debug("Kill called but pod context not available.");
            return;
        }

        log.warn("Task was killed, deleting the pod '{}' in namespace '{}'.", currentPodName.get(), currentNamespace);

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