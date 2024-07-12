package io.kestra.plugin.kubernetes.runner;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.CopyOrReadable;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.*;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ListUtils;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import io.kestra.plugin.kubernetes.models.Connection;
import io.kestra.plugin.kubernetes.services.PodLogService;
import io.kestra.plugin.kubernetes.services.PodService;
import io.kestra.plugin.kubernetes.watchers.PodWatcher;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.plugin.kubernetes.services.PodService.withRetries;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Task runner that executes a task inside a pod in a Kubernetes cluster.",
    description = """
        This task runner is container-based so the `containerImage` property must be set to be able to use it.
        
        To access the task's working directory, use the `{{workingDir}}` Pebble expression or the `WORKING_DIR` environment variable. Input files and namespace files will be available in this directory.

        To generate output files you can either use the `outputFiles` task's property and create a file with the same name in the task's working directory, or create any file in the output directory which can be accessed by the `{{outputDir}}` Pebble expression or the `OUTPUT_DIR` environment variables.
        
        Note that when the Kestra Worker running this task is terminated, the pod will still runs until completion, then after restarting, the Worker will resume processing on the existing pod unless `resume` is set to false.
        
        If your cluster is configure with [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/), you need to configure the service account running your pod need to have the following authorizations: 
        - pods: get, create, delete, watch, list
        - pods/log: get, watch
        As an example, here is a role that grant those authorizations:
        ```yaml
        apiVersion: rbac.authorization.k8s.io/v1
        kind: Role
        metadata:
          name: task-runner
        rules:
        - apiGroups: [""]
          resources: ["pods"]
          verbs: ["get", "create", "delete", "watch", "list"]
        - apiGroups: [""]
          resources: ["pods/logs"]
          verbs: ["get", "watch"]
        ```"""
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a Shell command.",
            code = """
                id: new-shell
                namespace: company.team
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    taskRunner:
                      type: io.kestra.plugin.kubernetes.runner.Kubernetes
                    commands:
                      - echo "Hello World\"""",
            full = true
        ),
        @Example(
            title = "Pass input files to the task, execute a Shell command, then retrieve output files.",
            code = """
                id: new-shell-with-file
                namespace: company.team
                
                inputs:
                  - id: file
                    type: FILE
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    inputFiles:
                      data.txt: "{{ inputs.file }}"
                    outputFiles:
                      - out.txt
                    containerImage: centos
                    taskRunner:
                      type: io.kestra.plugin.kubernetes.runner.Kubernetes
                    commands:
                      - cp {{ workingDir }}/data.txt {{ workingDir }}/out.txt""",
            full = true
        )
    }
)
public class Kubernetes extends TaskRunner implements RemoteRunnerInterface {
    private static final String INIT_FILES_CONTAINER_NAME = "init-files";
    private static final String FILES_VOLUME_NAME = "kestra-files";
    private static final String SIDECAR_FILES_CONTAINER_NAME = "out-files";
    private static final String MAIN_CONTAINER_NAME = "main";
    private static final Path WORKING_DIR = Path.of("/kestra/working-dir");

    @Schema(
        title = "The configuration of the target Kubernetes cluster."
    )
    @PluginProperty
    private Connection config;

    @NotNull
    @PluginProperty
    @Builder.Default
    private String pullPolicy = "Always";

    @Schema(
        title = "The namespace where the pod will be created."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    private String namespace = "default";

    @Schema(
        title = "The pod custom labels",
        description = "Kestra will add default labels to the pod with execution and flow identifiers."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> labels;

    @Schema(
            title = "Node selector for pod scheduling",
            description = "Kestra will assign the pod to the nodes you want (see https://kubernetes.io/docs/tasks/configure-pod-container/assign-pods-nodes/)"
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> nodeSelector;

    @Schema(
        title = "The pod custom resources"
    )
    @PluginProperty
    private Resources resources;

    @Schema(
        title = "The maximum duration to wait until the pod is created.",
        description = "This timeout is the maximum time that Kubernetes scheduler can take to\n" +
            "* schedule the pod\n" +
            "* pull the pod image\n" +
            "* and start the pod."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private Duration waitUntilRunning = Duration.ofMinutes(10);

    @Schema(
        title = "The maximum duration to wait for the pod completion unless the task `timeout` property is set which will take precedence over this property."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "Whether the pod should be deleted upon completion."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private Boolean delete = true;

    @Schema(
        title = "Whether to reconnect to the current pod if it already exists."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private Boolean resume = true;

    @Schema(
        title = "The configuration of the file sidecar container that handle download and upload of files."
    )
    @PluginProperty
    @Builder.Default
    private SideCar fileSidecar = SideCar.builder().build();

    @Schema(
        title = "The additional duration to wait for logs to arrive after pod completion.",
        description = "As logs are not retrieved in real time, we cannot guarantee that we have fetched all logs when the pod complete, therefore we wait for a fixed amount of time to fetch late logs."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private Duration waitForLogs = Duration.ofSeconds(1);

    @Override
    public RunnerResult run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        if(!PodService.tempDir(runContext).toFile().mkdir()) {
            throw new IOException("Unable to create the temp directory");
        }
        String namespace = runContext.render(this.namespace);
        Logger logger = runContext.logger();

        AbstractLogConsumer defaultLogConsumer = taskCommands.getLogConsumer();
        try (var client = PodService.client(runContext, config);
             var podLogService = new PodLogService(((DefaultRunContext)runContext).getApplicationContext().getBean(ThreadMainFactoryBuilder.class))) {
            Pod pod = null;
            PodResource resource = null;

            if (this.resume) {
                // try to locate an existing pod for this taskrun and attempt
                Map<String, String> taskrun = (Map<String, String>) runContext.getVariables().get("taskrun");
                String taskrunId = ScriptService.normalize(taskrun.get("id"));
                String attempt =  ScriptService.normalize(String.valueOf(taskrun.get("attemptsCount")));
                String labelSelector = "kestra.io/taskrun-id=" + taskrunId + "," + "kestra.io/taskrun-attempt=" + attempt;
                var existingPods = client.pods().inNamespace(namespace).list(new ListOptionsBuilder().withLabelSelector(labelSelector).build());
                if (existingPods.getItems().size() == 1) {
                    pod = existingPods.getItems().get(0);
                    resource = PodService.podRef(client, pod);
                    logger.info("Pod '{}' is resumed from an already running pod ", pod.getMetadata().getName());

                } else if (!existingPods.getItems().isEmpty()) {
                    logger.warn("More than one pod exist for the label selector {}, no pods will be resumed.", labelSelector);
                }
            }

            List<Path> filesToUploadWithOutputDir = new ArrayList<>(taskCommands.relativeWorkingDirectoryFilesPaths());

            Map<String, Object> additionalVars = this.additionalVars(runContext, taskCommands);
            Path outputDirPath = (Path) additionalVars.get(ScriptService.VAR_OUTPUT_DIR);
            boolean outputDirectoryEnabled = taskCommands.outputDirectoryEnabled();
            if (outputDirectoryEnabled) {
                Path outputDirName = WORKING_DIR.relativize(outputDirPath);
                runContext.workingDir().resolve(outputDirName).toFile().mkdir();
                filesToUploadWithOutputDir.add(outputDirName);
            }
            if (pod == null) {
                Container container = createContainer(runContext, taskCommands);
                pod = createPod(runContext, container, !ListUtils.isEmpty(filesToUploadWithOutputDir), !ListUtils.isEmpty(filesToDownload) || outputDirectoryEnabled);
                resource = client.pods().inNamespace(namespace).resource(pod);
                pod = resource.create();

                final String podName = pod.getMetadata().getName();
                onKill(() -> safelyKillPod(runContext, config, namespace, podName));
                logger.info("Pod '{}' is created ", podName);
            }

            try (var podWatch = resource.watch(new PodWatcher(logger))) {
                // in case of resuming an already running pod, the status will be running
                if (!"Running".equals(pod.getStatus().getPhase())) {
                    if (!ListUtils.isEmpty(filesToUploadWithOutputDir)) {
                        // wait for init container
                        pod = PodService.waitForInitContainerRunning(client, pod, INIT_FILES_CONTAINER_NAME, this.waitUntilRunning);
                        this.uploadInputFiles(runContext, PodService.podRef(client, pod), logger, filesToUploadWithOutputDir);
                    }

                    // wait for pod ready
                    pod = PodService.waitForPodReady(client, pod, this.waitUntilRunning);
                    if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed")) {
                        throw PodService.failedMessage(pod);
                    }
                }

                // watch log
                podLogService.watch(client, pod, defaultLogConsumer, runContext);

                // wait for terminated
                Duration waitDuration = Optional.ofNullable(taskCommands.getTimeout()).orElse(this.waitUntilCompletion);
                if (!ListUtils.isEmpty(filesToDownload) || outputDirectoryEnabled) {
                    pod = PodService.waitForCompletionExcept(client, logger, pod, waitDuration, SIDECAR_FILES_CONTAINER_NAME);
                    this.downloadOutputFiles(runContext, PodService.podRef(client, pod), logger, taskCommands, outputDirPath);
                } else {
                    pod = PodService.waitForCompletion(client, logger, pod, waitDuration);
                }

                // wait for logs to arrive
                Thread.sleep(waitForLogs.toMillis());

                // handle exception
                if (pod.getStatus() == null) {
                    runContext.logger().error("Pod terminated without any status, failing the task.");
                    throw new TaskException(-1, defaultLogConsumer.getStdOutCount(), defaultLogConsumer.getStdErrCount());
                }

                if (pod.getStatus().getContainerStatuses() == null) {
                    runContext.logger().error("Pod terminated without any container statuses, failing the task.");
                    throw new TaskException(-1, defaultLogConsumer.getStdOutCount(), defaultLogConsumer.getStdErrCount());
                }

                pod.getStatus().getContainerStatuses().stream()
                    .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null && containerStatus.getState().getTerminated().getExitCode() != 0)
                    .map(containerStatus -> containerStatus.getState().getTerminated())
                    .findFirst()
                    .ifPresent(throwConsumer(containerStateTerminated -> {
                        throw new TaskException(containerStateTerminated.getMessage(), containerStateTerminated.getExitCode(), defaultLogConsumer.getStdOutCount(), defaultLogConsumer.getStdErrCount());
                    }));

                podWatch.close();
                podLogService.close();

                return new RunnerResult(
                    pod.getStatus().getContainerStatuses().get(0).getState().getTerminated().getExitCode(),
                    defaultLogConsumer
                );
            } finally {
                if (this.delete) {
                    try {
                        resource.delete();
                        logger.info("Pod '{}' is deleted ", pod.getMetadata().getName());
                    } catch (Throwable e) {
                        logger.warn("Unable to delete pod {}", pod.getFullResourceName(), e);
                    }
                }
            }
        }
    }

    private void safelyKillPod(final RunContext runContext,
                               final Connection connection,
                               final String namespace,
                               final String podName) {
        // Use a dedicated KubernetesClient, as the one used in the run method may be closed in the meantime.
        try (KubernetesClient client = PodService.client(runContext, connection)) {
            client.pods()
                .inNamespace(namespace)
                .withName(podName)
                .withGracePeriod(0L) // delete immediately
                .delete();
            runContext.logger().info("Pod '{}' in namespace '{}' is deleted.", podName, namespace);
        } catch (Throwable e) {
            runContext.logger().warn("Failed to delete pod '{}' in namespace '{}'.", podName, namespace, e);
        }
    }

    private Container createContainer(RunContext runContext, TaskCommands taskCommands) throws IllegalVariableEvaluationException {
        List<EnvVar> env = this.env(runContext, taskCommands).entrySet().stream()
            .map(entry -> new EnvVarBuilder().withName(entry.getKey()).withValue(entry.getValue()).build())
            .collect(Collectors.toList());

        var builder = new ContainerBuilder()
            .withName(MAIN_CONTAINER_NAME)
            .withImage(runContext.render(taskCommands.getContainerImage(), this.additionalVars(runContext, taskCommands)))
            .withImagePullPolicy(this.pullPolicy)
            .withEnv(env)
            .withCommand(taskCommands.getCommands());

        if (this.resources != null) {
            builder.withResources(
                new ResourceRequirementsBuilder()
                    .withLimits(buildResource(this.resources.limit))
                    .withRequests(buildResource(this.resources.request))
                    .build()
            );
        }

        return builder.build();
    }

    private Map<String, Quantity> buildResource(Resource resource) {
        if (resource == null) {
            return null;
        }

        Map<String, Quantity> quantities = new HashMap<>();
        if (resource.cpu != null) {
            quantities.put("cpu", new Quantity(resource.cpu));
        }
        if (resource.memory != null) {
            quantities.put("memory", new Quantity(resource.memory));
        }
        return quantities;
    }

    private Pod createPod(RunContext runContext, Container mainContainer, boolean initContainer, boolean sidecarContainer) throws IllegalVariableEvaluationException {
        VolumeMount volumeMount = new VolumeMountBuilder()
            .withMountPath("/kestra")
            .withName(FILES_VOLUME_NAME)
            .build();

        var spec = new PodSpecBuilder()
            .withContainers(mainContainer)
            .withRestartPolicy("Never")
            .build();

        if (sidecarContainer) {
            spec.getContainers()
                .add(filesContainer(runContext, volumeMount, true));
        }

        if (initContainer) {
            spec.getInitContainers()
                .add(filesContainer(runContext, volumeMount, false));
        }

        if (initContainer || sidecarContainer) {
            spec.getContainers()
                .forEach(container -> {
                    List<VolumeMount> volumeMounts = container.getVolumeMounts();
                    volumeMounts.add(volumeMount);
                    container.setVolumeMounts(volumeMounts);
                    container.setWorkingDir(WORKING_DIR.toString());
                });

            spec.getVolumes()
                .add(new VolumeBuilder()
                    .withName(FILES_VOLUME_NAME)
                    .withNewEmptyDir()
                    .endEmptyDir()
                    .build()
                );
        }

        Map<String, String> allNodeSelector = this.nodeSelector == null ? new HashMap<>() : runContext.renderMap(this.nodeSelector);
        if (!allNodeSelector.isEmpty()){
            spec.setNodeSelector(allNodeSelector);
        }

        Map<String, String> allLabels = this.labels == null ? new HashMap<>() : runContext.renderMap(this.labels);
        allLabels.putAll(ScriptService.labels(runContext, "kestra.io/"));
        var metadata = new ObjectMetaBuilder()
            .withName(ScriptService.jobName(runContext))
            .withLabels(allLabels)
            .build();

        return new PodBuilder()
            .withSpec(spec)
            .withMetadata(metadata)
            .build();
    }

    private void uploadInputFiles(RunContext runContext, PodResource podResource, Logger logger, List<Path> inputFilesPaths) throws IOException {
        inputFilesPaths.forEach(
            throwConsumer(path -> withRetries(
                logger,
                "uploadInputFiles",
                () -> {
                    Path filePath = runContext.workingDir().resolve(path);
                    ContainerResource containerResource = podResource
                        .inContainer(INIT_FILES_CONTAINER_NAME)
                        .withReadyWaitTimeout(0);

                    String containerFilePath = WORKING_DIR.resolve(path).toString();
                    CopyOrReadable toUpload;
                    if (filePath.toFile().isDirectory()) {
                        toUpload = containerResource.dir(containerFilePath);
                    } else {
                        toUpload = containerResource.file(containerFilePath);
                    }
                    return toUpload.upload(filePath);
                }
            ))
        );

        PodService.uploadMarker(runContext, podResource, logger, "ready", INIT_FILES_CONTAINER_NAME);
    }

    protected void downloadOutputFiles(RunContext runContext, PodResource podResource, Logger logger, TaskCommands taskCommands, Path containerOutputDir) throws Exception {
        Path workingDirectory = taskCommands.getWorkingDirectory();
        withRetries(
            logger,
            "downloadOutputFiles",
            () -> podResource
                .inContainer(SIDECAR_FILES_CONTAINER_NAME)
                .dir(WORKING_DIR.toString())
                .copy(workingDirectory)
        );

        // kubernetes copy by keeping the target repository which we don't want, so we move the files
        String containerWorkingDirAsRelativePath = WORKING_DIR.toString().substring(1);
        try (Stream<Path> files = Files.walk(workingDirectory.resolve(containerWorkingDirAsRelativePath))) {
            files
                .filter(path -> !Files.isDirectory(path) && Files.isReadable(path))
                .forEach(throwConsumer(outputFile -> {
                    // If the file was in the container outputDir, we forward it to the task commands' outputDir
                    if (containerOutputDir != null && outputFile.startsWith(workingDirectory.resolve(containerOutputDir.toString().substring(1)))) {
                        Path relativePathFromContainerWDir = workingDirectory.resolve(containerOutputDir.toString().substring(1)).relativize(outputFile);
                        Path resolvedOutputFile = taskCommands.getOutputDirectory().resolve(relativePathFromContainerWDir);
                        moveFile(outputFile, resolvedOutputFile);
                    } else {
                        Path relativePathFromContainerWDir = workingDirectory.resolve(containerWorkingDirAsRelativePath).relativize(outputFile);
                        Path resolvedOutputFile = workingDirectory.resolve(relativePathFromContainerWDir);
                        moveFile(outputFile, resolvedOutputFile);
                    }
                }));
        }

        PodService.uploadMarker(runContext, podResource, logger, "ended", SIDECAR_FILES_CONTAINER_NAME);
    }

    private void moveFile(Path from, Path to) throws IOException {
        if (Files.notExists(to.getParent())) {
            Files.createDirectories(to.getParent());
        }
        Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
    }

    private Container filesContainer(RunContext runContext, VolumeMount volumeMount, boolean finished) throws IllegalVariableEvaluationException {
        String s = finished ? "ended" : "ready";

        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName(finished ? SIDECAR_FILES_CONTAINER_NAME : INIT_FILES_CONTAINER_NAME)
            .withImage(fileSidecar != null ? runContext.render(fileSidecar.getImage()) : "busybox")
            .withCommand(Arrays.asList(
                "sh",
                "-c",
                "echo 'waiting to be " + s + "!'\n" +
                    "while [ ! -f /kestra/" + s + " ]\n" +
                    "do\n" +
                    "  sleep 0.5\n" +
                    (finished ? "" : "echo '* still waiting!'\n") +
                    "done\n" +
                    "echo '" + s + " successfully'\n"
            ));

        if (!finished) {
            containerBuilder.withVolumeMounts(Collections.singletonList(volumeMount));
        }

        return containerBuilder.build();
    }

    @Override
    protected Map<String, Object> runnerAdditionalVars(RunContext runContext, TaskCommands taskCommands) {
        Map<String, Object> vars = new HashMap<>();
        vars.put(ScriptService.VAR_WORKING_DIR, WORKING_DIR);

        if (taskCommands.outputDirectoryEnabled()) {
            vars.put(ScriptService.VAR_OUTPUT_DIR, WORKING_DIR.resolve(IdUtils.create()));
        }

        return vars;
    }

    @Getter
    @Builder
    public static class Resources {
        private Resource request;
        private Resource limit;
    }

    @Getter
    @Builder
    public static class Resource {
        private String memory;
        private String cpu;
    }
}
