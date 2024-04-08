package io.kestra.plugin.kubernetes.runner;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.CopyOrReadable;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import io.kestra.plugin.kubernetes.services.PodLogService;
import io.kestra.plugin.kubernetes.services.PodService;
import io.kestra.plugin.kubernetes.watchers.PodWatcher;
import io.micronaut.core.annotation.Introspected;
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

@Introspected
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
         
        Note that when the Kestra Worker running this task is terminated, the pod will still runs until completion, then after restarting, the Worker will resume processing on the existing pod unless `resume` is set to false."""
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a Shell command.",
            code = """
                id: new-shell
                namespace: myteam
                                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    containerImage: centos
                    taskRunner:
                      type: io.kestra.plugin.kubernetes.runner.KubernetesTaskRunner
                    commands:
                    - echo "Hello World\"""",
            full = true
        ),
        @Example(
            title = "Pass input files to the task, execute a Shell command, then retrieve output files.",
            code = """
                id: new-shell-with-file
                namespace: myteam
                                
                inputs:
                  - id: file
                    type: FILE
                                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    inputFiles:
                      data.txt: "{{inputs.file}}"
                    outputFiles:
                      - out.txt
                    containerImage: centos
                    taskRunner:
                      type: io.kestra.plugin.kubernetes.runner.KubernetesTaskRunner
                    commands:
                    - cp {{workingDir}}/data.txt {{workingDir}}/out.txt""",
            full = true
        )
    },
    beta = true // all task runners are beta for now, but this one is stable as it was the one used before
)
public class KubernetesTaskRunner extends TaskRunner implements RemoteRunnerInterface {
    private static final String INIT_FILES_CONTAINER_NAME = "init-files";
    private static final String FILES_VOLUME_NAME = "kestra-files";
    private static final String SIDECAR_FILES_CONTAINER_NAME = "out-files";
    private static final String MAIN_CONTAINER_NAME = "main";
    private static final Path WORKING_DIR = Path.of("/kestra/working-dir");

    @Schema(
        title = "The configuration of the target Kubernetes cluster."
    )
    @PluginProperty
    private Config config;

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
    private Duration waitUntilRunning = Duration.ofMinutes(10);

    @Schema(
        title = "The maximum duration to wait for the pod completion."
    )
    @NotNull
    @Builder.Default
    private Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "Whether the pod should be deleted upon completion."
    )
    @NotNull
    @Builder.Default
    private Boolean delete = true;

    @Schema(
        title = "Whether to reconnect to the current pod if it already exists."
    )
    @NotNull
    @Builder.Default
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
    private Duration waitForLogs = Duration.ofSeconds(1);

    @Override
    public RunnerResult run(RunContext runContext, TaskCommands taskCommands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        if(!PodService.tempDir(runContext).toFile().mkdir()) {
            throw new IOException("Unable to create the temp directory");
        }
        String namespace = runContext.render(this.namespace);
        Logger logger = runContext.logger();

        AbstractLogConsumer defaultLogConsumer = taskCommands.getLogConsumer();
        try (var client = PodService.client(convert(runContext, config));
             var podLogService = new PodLogService(runContext.getApplicationContext().getBean(ThreadMainFactoryBuilder.class))) {
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

            List<String> filesToUploadWithOutputDir = new ArrayList<>(filesToUpload);
            Map<String, Object> additionalVars = this.additionalVars(runContext, taskCommands);
            Path outputDirPath = (Path) additionalVars.get(ScriptService.VAR_OUTPUT_DIR);
            Path outputDirName = WORKING_DIR.relativize(outputDirPath);
            runContext.resolve(outputDirName).toFile().mkdir();
            filesToUploadWithOutputDir.add(outputDirName.toString());
            if (pod == null) {
                Container container = createContainer(runContext, taskCommands);
                pod = createPod(runContext, container);
                resource = client.pods().inNamespace(namespace).resource(pod);
                pod = resource.create();
                logger.info("Pod '{}' is created ", pod.getMetadata().getName());
            }

            try (var podWatch = resource.watch(new PodWatcher(logger))) {
                // in case of resuming an already running pod, the status will be running
                if (!"Running".equals(pod.getStatus().getPhase())) {
                    // wait for init container
                    pod = PodService.waitForInitContainerRunning(client, pod, INIT_FILES_CONTAINER_NAME, this.waitUntilRunning);
                    this.uploadInputFiles(runContext, PodService.podRef(client, pod), logger, filesToUploadWithOutputDir);

                    // wait for pod ready
                    pod = PodService.waitForPodReady(client, pod, this.waitUntilRunning);
                    if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed")) {
                        throw PodService.failedMessage(pod);
                    }
                }

                // watch log
                podLogService.watch(client, pod, defaultLogConsumer, runContext);

                // wait for terminated
                pod = PodService.waitForCompletionExcept(client, logger, pod, this.waitUntilCompletion, SIDECAR_FILES_CONTAINER_NAME);
                this.downloadOutputFiles(runContext, PodService.podRef(client, pod), logger, taskCommands, outputDirPath);

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

    private io.fabric8.kubernetes.client.Config convert(RunContext runContext, Config config) throws IllegalVariableEvaluationException {
        if (config == null) {
            return null;
        }

        var builder =  new io.fabric8.kubernetes.client.ConfigBuilder()
            .withMasterUrl(runContext.render(config.getMasterUrl()))
            .withUsername(runContext.render(config.getUsername()))
            .withPassword(runContext.render(config.getPassword()))
            .withNamespace(runContext.render(config.getNamespace()))
            .withCaCertData(runContext.render(config.getCaCert()))
            .withClientCertData(runContext.render(config.getClientCert()))
            .withClientKeyData(runContext.render(config.getClientKey()))
            .withClientKeyAlgo(runContext.render(config.getClientKeyAlgo()))
            .withOauthToken(runContext.render(config.getOAuthToken()));

        if (Boolean.TRUE.equals(config.getTrustCerts())) {
            builder.withTrustCerts(true);
        }

        return builder.build();
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

    private Pod createPod(RunContext runContext, Container mainContainer) throws IllegalVariableEvaluationException {
        VolumeMount volumeMount = new VolumeMountBuilder()
            .withMountPath("/kestra")
            .withName(FILES_VOLUME_NAME)
            .build();

        var spec = new PodSpecBuilder()
            .withContainers(mainContainer)
            .withRestartPolicy("Never")
            .build();

        spec.getContainers()
            .add(filesContainer(runContext, volumeMount, true));

        spec.getInitContainers()
            .add(filesContainer(runContext, volumeMount, false));

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

    private void uploadInputFiles(RunContext runContext, PodResource podResource, Logger logger, List<String> inputFiles) throws IOException {
        inputFiles.forEach(
            throwConsumer(file -> withRetries(
                logger,
                "uploadInputFiles",
                () -> {
                    Path filePath = runContext.resolve(Path.of(file));
                    ContainerResource containerResource = podResource
                        .inContainer(INIT_FILES_CONTAINER_NAME)
                        .withReadyWaitTimeout(0);

                    String containerFilePath = WORKING_DIR.resolve(file.startsWith("/") ? file.substring(1) : file).toString();
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
        try(Stream<Path> files = Files.list(workingDirectory.resolve(containerWorkingDirAsRelativePath))) {
            files.forEach(throwConsumer(outputFile -> {
                Path relativePathFromContainerWDir = workingDirectory.resolve(containerWorkingDirAsRelativePath).relativize(outputFile);
                // If the file was in the container outputDir, we forward it to the task commands' outputDir
                if (outputFile.equals(workingDirectory.resolve(containerOutputDir.toString().substring(1)))) {
                    Files.move(outputFile, taskCommands.getOutputDirectory(), StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.move(outputFile, workingDirectory.resolve(relativePathFromContainerWDir), StandardCopyOption.REPLACE_EXISTING);
                }
            }));
        }

        PodService.uploadMarker(runContext, podResource, logger, "ended", SIDECAR_FILES_CONTAINER_NAME);
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
        return Map.of(
            ScriptService.VAR_WORKING_DIR, WORKING_DIR,
            ScriptService.VAR_OUTPUT_DIR, WORKING_DIR.resolve(IdUtils.create())
        );
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
