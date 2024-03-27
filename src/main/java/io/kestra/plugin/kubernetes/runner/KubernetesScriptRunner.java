package io.kestra.plugin.kubernetes.runner;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.script.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.ListUtils;
import io.kestra.core.utils.MapUtils;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.plugin.kubernetes.services.PodService.normalizedValue;
import static io.kestra.plugin.kubernetes.services.PodService.withRetries;

@Introspected
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(beta = true, examples = {})
@Schema(
    title = "A script runner that runs a script inside a pod in Kubernetes.",
    description = """
        This script runner is container-based so the `containerImage` property must be set to be able to use it.
        When the Kestra Worker that runs this script is terminated, the pod will still runs until completion, then after restarting, the Worker will resume processing on the existing pod unless `resume` is set to false."""
)
public class KubernetesScriptRunner extends ScriptRunner {
    private static final String INIT_FILES_CONTAINER_NAME = "init-files";
    private static final String FILES_VOLUME_NAME = "kestra-files";
    private static final String SIDECAR_FILES_CONTAINER_NAME = "out-files";
    private static final String MAIN_CONTAINER_NAME = "main";
    private static final String WORKING_DIR = "/kestra/working-dir";

    @Schema(
        title = "The configuration of the target Kubernetes cluster."
    )
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
    private final Duration waitUntilRunning = Duration.ofMinutes(10);

    @Schema(
        title = "The maximum duration to wait for the pod completion."
    )
    @NotNull
    @Builder.Default
    private final Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "Whether the pod should be deleted upon completion."
    )
    @NotNull
    @Builder.Default
    private final Boolean delete = true;

    @Schema(
        title = "Whether to reconnect to the current pod if it already exists."
    )
    @NotNull
    @Builder.Default
    private final Boolean resume = true;

    @Schema(
        title = "The configuration of the file sidecar container that handle download and upload of files."
    )
    @PluginProperty
    @Builder.Default
    protected SideCar fileSidecar = SideCar.builder().build();

    @Override
    public RunnerResult run(RunContext runContext, ScriptCommands commands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        if(!PodService.tempDir(runContext).toFile().mkdir()) {
            throw new IOException("Unable to create the temp directory");
        }
        String namespace = runContext.render(this.namespace);
        Logger logger = runContext.logger();
        Map<String, Object> additionalVars = new HashMap<>(commands.getAdditionalVars());
        additionalVars.put("workingDir", WORKING_DIR);
        if (!ListUtils.isEmpty(filesToDownload)) {
            Map<String, Object> outputFileVariables = new HashMap<>();
            filesToDownload.forEach(file -> outputFileVariables.put(file, "/kestra/working-dir/" + file));
            additionalVars.put("outputFiles", outputFileVariables);
        }

        AbstractLogConsumer defaultLogConsumer = commands.getLogConsumer();
        try (var client = PodService.client(convert(runContext, config));
             var podLogService = new PodLogService(runContext.getApplicationContext().getBean(ThreadMainFactoryBuilder.class))) {
            Pod pod = null;
            PodResource resource = null;

            if (this.resume) {
                // try to locate an existing pod for this taskrun and attempt
                Map<String, String> taskrun = (Map<String, String>) runContext.getVariables().get("taskrun");
                String taskrunId = normalizedValue(taskrun.get("id"));
                String attempt =  normalizedValue(String.valueOf(taskrun.get("attemptsCount")));
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

            if (pod == null) {
                Container container = createContainer(runContext, commands, additionalVars);
                pod = createPod(runContext, container, filesToUpload, filesToDownload);
                resource = client.pods().inNamespace(namespace).resource(pod);
                pod = resource.create();
                logger.info("Pod '{}' is created ", pod.getMetadata().getName());
            }

            try (var podWatch = resource.watch(new PodWatcher(logger))) {
                // in case of resuming an already running pod, the status will be running
                if (!"Running".equals(pod.getStatus().getPhase())) {
                    // wait for init container
                    if (!ListUtils.isEmpty(filesToUpload)) {
                        pod = PodService.waitForInitContainerRunning(client, pod, INIT_FILES_CONTAINER_NAME, this.waitUntilRunning);
                        this.uploadInputFiles(runContext, PodService.podRef(client, pod), logger, filesToUpload);
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
                if (!ListUtils.isEmpty(filesToDownload)) {
                    pod = PodService.waitForCompletionExcept(client, logger, pod, this.waitUntilCompletion, SIDECAR_FILES_CONTAINER_NAME);
                    this.downloadOutputFiles(runContext, PodService.podRef(client, pod), logger, commands.getOutputDirectory());
                } else {
                    pod = PodService.waitForCompletion(client, logger, pod, this.waitUntilCompletion);
                }

                // wait for logs to arrives
                // TODO make it configurable
                Thread.sleep(1000);

                // handle exception
                if (pod.getStatus() == null) {
                    runContext.logger().error("Pod terminated without any status, failing the task.");
                    throw new ScriptException(-1, defaultLogConsumer.getStdOutCount(), defaultLogConsumer.getStdErrCount());
                }

                if (pod.getStatus().getPhase().equals("Failed")) {
                    if (pod.getStatus().getContainerStatuses() == null) {
                        runContext.logger().error("Pod terminated without any container statuses, failing the task.");
                        throw new ScriptException(-1, defaultLogConsumer.getStdOutCount(), defaultLogConsumer.getStdErrCount());
                    }

                    // TODO we should be able to send a proper message thanks to the ScriptException
                    throw pod.getStatus().getContainerStatuses().stream()
                        .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null)
                        .map(containerStatus -> containerStatus.getState().getTerminated())
                        .findFirst()
                        .map(containerStateTerminated -> new ScriptException(containerStateTerminated.getExitCode(), defaultLogConsumer.getStdOutCount(), defaultLogConsumer.getStdErrCount()))
                        .orElse(new ScriptException(-1, defaultLogConsumer.getStdOutCount(), defaultLogConsumer.getStdErrCount()));
                }

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

    private Container createContainer(RunContext runContext, ScriptCommands commands, Map<String, Object> additionalVars) throws IllegalVariableEvaluationException, IOException {
        List<String> command = ScriptService.uploadInputFiles(runContext, runContext.render(commands.getCommands(), additionalVars));
        List<EnvVar> env = MapUtils.emptyOnNull(commands.getEnv()).entrySet().stream()
            .map(entry -> new EnvVarBuilder().withName(entry.getKey()).withValue(entry.getValue()).build())
            .toList();

        var builder = new ContainerBuilder()
            .withName(MAIN_CONTAINER_NAME)
            .withImage(runContext.render(commands.getContainerImage(), commands.getAdditionalVars()))
            .withImagePullPolicy(this.pullPolicy)
            .withEnv(env)
            .withCommand(command);

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

    private Pod createPod(RunContext runContext, Container mainContainer, List<String> filesToUpload, List<String> filesToDownload) throws IllegalVariableEvaluationException {
        VolumeMount volumeMount = new VolumeMountBuilder()
            .withMountPath("/kestra")
            .withName(FILES_VOLUME_NAME)
            .build();

        var spec = new PodSpecBuilder()
            .withContainers(mainContainer)
            .withRestartPolicy("Never")
            .build();

        if (!ListUtils.isEmpty(filesToDownload)) {
            spec
                .getContainers()
                .add(filesContainer(runContext, volumeMount, true));
        }

        if (!ListUtils.isEmpty(filesToUpload)) {
            spec
                .getInitContainers()
                .add(filesContainer(runContext, volumeMount, false));
        }

        if (!ListUtils.isEmpty(filesToDownload) || !ListUtils.isEmpty(filesToUpload)) {
            spec.getContainers()
                .forEach(container -> {
                    List<VolumeMount> volumeMounts = container.getVolumeMounts();
                    volumeMounts.add(volumeMount);
                    container.setVolumeMounts(volumeMounts);
                    container.setWorkingDir(WORKING_DIR);
                });

            spec.getVolumes()
                .add(new VolumeBuilder()
                    .withName(FILES_VOLUME_NAME)
                    .withNewEmptyDir()
                    .endEmptyDir()
                    .build()
                );
        }

        Map<String, String> allLabels = this.labels == null ? new HashMap<>() : runContext.renderMap(this.labels);
        allLabels.putAll(PodService.labels(runContext));
        var metadata = new ObjectMetaBuilder()
            .withName(PodService.podName(runContext))
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
                    try (var fileInputStream = new FileInputStream(runContext.resolve(Path.of(file)).toFile())) {
                        return podResource
                            .inContainer(INIT_FILES_CONTAINER_NAME)
                            .withReadyWaitTimeout(0)
                            .file("/kestra/working-dir/" + file)
                            .upload(fileInputStream);
                    }
                }
            ))
        );

        PodService.uploadMarker(runContext, podResource, logger, "ready", INIT_FILES_CONTAINER_NAME);
    }

    protected void downloadOutputFiles(RunContext runContext, PodResource podResource, Logger logger, Path outputDirectory) throws Exception {
        withRetries(
            logger,
            "downloadOutputFiles",
            () -> podResource
                .inContainer(SIDECAR_FILES_CONTAINER_NAME)
                .dir("/kestra/working-dir/")
                .copy(outputDirectory)
        );

        // kubernetes copy by keeping the target repository which we don't want, so we move the files
        try(Stream<Path> files = Files.list(outputDirectory.resolve("kestra/working-dir/"))) {
            files.forEach(throwConsumer(outputFile -> {
                    Files.move(outputFile, outputDirectory.resolve(outputDirectory.resolve("kestra/working-dir/").relativize(outputFile)), StandardCopyOption.REPLACE_EXISTING);
                }
            ));
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
