package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.models.SideCar;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.plugin.kubernetes.services.PodService.withRetries;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractPod extends AbstractConnection {
    protected static final String INIT_FILES_CONTAINER_NAME = "init-files";
    protected static final String SIDECAR_FILES_CONTAINER_NAME = "out-files";
    protected static final String FILES_VOLUME_NAME = "kestra-files";

    @Schema(
        title = "The files from the container filesystem to send to Kestra's internal storage",
        description = "Only files created inside the `kestra/working-dir` directory of the container can be retrieved.\n" +
            "Must be a list of [glob](https://en.wikipedia.org/wiki/Glob_(programming)) expressions relative to the current working directory, some examples: `my-dir/**`, `my-dir/*/**` or `my-dir/my-file.txt`.."
    )
    protected Property<List<String>> outputFiles;

    @Schema(
        title = "The files to create on the local filesystem – it can be a map or a JSON object.",
        description = "The files will be available inside the `kestra/working-dir` directory of the container. You can use the special variable `{{workingDir}}` in your command to refer to it."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Object inputFiles;


    @Schema(
        title = "The configuration of the file sidecar container that handles the download and upload of files"
    )
    @PluginProperty
    @Builder.Default
    protected SideCar fileSidecar = SideCar.builder().build();

    @Builder.Default
    @Schema(
        title = "The maximum duration to wait until the resource becomes ready",
        description = "When set to a positive duration, waits for the resource to report Ready=True in its status conditions. " +
            "Set to PT0S (zero, default) to skip waiting. " +
            "Supports Pods, StatefulSets, and custom resources that use the Ready condition. " +
            "Note: Deployments are not supported as they use the Available condition instead of Ready."
    )
    protected Property<Duration> waitUntilReady = Property.ofValue(Duration.ZERO);

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void init(RunContext runContext) {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        PodService.tempDir(runContext).toFile().mkdir();
    }

    protected void uploadInputFiles(RunContext runContext, PodResource podResource, Logger logger, Set<String> inputFiles) throws IOException {
        inputFiles.forEach(
            throwConsumer(file -> withRetries(
                logger,
                "uploadInputFiles",
                () -> {
                    try (var fileInputStream = new FileInputStream(PodService.tempDir(runContext).resolve(file).toFile())) {
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

    protected Map<Path, Path> downloadOutputFiles(RunContext runContext, PodResource podResource, Logger logger, Map<String, Object> additionalVars) throws Exception {
        withRetries(
            logger,
            "downloadOutputFiles",
            () -> podResource
                .inContainer(SIDECAR_FILES_CONTAINER_NAME)
                .dir("/kestra/working-dir/")
                .copy(PodService.tempDir(runContext))
        );

        // Download output files
        // path map from copied file path with encoded parts to the actually produced relative file path
        Map<Path, Path> pathMap = new HashMap<>();
        // kubernetes copy by keeping the target repository which we don't want, so we move the files
        try (Stream<Path> files = Files.walk(runContext.workingDir().resolve(Path.of("working-dir/kestra/working-dir/")))) {
            files
                .filter(path -> !Files.isDirectory(path) && Files.isReadable(path))
                .forEach(throwConsumer(outputFile -> {
                    Path relativePathFromContainerWDir = runContext.workingDir().resolve(Path.of("working-dir/kestra/working-dir/")).relativize(outputFile);
                    // Split path into components and sanitize by encoding special characters
                    Path resolvedOutputFile = runContext.workingDir().path();
                    for (int i = 0; i < relativePathFromContainerWDir.getNameCount(); i++) {
                        resolvedOutputFile = resolvedOutputFile.resolve(Path.of(java.net.URLEncoder.encode(
                            relativePathFromContainerWDir.getName(i).toString(),
                            StandardCharsets.UTF_8)));
                    }
                    pathMap.put(resolvedOutputFile, relativePathFromContainerWDir);
                    moveFile(outputFile, resolvedOutputFile);
                }));
        }
        return pathMap;
    }

    private void moveFile(Path from, Path to) throws IOException {
        if (Files.notExists(to.getParent())) {
            Files.createDirectories(to.getParent());
        }
        Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
    }

    protected void handleFiles(RunContext runContext, PodSpec spec) throws IllegalVariableEvaluationException {
        VolumeMount volumeMount = new VolumeMountBuilder()
            .withMountPath("/kestra")
            .withName(FILES_VOLUME_NAME)
            .build();

        if (this.outputFiles != null) {
            spec
                .getContainers()
                .add(filesContainer(runContext, volumeMount, true));
        }

        if (this.inputFiles != null) {
            spec
                .getInitContainers()
                .add(filesContainer(runContext, volumeMount, false));
        } else if (this.outputFiles != null) {
            spec
                .getInitContainers()
                .add(workingDirectoryInitContainer(runContext, volumeMount));
        }

        if (this.inputFiles != null || this.outputFiles != null) {
            spec.getContainers()
                .forEach(container -> {
                    List<VolumeMount> volumeMounts = container.getVolumeMounts();
                    volumeMounts.add(volumeMount);
                    container.setVolumeMounts(volumeMounts);
                });

            spec.getVolumes()
                .add(new VolumeBuilder()
                    .withName(FILES_VOLUME_NAME)
                    .withNewEmptyDir()
                    .endEmptyDir()
                    .build()
                );
        }
    }

    protected List<HasMetadata> parseSpec(String spec) {
        var serialization = new KubernetesSerialization();
        var resource = serialization.unmarshal(spec);

        List<HasMetadata> resources = new ArrayList<>();
        switch (resource) {
            case List<?> parsed -> resources.addAll((List<? extends HasMetadata>) parsed);
            case HasMetadata parsed -> resources.add(parsed);
            case KubernetesResourceList<?> parsed -> resources.addAll(parsed.getItems());
            case null, default -> throw new IllegalArgumentException("Unknown resource");
        }

        return resources;
    }

    private static ResourceRequirements mapSidecarResources(RunContext runContext, SideCar sideCar) throws IllegalVariableEvaluationException {
        if (sideCar == null) {
            return null;
        }

        Map<String, Object> sidecarResources = runContext.render(sideCar.getResources()).asMap(String.class, Object.class);
        if (sidecarResources == null) {
            return null;
        }

        ResourceRequirements resourceRequirements = new ResourceRequirements();
        ResourceRequirementsBuilder resourceRequirementsBuilder = new ResourceRequirementsBuilder();
        if (sidecarResources.containsKey("claims")) {
            try {
                resourceRequirementsBuilder.withClaims((List<ResourceClaim>) sidecarResources.get("claims"));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Sidecar resources claims must be a list of resource claims");
            }
        }
        if (sidecarResources.containsKey("limits")) {
            try {
                resourceRequirementsBuilder.withLimits((Map<String, Quantity>) sidecarResources.get("limits"));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Sidecar resources limits must be a map of string to quantity");
            }
        }
        if (sidecarResources.containsKey("requests")) {
            try {
                resourceRequirementsBuilder.withRequests((Map<String, Quantity>) sidecarResources.get("requests"));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Sidecar resources requests must be a map of string to quantity");
            }
        }
        resourceRequirements = resourceRequirementsBuilder.build();
        return resourceRequirements;
    }

    private Container filesContainer(RunContext runContext, VolumeMount volumeMount, boolean finished) throws IllegalVariableEvaluationException {
        String s = finished ? "ended" : "ready";

        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName(finished ? SIDECAR_FILES_CONTAINER_NAME : INIT_FILES_CONTAINER_NAME)
            .withImage(fileSidecar != null ? runContext.render(fileSidecar.getImage()).as(String.class).orElse("busybox") : "busybox")
            .withResources(fileSidecar != null ? mapSidecarResources(runContext, fileSidecar) : null)
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

    private Container workingDirectoryInitContainer(RunContext runContext, VolumeMount volumeMount) throws IllegalVariableEvaluationException {
        return new ContainerBuilder()
            .withName(INIT_FILES_CONTAINER_NAME)
            .withImage(fileSidecar != null ? runContext.render(fileSidecar.getImage()).as(String.class).orElse("busybox") : "busybox")
            .withResources(fileSidecar != null ? mapSidecarResources(runContext, fileSidecar) : null)
            .withCommand(Arrays.asList(
                "sh",
                "-c",
                "echo 'Creating working directory'\n" +
                    "mkdir -p /kestra/working-dir\n"
            ))
            .withVolumeMounts(Collections.singletonList(volumeMount))
            .build();
    }
}
