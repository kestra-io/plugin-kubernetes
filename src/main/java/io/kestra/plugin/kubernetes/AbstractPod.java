package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.kubernetes.models.SideCar;
import io.kestra.plugin.kubernetes.services.InstanceService;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.plugin.kubernetes.services.PodService.withRetries;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPod extends AbstractConnection {
    protected static final String INIT_FILES_CONTAINER_NAME = "init-files";
    protected static final String SIDECAR_FILES_CONTAINER_NAME = "out-files";
    protected static final String FILES_VOLUME_NAME = "kestra-files";

    // Constants for marker files used in file transfer coordination
    protected static final String READY_MARKER = "ready";
    protected static final String ENDED_MARKER = "ended";

    @Schema(
        title = "The namespace where the operation will be done",
        description = "The Kubernetes namespace in which to execute the operation. Defaults to 'default' if not specified."
    )
    @NotNull
    @Builder.Default
    protected Property<String> namespace = Property.ofValue("default");

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

    @Schema(
        title = "Default container spec applied to all containers in the pod",
        description = """
            When set, these container spec fields are merged into all containers including:
            - User-defined containers in the spec
            - Init and sidecar containers for file transfer (unless fileSidecar.defaultSpec is set)

            This provides a convenient way to apply uniform container settings across all containers,
            which is especially useful in restrictive environments like GovCloud.

            Supports any valid Kubernetes container spec fields such as:
            - securityContext: Security settings for all containers
            - volumeMounts: Volume mounts to add to all containers
            - resources: Resource limits/requests for all containers
            - env: Environment variables for all containers

            Merge behavior:
            - For nested objects (like securityContext): deep merge, container-specific values take precedence
            - For lists (like volumeMounts, env): concatenated, with defaults added first
            - Container-specific values always override defaults

            Example configuration:
            ```yaml
            containerDefaultSpec:
              securityContext:
                allowPrivilegeEscalation: false
                capabilities:
                  drop:
                  - ALL
                readOnlyRootFilesystem: true
                seccompProfile:
                  type: RuntimeDefault
              volumeMounts:
                - name: tmp
                  mountPath: /tmp
              resources:
                limits:
                  memory: "256Mi"
            ```
            """
    )
    protected Property<Map<String, Object>> containerDefaultSpec;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void init(RunContext runContext) {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        PodService.tempDir(runContext).toFile().mkdir();
    }

    protected void uploadInputFiles(RunContext runContext, PodResource podResource, Logger logger, Set<String> inputFiles) throws IOException {
        Path tempDir = PodService.tempDir(runContext);

        Map<String, List<String>> grouped = inputFiles.stream()
            .collect(Collectors.groupingBy(file -> {
                Path p = Path.of(file);
                return p.getNameCount() > 0 ? p.getName(0).toString() : file;
            }));

        ContainerResource container = podResource
            .inContainer(INIT_FILES_CONTAINER_NAME)
            .withReadyWaitTimeout(0);

        for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {

            String top = entry.getKey();
            Path topAbsolute = tempDir.resolve(top);
            String containerTop = "/kestra/working-dir/" + top;

            if (Files.isDirectory(topAbsolute)) {
                try {
                    withRetries(logger, "uploadInputFilesBulk",
                        () -> container
                            .dir(containerTop)
                            .upload(topAbsolute)
                    );
                    continue;
                } catch (Exception e) {
                    logger.info("Bulk upload failed for '{}', falling back to per-file upload. Reason: {}", top, e.getMessage());
                }
            }

            for (String file : entry.getValue()) {
                withRetries(logger, "uploadInputFiles",
                    () -> {
                        try (var fileInputStream = new FileInputStream(tempDir.resolve(file).toFile())) {
                            return container
                                .file("/kestra/working-dir/" + file)
                                .upload(fileInputStream);
                        }
                    }
                );
            }
        }

        PodService.uploadMarker(runContext, podResource, logger, READY_MARKER, INIT_FILES_CONTAINER_NAME);
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

    /**
     * Applies the containerDefaultSpec to all user-defined containers.
     * The default spec is deep-merged with each container's existing spec, with container values taking precedence.
     * This should be called after the spec is created but before file handling containers are added.
     */
    protected void applyContainerDefaultSpec(RunContext runContext, PodSpec spec) throws IllegalVariableEvaluationException {
        if (this.containerDefaultSpec == null) {
            return;
        }

        Map<String, Object> defaultSpecMap = runContext.render(this.containerDefaultSpec).asMap(String.class, Object.class);
        if (defaultSpecMap == null || defaultSpecMap.isEmpty()) {
            return;
        }

        // Apply to all containers
        spec.getContainers().forEach(container -> {
            try {
                mergeContainerDefaults(runContext, container, defaultSpecMap);
            } catch (Exception e) {
                throw new RuntimeException("Failed to apply container default spec: " + e.getMessage(), e);
            }
        });

        // Apply to all init containers
        if (spec.getInitContainers() != null) {
            spec.getInitContainers().forEach(container -> {
                try {
                    mergeContainerDefaults(runContext, container, defaultSpecMap);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to apply container default spec to init container: " + e.getMessage(), e);
                }
            });
        }
    }

    /**
     * Merges default container spec fields into a container.
     * Container-specific values take precedence over defaults.
     * For lists (volumeMounts, env, etc.), defaults are prepended to existing values.
     * For objects (securityContext, resources), deep merge is performed.
     */
    @SuppressWarnings("unchecked")
    private void mergeContainerDefaults(RunContext runContext, Container container, Map<String, Object> defaultSpecMap) throws Exception {
        // Handle securityContext - deep merge
        if (defaultSpecMap.containsKey("securityContext")) {
            Map<String, Object> defaultSecurityContext = (Map<String, Object>) defaultSpecMap.get("securityContext");
            if (container.getSecurityContext() == null) {
                container.setSecurityContext(InstanceService.fromMap(SecurityContext.class, runContext, Map.of(), defaultSecurityContext));
            } else {
                // Deep merge: container values take precedence
                Map<String, Object> mergedSecurityContext = deepMerge(defaultSecurityContext, containerToMap(container.getSecurityContext()));
                container.setSecurityContext(InstanceService.fromMap(SecurityContext.class, runContext, Map.of(), mergedSecurityContext));
            }
        }

        // Handle volumeMounts - concatenate lists
        if (defaultSpecMap.containsKey("volumeMounts")) {
            List<Map<String, Object>> defaultVolumeMounts = (List<Map<String, Object>>) defaultSpecMap.get("volumeMounts");
            List<VolumeMount> defaultMounts = defaultVolumeMounts.stream()
                .map(vm -> {
                    try {
                        return InstanceService.fromMap(VolumeMount.class, runContext, Map.of(), vm);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse default volumeMount: " + e.getMessage(), e);
                    }
                })
                .toList();

            List<VolumeMount> existingMounts = container.getVolumeMounts();
            if (existingMounts == null) {
                existingMounts = new ArrayList<>();
            }
            // Prepend defaults, then add existing (allows container to override by having same mountPath)
            List<VolumeMount> mergedMounts = new ArrayList<>(defaultMounts);
            mergedMounts.addAll(existingMounts);
            container.setVolumeMounts(mergedMounts);
        }

        // Handle resources - deep merge
        if (defaultSpecMap.containsKey("resources")) {
            Map<String, Object> defaultResources = (Map<String, Object>) defaultSpecMap.get("resources");
            if (container.getResources() == null) {
                container.setResources(InstanceService.fromMap(ResourceRequirements.class, runContext, Map.of(), defaultResources));
            } else {
                Map<String, Object> mergedResources = deepMerge(defaultResources, containerToMap(container.getResources()));
                container.setResources(InstanceService.fromMap(ResourceRequirements.class, runContext, Map.of(), mergedResources));
            }
        }

        // Handle env - concatenate lists
        if (defaultSpecMap.containsKey("env")) {
            List<Map<String, Object>> defaultEnv = (List<Map<String, Object>>) defaultSpecMap.get("env");
            List<EnvVar> defaultEnvVars = defaultEnv.stream()
                .map(ev -> {
                    try {
                        return InstanceService.fromMap(EnvVar.class, runContext, Map.of(), ev);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse default env var: " + e.getMessage(), e);
                    }
                })
                .toList();

            List<EnvVar> existingEnv = container.getEnv();
            if (existingEnv == null) {
                existingEnv = new ArrayList<>();
            }
            // Prepend defaults, then add existing (allows container to override by having same name)
            List<EnvVar> mergedEnv = new ArrayList<>(defaultEnvVars);
            mergedEnv.addAll(existingEnv);
            container.setEnv(mergedEnv);
        }
    }

    /**
     * Converts a Kubernetes object to a Map for merging purposes.
     */
    private Map<String, Object> containerToMap(Object obj) {
        if (obj == null) {
            return new HashMap<>();
        }
        try {
            String yaml = JacksonMapper.ofYaml().writeValueAsString(obj);
            return JacksonMapper.ofYaml().readValue(yaml, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    /**
     * Deep merges two maps. Values from the override map take precedence.
     * For nested maps, recursively merges. For other values, override wins.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> deepMerge(Map<String, Object> base, Map<String, Object> override) {
        Map<String, Object> result = new HashMap<>(base);
        for (Map.Entry<String, Object> entry : override.entrySet()) {
            String key = entry.getKey();
            Object overrideValue = entry.getValue();
            Object baseValue = result.get(key);

            if (overrideValue instanceof Map && baseValue instanceof Map) {
                result.put(key, deepMerge((Map<String, Object>) baseValue, (Map<String, Object>) overrideValue));
            } else if (overrideValue != null) {
                result.put(key, overrideValue);
            }
        }
        return result;
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

    /**
     * Gets the default spec to apply to sidecar/init containers.
     * Priority: fileSidecar.defaultSpec > containerDefaultSpec
     * @return the default spec map or null if none configured
     */
    private Map<String, Object> getSidecarDefaultSpec(RunContext runContext, SideCar sideCar) throws IllegalVariableEvaluationException {
        // First try fileSidecar.defaultSpec
        if (sideCar != null && sideCar.getDefaultSpec() != null) {
            Map<String, Object> defaultSpecMap = runContext.render(sideCar.getDefaultSpec()).asMap(String.class, Object.class);
            if (defaultSpecMap != null && !defaultSpecMap.isEmpty()) {
                return defaultSpecMap;
            }
        }

        // Fall back to containerDefaultSpec if set
        if (this.containerDefaultSpec != null) {
            Map<String, Object> defaultSpecMap = runContext.render(this.containerDefaultSpec).asMap(String.class, Object.class);
            if (defaultSpecMap != null && !defaultSpecMap.isEmpty()) {
                return defaultSpecMap;
            }
        }

        return null;
    }

    /**
     * Applies default spec to a sidecar/init container.
     * This applies containerDefaultSpec (or fileSidecar.defaultSpec if set) to file transfer containers.
     */
    @SuppressWarnings("unchecked")
    private void applySidecarDefaultSpec(RunContext runContext, Container container, SideCar sideCar) throws Exception {
        Map<String, Object> defaultSpecMap = getSidecarDefaultSpec(runContext, sideCar);
        if (defaultSpecMap == null || defaultSpecMap.isEmpty()) {
            return;
        }

        // Apply security context
        if (defaultSpecMap.containsKey("securityContext")) {
            Map<String, Object> securityContextMap = (Map<String, Object>) defaultSpecMap.get("securityContext");
            container.setSecurityContext(InstanceService.fromMap(SecurityContext.class, runContext, Map.of(), securityContextMap));
        }

        // Apply volume mounts - prepend to existing
        if (defaultSpecMap.containsKey("volumeMounts")) {
            List<Map<String, Object>> defaultVolumeMounts = (List<Map<String, Object>>) defaultSpecMap.get("volumeMounts");
            List<VolumeMount> defaultMounts = defaultVolumeMounts.stream()
                .map(vm -> {
                    try {
                        return InstanceService.fromMap(VolumeMount.class, runContext, Map.of(), vm);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse default volumeMount for sidecar: " + e.getMessage(), e);
                    }
                })
                .toList();

            List<VolumeMount> existingMounts = container.getVolumeMounts();
            if (existingMounts == null) {
                existingMounts = new ArrayList<>();
            }
            List<VolumeMount> mergedMounts = new ArrayList<>(defaultMounts);
            mergedMounts.addAll(existingMounts);
            container.setVolumeMounts(mergedMounts);
        }

        // Apply resources
        if (defaultSpecMap.containsKey("resources")) {
            Map<String, Object> resourcesMap = (Map<String, Object>) defaultSpecMap.get("resources");
            container.setResources(InstanceService.fromMap(ResourceRequirements.class, runContext, Map.of(), resourcesMap));
        }

        // Apply env vars - prepend to existing
        if (defaultSpecMap.containsKey("env")) {
            List<Map<String, Object>> defaultEnv = (List<Map<String, Object>>) defaultSpecMap.get("env");
            List<EnvVar> defaultEnvVars = defaultEnv.stream()
                .map(ev -> {
                    try {
                        return InstanceService.fromMap(EnvVar.class, runContext, Map.of(), ev);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse default env var for sidecar: " + e.getMessage(), e);
                    }
                })
                .toList();

            List<EnvVar> existingEnv = container.getEnv();
            if (existingEnv == null) {
                existingEnv = new ArrayList<>();
            }
            List<EnvVar> mergedEnv = new ArrayList<>(defaultEnvVars);
            mergedEnv.addAll(existingEnv);
            container.setEnv(mergedEnv);
        }
    }

    private Container filesContainer(RunContext runContext, VolumeMount volumeMount, boolean finished) throws IllegalVariableEvaluationException {
        String status = finished ? ENDED_MARKER : READY_MARKER;

        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName(finished ? SIDECAR_FILES_CONTAINER_NAME : INIT_FILES_CONTAINER_NAME)
            .withImage(fileSidecar != null ? runContext.render(fileSidecar.getImage()).as(String.class).orElse("busybox") : "busybox")
            .withResources(fileSidecar != null ? mapSidecarResources(runContext, fileSidecar) : null)
            .withCommand(Arrays.asList(
                "sh",
                "-c",
                "echo 'waiting to be " + status + "!'\n" +
                    "while [ ! -f /kestra/" + status + " ]\n" +
                    "do\n" +
                    "  sleep 0.5\n" +
                    (finished ? "" : "echo '* still waiting!'\n") +
                    "done\n" +
                    "echo '" + status + " successfully'\n"
            ));

        if (!finished) {
            containerBuilder.withVolumeMounts(Collections.singletonList(volumeMount));
        }

        Container container = containerBuilder.build();

        // Apply containerDefaultSpec or fileSidecar.defaultSpec to the container
        try {
            applySidecarDefaultSpec(runContext, container, fileSidecar);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to apply default spec to sidecar container: " + e.getMessage(), e);
        }

        return container;
    }

    private Container workingDirectoryInitContainer(RunContext runContext, VolumeMount volumeMount) throws IllegalVariableEvaluationException {
        Container container = new ContainerBuilder()
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

        // Apply containerDefaultSpec or fileSidecar.defaultSpec to the container
        try {
            applySidecarDefaultSpec(runContext, container, fileSidecar);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to apply default spec to init container: " + e.getMessage(), e);
        }

        return container;
    }
}
