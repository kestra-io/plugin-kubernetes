package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.runner.SideCar;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;

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
    protected static final String OUTPUTFILES_FINALIZERS = "kestra.io/output-files";

    @Schema(
        title = "The files from the container filesystem to send to Kestra's internal storage.",
        description = "Only files created inside the `kestra/working-dir` directory of the container can be retrieved. You can use the special variable `{{workingDir}}` in this property to refer to it"
    )
    @PluginProperty
    protected List<String> outputFiles;

    @Schema(
        title = "The files to create on the local filesystem. It can be a map or a JSON object.",
        description = "The files will be available inside the `kestra/working-dir` directory of the container. You can use the special variable `{{workingDir}}` in your command to refer to it."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Object inputFiles;


    @Schema(
        title = "The configuration of the file sidecar container that handle download and upload of files."
    )
    @PluginProperty
    @Builder.Default
    protected SideCar fileSidecar = SideCar.builder().build();

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

    protected Map<String, URI> downloadOutputFiles(RunContext runContext, PodResource podResource, Logger logger, Map<String, Object> additionalVars) throws Exception {
        withRetries(
            logger,
            "downloadOutputFiles",
            () -> podResource
                .inContainer(SIDECAR_FILES_CONTAINER_NAME)
                .dir("/kestra/working-dir/")
                .copy(PodService.tempDir(runContext))
        );

        PodService.uploadMarker(runContext, podResource, logger, "ended", SIDECAR_FILES_CONTAINER_NAME);

        // upload output files
        Map<String, URI> uploaded = new HashMap<>();

        outputFiles.
            forEach(throwConsumer(f -> {
                File file = runContext.workingDir().resolve(Path.of("working-dir/kestra/working-dir/" + runContext.render(f, additionalVars))).toFile();
                uploaded.put(f, runContext.storage().putFile(file));
            }));

        return uploaded;
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
}
