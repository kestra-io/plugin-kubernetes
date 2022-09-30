package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.BashService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

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
        title = "Output file list that will be uploaded to internal storage",
        description = "List of key that will generate temporary files.\n" +
            "On the command, just can use with special variable named `outputFiles.key`.\n" +
            "If you add a files with `[\"first\"]`, you can use the special vars `echo 1 >> {[ outputFiles.first }}`" +
            " and you used on others tasks using `{{ outputs['task-id'].files.first }}`"
    )
    @PluginProperty(dynamic = false)
    protected List<String> outputFiles;

    @Schema(
        title = "Input files are extra files supplied by user that make it simpler organize code.",
        description = "Describe a files map that will be written and usable in execution context. In python execution " +
            "context is in a temp folder, for bash scripts, you can reach files using a workingDir variable " +
            "like 'source {{workingDir}}/myfile.sh' "
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Object inputFiles;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    @Getter(AccessLevel.NONE)
    protected transient Map<String, String> generatedOutputFiles;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void init(RunContext runContext) throws IOException {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        additionalVars.put("workingDir", "/kestra/working-dir");
        tempDir(runContext).toFile().mkdir();
    }

    private Path tempDir(RunContext runContext) throws IOException {
        return runContext.tempDir().resolve("working-dir");
    }

    protected void uploadInputFiles(RunContext runContext, PodResource podResource, Logger logger) throws IOException {
        podResource
            .inContainer(INIT_FILES_CONTAINER_NAME)
            .dir("/kestra/working-dir")
            .upload(tempDir(runContext));

        this.uploadMarker(runContext, podResource, logger, false);
    }

    protected Map<String, URI> downloadOutputFiles(RunContext runContext, PodResource podResource, Logger logger) throws Exception {
        podResource
            .inContainer(SIDECAR_FILES_CONTAINER_NAME)
            .dir("/kestra/working-dir/")
            .copy(tempDir(runContext));

        this.uploadMarker(runContext, podResource, logger, true);

        // upload output files
        Map<String, URI> uploaded = new HashMap<>();

        generatedOutputFiles.
            forEach(throwBiConsumer((k, v) -> {
                File file = Paths
                    .get(
                        tempDir(runContext).toAbsolutePath().toString(),
                        runContext.render(v, additionalVars)
                    )
                    .toFile();

                uploaded.put(k, runContext.putTempFile(file));
            }));

        return uploaded;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void uploadMarker(RunContext runContext, PodResource podResource, Logger logger, boolean finished) throws IOException {
        String s = finished ? "ended" : "ready";

        File marker = tempDir(runContext).resolve(s).toFile();
        marker.createNewFile();

        podResource
            .inContainer(finished ? SIDECAR_FILES_CONTAINER_NAME : INIT_FILES_CONTAINER_NAME)
            .file("/kestra/" + s)
            .upload(marker.toPath());

        logger.debug(s + " marker uploaded");
    }

    protected void handleFiles(RunContext runContext, ObjectMeta metadata, PodSpec spec) throws IOException, IllegalVariableEvaluationException, URISyntaxException {
        VolumeMount volumeMount = new VolumeMountBuilder()
            .withMountPath("/kestra")
            .withName(FILES_VOLUME_NAME)
            .build();

        if (this.outputFiles != null) {
            generatedOutputFiles = BashService.createOutputFiles(
                tempDir(runContext),
                this.outputFiles,
                additionalVars
            );

            spec
                .getContainers()
                .add(filesContainer(volumeMount, true));
        }

        if (this.inputFiles != null) {
            Map<String, String> finalInputFiles = BashService.transformInputFiles(runContext, this.inputFiles);

            BashService.createInputFiles(
                runContext,
                tempDir(runContext),
                finalInputFiles,
                additionalVars
            );

            spec
                .getInitContainers()
                .add(filesContainer(volumeMount, false));
        }

        if (this.inputFiles != null || this.outputFiles != null) {
            spec.getContainers()
                .forEach(container -> {
                    List<VolumeMount> volumeMounts = container.getVolumeMounts();
                    volumeMounts.add(volumeMount);
                    container.setVolumeMounts(volumeMounts);
                });
        }

        spec.getVolumes()
            .add(new VolumeBuilder()
                .withName(FILES_VOLUME_NAME)
                .withNewEmptyDir()
                .endEmptyDir()
                .build()
            );
    }

    private Container filesContainer(VolumeMount volumeMount, boolean finished) {
        String s = finished ? "ended" : "ready";

        ContainerBuilder containerBuilder = new ContainerBuilder()
            .withName(finished ? SIDECAR_FILES_CONTAINER_NAME : INIT_FILES_CONTAINER_NAME)
            .withImage("busybox")
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
