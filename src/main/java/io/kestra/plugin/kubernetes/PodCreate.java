package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import jakarta.validation.constraints.NotNull;

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
            code = {
                "namespace: default",
                "metadata:",
                "  labels:",
                "    my-label: my-value",
                "spec:",
                "  containers:",
                "  - name: unittest",
                "    image: debian:stable-slim",
                "    command: ",
                "      - 'bash' ",
                "      - '-c'",
                "      - 'for i in {1..10}; do echo $i; sleep 0.1; done'",
                "  restartPolicy: Never"
            }
        ),
        @Example(
            title = "Launch a Pod with input files and gather its output files.",
            full = true,
            code = {
                """
                    id: kubernetes
                    namespace: myteam
                                        
                    inputs:
                      - id: file
                        type: FILE
                                        
                    tasks:
                      - id: kubernetes
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
                          - out.txt"""
            }
        )
    }
)
@Slf4j
public class PodCreate extends AbstractPod implements RunnableTask<PodCreate.Output> {
    @Schema(
        title = "The namespace where the pod will be created"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    private String namespace = "default";

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
    private final Duration waitRunning = Duration.ofHours(1);

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

    @Override
    public PodCreate.Output run(RunContext runContext) throws Exception {
        super.init(runContext);
        Map<String, Object> additionalVars = new HashMap<>();
        additionalVars.put("workingDir", "/kestra/working-dir");
        if (this.outputFiles != null) {
            Map<String, Object> outputFileVariables = new HashMap<>();
            this.outputFiles.forEach(file -> outputFileVariables.put(file, "/kestra/working-dir/" + file));
            additionalVars.put("outputFiles", outputFileVariables);
        }

        try (KubernetesClient client = this.client(runContext)) {
            String namespace = runContext.render(this.namespace);
            Logger logger = runContext.logger();

            // create the job
            Pod pod = createPod(runContext, client, namespace, additionalVars);
            PodLogService podLogService = new PodLogService(runContext.getApplicationContext().getBean(ThreadMainFactoryBuilder.class));

            try {
                try (Watch podWatch = PodService.podRef(client, pod).watch(listOptions(), new PodWatcher(logger))) {
                    // wait for init container
                    if (this.inputFiles != null || this.outputFiles != null) {
                        pod = PodService.waitForInitContainerRunning(client, pod, INIT_FILES_CONTAINER_NAME, this.waitUntilRunning);
                        this.uploadInputFiles(runContext, PodService.podRef(client, pod), logger);
                    }

                    // wait for pods ready
                    pod = PodService.waitForPodReady(client, pod, this.waitUntilRunning);

                    if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed")) {
                        throw PodService.failedMessage(pod);
                    }

                    // watch log
                    podLogService.watch(client, pod, logger, runContext);

                    // wait until completion of the pods
                    Pod ended;
                    if (this.outputFiles != null) {
                        ended = PodService.waitForCompletionExcept(client, logger, pod, this.waitRunning, SIDECAR_FILES_CONTAINER_NAME);
                    } else {
                        ended = PodService.waitForCompletion(client, logger, pod, this.waitRunning);
                    }

                    PodService.handleEnd(ended);

                    podWatch.close();
                    podLogService.close();

                    Output.OutputBuilder output = Output.builder()
                        .metadata(Metadata.from(ended.getMetadata()))
                        .status(PodStatus.from(ended.getStatus()))
                        .vars(podLogService.getOutputStream().getOutputs());

                    if (this.outputFiles != null) {
                        output.outputFiles(
                            this.downloadOutputFiles(runContext, PodService.podRef(client, pod), logger, additionalVars)
                        );
                    }

                    return output
                        .build();
                }
            } finally {
                delete(client, logger, pod);
                podLogService.close();
            }
        }
    }

    private Pod createPod(RunContext runContext, KubernetesClient client, String namespace, Map<String, Object> additionalVars) throws java.io.IOException, io.kestra.core.exceptions.IllegalVariableEvaluationException, URISyntaxException {
        ObjectMeta metadata = InstanceService.fromMap(
            ObjectMeta.class,
            runContext,
            additionalVars,
            this.metadata,
            metadata(runContext)
        );

        if (this.resume) {
            PodResource resumePod = client.pods()
                .inNamespace(namespace)
                .withName(metadata.getName());

            if (resumePod.get() != null) {
                runContext.logger().info("Find a resumable pods with status '{}', resume it", resumePod.get().getStatus().getPhase());
                return resumePod.get();
            } else {
                runContext.logger().debug("Unable to resume pods, start a new one");
            }
        }

        PodSpec spec = InstanceService.fromMap(
            PodSpec.class,
            runContext,
            additionalVars,
            this.spec
        );


        this.handleFiles(runContext, spec, additionalVars);

        return client.pods()
            .inNamespace(namespace)
            .resource(new PodBuilder()
                .withMetadata(metadata)
                .withSpec(spec)
                .build()
            )
            .create();
    }

    private void delete(KubernetesClient client, Logger logger, Pod pod) {
        if (delete) {
            try {
                PodService.podRef(client, pod).delete();
                logger.info("Pod '{}' is deleted ", pod.getMetadata().getName());
            } catch (Throwable e) {
                logger.warn("Unable to delete pod {}", pod.getFullResourceName(), e);
            }
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
    }
}
