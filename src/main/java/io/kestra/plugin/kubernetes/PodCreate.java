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
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a pod on a kubernetes cluster."
)
@Plugin(
    examples = {
        @Example(
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
    private String namespace;

    @Schema(
        title = "Full metadata yaml for a pod."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> metadata;

    @Schema(
        title = "Full spec yaml for a pod."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Map<String, Object> spec;

    @Schema(
        title = "The maximum duration we need to wait until the pod is created.",
        description = "This timeout is the maximum time that k8s scheduler take to\n" +
            "* schedule the pod\n" +
            "* pull the pod image\n" +
            "* and start the pod"
    )
    @NotNull
    @Builder.Default
    private final Duration waitUntilRunning = Duration.ofMinutes(10);

    @Schema(
        title = "The maximum duration we need to wait until the pod complete."
    )
    @NotNull
    @Builder.Default
    private final Duration waitRunning = Duration.ofHours(1);

    @Schema(
        title = "If the pod will be deleted on completion"
    )
    @NotNull
    @Builder.Default
    private final Boolean delete = true;

    @Schema(
        title = "If we try to reconnect to current pod if it exist"
    )
    @NotNull
    @Builder.Default
    private final Boolean resume = true;

    @Override
    public PodCreate.Output run(RunContext runContext) throws Exception {
        super.init(runContext);

        try (KubernetesClient client = this.client(runContext)) {
            String namespace = runContext.render(this.namespace);
            Logger logger = runContext.logger();

            // create the job
            Pod pod = createPod(runContext, client, namespace);
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
                            this.downloadOutputFiles(runContext, PodService.podRef(client, pod), logger)
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

    private Pod createPod(RunContext runContext, KubernetesClient client, String namespace) throws java.io.IOException, io.kestra.core.exceptions.IllegalVariableEvaluationException, URISyntaxException {
        ObjectMeta metadata = InstanceService.fromMap(
            ObjectMeta.class,
            runContext,
            this.metadata,
            metadata(runContext)
        );

        PodSpec spec = InstanceService.fromMap(
            PodSpec.class,
            runContext,
            this.spec
        );


        this.handleFiles(runContext, metadata, spec);

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
            title = "The full pod metadata"
        )
        private final Metadata metadata;

        @Schema(
            title = "The full pod status"
        )
        private final PodStatus status;

        @Schema(
            title = "The output files uri in Kestra internal storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> outputFiles;

        @Schema(
            title = "The value extract from output of the commands"
        )
        private final Map<String, Object> vars;
    }
}
