package org.kestra.task.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.task.kubernetes.models.Metadata;
import org.kestra.task.kubernetes.models.PodStatus;
import org.kestra.task.kubernetes.services.InstanceService;
import org.kestra.task.kubernetes.services.LoggingOutputStream;
import org.kestra.task.kubernetes.services.PodService;
import org.kestra.task.kubernetes.watchers.PodWatcher;
import org.slf4j.Logger;
import org.slf4j.event.Level;

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
public class PodCreate extends AbstractConnection implements RunnableTask<PodCreate.Output> {
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

    @Override
    public PodCreate.Output run(RunContext runContext) throws Exception {
        try (KubernetesClient client = this.client(runContext)) {
            String namespace = runContext.render(this.namespace);
            Logger logger = runContext.logger();

            // create the job
            Pod pod = createPod(runContext, client, namespace);

            try {
                try (Watch podWatch = PodService.podRef(client, namespace, pod).watch(listOptions(), new PodWatcher(logger))) {
                    // wait for pods ready
                    pod = PodService.waitForPodReady(client, namespace, pod, this.waitUntilRunning);

                    if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed")) {
                        throw PodService.failedMessage(pod);
                    }

                    // watch log
                    try (LogWatch podLogs = PodService.podRef(client, namespace, pod)
                        .tailingLines(1000)
                        .watchLog(new LoggingOutputStream(logger, Level.INFO, null));
                    ) {
                        // wait until completion of the pods
                        Pod ended = PodService.waitForCompletion(client, namespace, pod, this.waitRunning);

                        // let some time to gather the logs before delete
                        Thread.sleep(1000);

                        delete(client, logger, namespace, pod);

                        podWatch.close();
                        podLogs.close();

                        return Output.builder()
                            .metadata(Metadata.from(ended.getMetadata()))
                            .status(PodStatus.from(ended.getStatus()))
                            .build();
                    }
                }
            } catch (Exception e) {
                delete(client, logger, namespace, pod);
                throw e;
            }
        }
    }

    private Pod createPod(RunContext runContext, KubernetesClient client, String namespace) throws java.io.IOException, org.kestra.core.exceptions.IllegalVariableEvaluationException {
        return client.pods()
            .inNamespace(namespace)
            .create(new PodBuilder()
                .withMetadata(InstanceService.fromMap(
                    ObjectMeta.class,
                    runContext,
                    this.metadata,
                    metadata(runContext)
                ))
                .withSpec(InstanceService.fromMap(
                    PodSpec.class,
                    runContext,
                    this.spec,
                    metadata(runContext)
                ))
                .build()
            );
    }

    private void delete(KubernetesClient client, Logger logger, String namespace, Pod pod) {
        if (delete) {
            PodService.podRef(client, namespace, pod).delete();
            logger.info("Pod '{}' is deleted ", pod.getMetadata().getName());
        }
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @Schema(
            title = "The full pod metadata"
        )
        private final Metadata metadata;

        @Schema(
            title = "The full pod status"
        )
        private final PodStatus status;
    }

}
