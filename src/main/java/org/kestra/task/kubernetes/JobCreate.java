package org.kestra.task.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.JobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.Await;
import org.kestra.task.kubernetes.models.Metadata;
import org.kestra.task.kubernetes.services.InstanceService;
import org.kestra.task.kubernetes.services.LoggingOutputStream;
import org.kestra.task.kubernetes.watchers.JobWatcher;
import org.kestra.task.kubernetes.watchers.PodWatcher;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a job on a kubernetes cluster."
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
                "  template:",
                "    spec:",
                "      containers:",
                "      - name: unittest",
                "        image: debian:stable-slim",
                "        command: ",
                "          - 'bash' ",
                "          - '-c'",
                "          - 'for i in {1..10}; do echo $i; sleep 0.1; done'",
                "      restartPolicy: Never"
            }
        )
    }
)
@Slf4j
public class JobCreate extends AbstractConnection implements RunnableTask<JobCreate.Output> {
    @Schema(
        title = "The namespace where the job will be created"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String namespace;

    @Schema(
        title = "Full metadata yaml for a job."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> metadata;

    @Schema(
        title = "Full spec yaml for a job."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Map<String, Object> spec;

    @Schema(
        title = "The maximum duration we need to wait until the job & the pod is created.",
        description = "This timeout is the maximum time that k8s scheduler take to\n" +
            "* schedule the job\n" +
            "* pull the pod image\n" +
            "* and start the pod"
    )
    @NotNull
    @Builder.Default
    private final Duration waitUntilRunning = Duration.ofMinutes(10);

    @Schema(
        title = "The maximum duration we need to wait until the job complete."
    )
    @NotNull
    @Builder.Default
    private final Duration waitRunning = Duration.ofHours(1);

    @Schema(
        title = "If the job will be deleted on completion"
    )
    @NotNull
    @Builder.Default
    private final Boolean delete = true;

    @Override
    public JobCreate.Output run(RunContext runContext) throws Exception {
        try (KubernetesClient client = this.client(runContext)) {
            String namespace = runContext.render(this.namespace);
            Logger logger = runContext.logger();

            // create the job
            Job job = createJob(runContext, client, namespace);

            // timeout for watch
            ListOptions listOptions = new ListOptionsBuilder()
                .withTimeoutSeconds(this.waitRunning.toSeconds())
                .build();

            try {
                // watch for jobs
                try (
                    Watch jobWatch = jobRef(client, namespace, job).watch(listOptions, new JobWatcher(logger));
                    LogWatch jobLogs = jobRef(client, namespace, job).watchLog(new LoggingOutputStream(logger, Level.DEBUG, "Job Log:"));
                ) {
                    // wait until pod is created
                    waitForPodCreated(client, namespace, job);
                    Pod pod = findPod(client, namespace, job);

                    // watch for pods
                    try (Watch podWatch = podRef(client, namespace, pod).watch(listOptions, new PodWatcher(logger))) {
                        // wait for pods ready
                        pod = waitForPods(client, namespace, pod);

                        if (pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed")) {
                            throw failedMessage(pod);
                        }

                        // watch log
                        try (LogWatch podLogs = podRef(client, namespace, pod)
                            .tailingLines(1000)
                            .watchLog(new LoggingOutputStream(logger, Level.INFO, null));
                        ) {
                            // wait until completion of the jobs
                            waitForJobCompletion(client, namespace, job);

                            delete(client, logger, namespace, job);

                            jobWatch.close();
                            jobLogs.close();
                            podWatch.close();
                            podLogs.close();

                            return Output.builder()
                                .job(Metadata.fromObjectMeta(job.getMetadata()))
                                .pod(Metadata.fromObjectMeta(pod.getMetadata()))
                                .build();
                        }
                    }
                }
            } catch (Exception e) {
                delete(client, logger, namespace, job);
                throw e;
            }
        }
    }

    private IllegalStateException failedMessage(Pod pod) throws IllegalStateException {
        if (pod.getStatus() == null) {
            return new IllegalStateException("Pods terminated without any status !");
        }

        return (pod.getStatus().getContainerStatuses() == null ? new ArrayList<ContainerStatus>() : pod.getStatus().getContainerStatuses())
            .stream()
            .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState()
                .getTerminated() != null)
            .map(containerStatus -> containerStatus.getState().getTerminated())
            .findFirst()
            .map(containerStateTerminated -> new IllegalStateException(
                "Pods terminated with status '" + pod.getStatus().getPhase() + "', " +
                "exitcode '" + containerStateTerminated.getExitCode() + "' & " +
                "message '" + containerStateTerminated.getMessage() + "'"
            ))
            .orElse(new IllegalStateException("Pods terminated without any containers status !"));
    }

    private Job waitForJobCompletion(KubernetesClient client, String namespace, Job job) throws InterruptedException {
        return jobRef(client, namespace, job)
            .waitUntilCondition(
                j -> j.getStatus() == null || j.getStatus().getCompletionTime() != null,
                this.waitRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    private void waitForPodCreated(KubernetesClient client, String namespace, Job job) throws TimeoutException {
        Await.until(
            () -> client
                .pods()
                .inNamespace(namespace)
                .withLabel("controller-uid", job.getMetadata().getUid())
                .list()
                .getItems()
                .size() > 0,
            Duration.ofMillis(500),
            this.waitUntilRunning
        );
    }

    private Pod waitForPods(KubernetesClient client, String namespace, Pod pod) throws InterruptedException {
        return podRef(client, namespace, pod)
            .waitUntilCondition(
                j -> j.getStatus() == null ||
                    j.getStatus().getPhase().equals("Failed") ||
                    j.getStatus()
                        .getConditions()
                        .stream()
                        .filter(podCondition -> podCondition.getStatus().equalsIgnoreCase("True"))
                        .anyMatch(podCondition -> podCondition.getType().equals("ContainersReady") ||
                            (podCondition.getReason() != null && podCondition.getReason()
                                .equals("PodCompleted"))
                        ),
                this.waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    private Pod findPod(KubernetesClient client, String namespace, Job job) {
        return client
            .pods()
            .inNamespace(namespace)
            .withLabel("controller-uid", job.getMetadata().getUid())
            .list()
            .getItems()
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "Can't find pod for job '" + Objects.requireNonNull(job.getMetadata()).getName() + "'"
            ));
    }

    private Job createJob(RunContext runContext, KubernetesClient client, String namespace) throws java.io.IOException, org.kestra.core.exceptions.IllegalVariableEvaluationException {
        return client.batch()
            .jobs()
            .inNamespace(namespace)
            .create(new JobBuilder()
                .withMetadata(InstanceService.fromMap(
                    client,
                    ObjectMeta.class,
                    runContext,
                    this.metadata,
                    metadata(runContext)
                ))
                .withSpec(InstanceService.fromMap(
                    client,
                    JobSpec.class,
                    runContext,
                    this.spec,
                    metadata(runContext)
                ))
                .build()
            );
    }

    private void delete(KubernetesClient client, Logger logger, String namespace, Job job) {
        if (delete) {
            jobRef(client, namespace, job).delete();
            logger.info("Job '{}' is deleted ", job.getMetadata().getName());
        }
    }

    private static ScalableResource<Job, DoneableJob> jobRef(KubernetesClient client, String namespace, Job job) {
        return client
            .batch()
            .jobs()
            .inNamespace(namespace)
            .withName(job.getMetadata().getName());
    }

    private static PodResource<Pod, DoneablePod> podRef(KubernetesClient client, String namespace, Pod pod) {
        return client.pods()
            .inNamespace(namespace)
            .withName(pod.getMetadata().getName());
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @Schema(
            title = "The full job metadata"
        )
        private final Metadata job;

        @Schema(
            title = "The full pod metadata"
        )
        private final Metadata pod;
    }

}
