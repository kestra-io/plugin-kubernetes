package org.kestra.task.kubernetes;

import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.JobSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.Await;
import org.kestra.task.kubernetes.models.Metadata;
import org.kestra.task.kubernetes.services.LoggingOutputStream;
import org.kestra.task.kubernetes.services.InstanceService;
import org.kestra.task.kubernetes.watchers.JobWatcher;
import org.kestra.task.kubernetes.watchers.PodWatcher;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Create a job on a kubernetes cluster."
)
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
@Slf4j
public class JobCreate extends AbstractConnection implements RunnableTask<JobCreate.Output> {
    @InputProperty(
        description = "The namespace where the job will be created",
        dynamic = true
    )
    @NotNull
    private String namespace;

    @InputProperty(
        description = "Full metadata yaml for a job.",
        dynamic = true
    )
    private Map<String, Object> metadata;

    @InputProperty(
        description = "Full spec yaml for a job.",
        dynamic = true
    )
    @NotNull
    private Map<String, Object> spec;

    @InputProperty(
        description = "The maximum duration we need to wait until the job & the pod is created.",
        body = {
            "This timeout is the maximum time that k8s scheduler take to",
            "* schedule the job",
            "* pull the pod image ",
            "* and start the pod",
        }
    )
    @NotNull
    @Builder.Default
    private Duration waitUntilRunning = Duration.ofMinutes(10);

    @InputProperty(
        description = "If the job will be deleted on completion"
    )
    @NotNull
    @Builder.Default
    private boolean delete = true;

    @Override
    public JobCreate.Output run(RunContext runContext) throws Exception {
        try (KubernetesClient client = this.client(runContext)) {
            String namespace = runContext.render(this.namespace);
            Logger logger = runContext.logger();

            // create the job
            Job job = client.batch()
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

            // watch for jobs
            Watch jobWatch = jobRef(client, namespace, job)
                .watch(new JobWatcher(logger));

            LogWatch jobLogs = jobRef(client, namespace, job)
                .watchLog(new LoggingOutputStream(logger, Level.DEBUG, "Job Log:"));

            // wait until pod is created
            Await.until(
                () -> client
                    .pods()
                    .withLabel("controller-uid", job.getMetadata().getUid())
                    .list()
                    .getItems()
                    .size() > 0,
                Duration.ofMillis(500),
                this.waitUntilRunning
            );

            Pod pod = client
                .pods()
                .withLabel("controller-uid", job.getMetadata().getUid())
                .list()
                .getItems()
                .stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Can't find pod for job '" + Objects.requireNonNull(job.getMetadata())
                    .getName() + "'"));

            // watch for pods
            Watch podWatch = podRef(client, namespace, pod)
                .watch(new PodWatcher(logger));

            // wait for pods ready
            podRef(client, namespace, pod)
                .waitUntilCondition(
                    j -> j.getStatus()
                        .getConditions()
                        .stream()
                        .filter(podCondition -> podCondition.getStatus().equalsIgnoreCase("True"))
                        .anyMatch(podCondition -> podCondition.getType().equals("ContainersReady") ||
                            (podCondition.getReason() != null && podCondition.getReason().equals("PodCompleted"))
                        ),
                    this.waitUntilRunning.toSeconds(),
                    TimeUnit.SECONDS
                );

            // watch log
            LogWatch podLogs = podRef(client, namespace, pod)
                .tailingLines(1000)
                .watchLog(new LoggingOutputStream(logger, Level.INFO, null));

            // wait until completion of the jobs
            jobRef(client, namespace, job)
                .waitUntilCondition(
                    j -> j.getStatus().getCompletionTime() != null,
                    this.waitUntilRunning.toSeconds(),
                    TimeUnit.SECONDS
                );

            if (delete) {
                jobRef(client, namespace, job).delete();
                logger.info("Job '{}' is deleted ", job.getMetadata().getName());
            }

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
        @OutputProperty(
            description = "The full job metadata"
        )
        private Metadata job;

        @OutputProperty(
            description = "The full pod metadata"
        )
        private Metadata pod;
    }

}
