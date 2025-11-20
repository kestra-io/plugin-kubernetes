package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.AbstractPod;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Trigger a rolling restart of a StatefulSet named 'api' (equivalent to `kubectl rollout restart statefulset api`).",
            full = true,
            code = """
                id: restart_api
                namespace: company.team

                tasks:
                  - id: restart
                    type: io.kestra.plugin.kubernetes.kubectl.Restart
                    namespace: api
                    resourceType: StatefulSet
                    resourcesNames:
                      - api
                    connection:
                      masterUrl: "{{ secret('MASTER_URL') }}"
                      caCertData: "{{ secret('CA_CERT_DATA') }}"
                      oauthToken: "{{ secret('OAUTH_TOKEN') }}"
                """
        )
    }
)
@Schema(
    title = "Trigger a rolling restart of one or multiple Kubernetes resources (Deployment, DaemonSet, StatefulSet)."
)
public class Restart extends AbstractPod implements RunnableTask<VoidOutput> {

    @Schema(title = "The Kubernetes resource type (Deployment, DaemonSet, StatefulSet).")
    @NotNull
    private Property<ResourceType> resourceType;

    @Schema(title = "The Kubernetes resource names to restart.")
    @NotNull
    private Property<List<String>> resourcesNames;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        try (KubernetesClient client = PodService.client(runContext, this.getConnection())) {

            var rNamespace = runContext.render(this.namespace).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("namespace must be provided and rendered."));
            var rResourceType = runContext.render(this.resourceType).as(ResourceType.class)
                .orElseThrow(() -> new IllegalArgumentException("resourceType must be provided and rendered."));
            var rResourcesNames = runContext.render(this.resourcesNames).asList(String.class);

            var rWaitUntilReady = runContext.render(this.waitUntilReady).as(Duration.class).orElse(Duration.ZERO);
            if (!rWaitUntilReady.isZero()) {
                logger.warn("waitUntilReady parameter is not yet supported by Restart task and will be ignored. The task will return immediately after triggering the restart.");
            }

            logger.info("Triggering rolling restart for '{}' resources '{}' in namespace '{}'",
                rResourceType, rResourcesNames, rNamespace);

            for (String name : rResourcesNames) {
                logger.info("Restarting {} '{}'", rResourceType, name);

                switch (rResourceType) {
                    case Deployment -> client.apps()
                        .deployments()
                        .inNamespace(rNamespace)
                        .withName(name)
                        .rolling()
                        .restart();

                    case StatefulSet -> client.apps()
                        .statefulSets()
                        .inNamespace(rNamespace)
                        .withName(name)
                        .rolling()
                        .restart();

                    default -> throw new IllegalStateException("Unsupported resource type: " + rResourceType);
                }
            }

        }

        return null;
    }

    /**
     * Enum representing supported resource types for rolling restart.
     */
    public enum ResourceType {
        Deployment,
        StatefulSet
    }
}
