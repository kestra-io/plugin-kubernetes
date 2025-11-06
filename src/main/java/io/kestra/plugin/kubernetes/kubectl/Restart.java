package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Schema(
        title = "The Kubernetes namespace"
    )
    @NotNull
    private Property<String> namespace;

    @Schema(title = "The Kubernetes resource type (Deployment, DaemonSet, StatefulSet).")
    @NotNull
    private Property<ResourceType> resourceType;

    @Schema(
        title = "The Kubernetes resources names"
    )
    @NotNull
    private Property<List<String>> resourcesNames;

    @Schema(
        title = "The Kubernetes resource apiGroup"
    )
    private Property<String> apiGroup;

    @Schema(
        title = "The Kubernetes resource apiVersion"
    )
    private Property<String> apiVersion;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        try (KubernetesClient client = PodService.client(runContext, this.getConnection())) {

            var rNamespace = runContext.render(this.namespace).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("namespace must be provided and rendered."));
            var rKind = runContext.render(this.resourceType).as(ResourceType.class)
                .orElseThrow(() -> new IllegalArgumentException("resourceType must be provided and rendered."));
            var rResourcesNames = runContext.render(this.resourcesNames).asList(String.class);
            var rApiGroup = runContext.render(this.apiGroup).as(String.class).orElse("apps");
            var rApiVersion = runContext.render(this.apiVersion).as(String.class).orElse("v1");

            logger.info("Triggering rolling restart for '{}' resources '{}' in namespace '{}'",
                rKind, rResourcesNames, rNamespace);

            var resourceDefinitionContext = new ResourceDefinitionContext.Builder()
                .withGroup(rApiGroup)
                .withVersion(rApiVersion)
                .withKind(rKind.name())
                .withNamespaced(true)
                .build();

            rResourcesNames.forEach(name -> {
                var resource = client.genericKubernetesResources(resourceDefinitionContext)
                    .inNamespace(rNamespace)
                    .withName(name)
                    .get();

                if (resource == null) {
                    logger.warn("Resource '{}' of kind '{}' not found in namespace '{}'",
                        name, rKind, rNamespace);
                    return;
                }

                // Ensure annotations map exists
                Map<String, String> annotations = resource.getMetadata().getAnnotations();
                if (annotations == null) {
                    annotations = new HashMap<>();
                }

                // Add the restart annotation (same behavior as kubectl rollout restart)
                annotations.put("kubectl.kubernetes.io/restartedAt", Instant.now().toString());
                resource.getMetadata().setAnnotations(annotations);

                // Update the resource on the cluster
                client.genericKubernetesResources(resourceDefinitionContext)
                    .inNamespace(rNamespace)
                    .withName(name)
                    .edit(r -> {
                        r.setMetadata(resource.getMetadata());
                        return r;
                    });

                logger.info("Rolling restart triggered for {} '{}'", rKind, name);
            });
        }

        return null;
    }

    public enum ResourceType {
        Deployment,
        DaemonSet,
        StatefulSet
    }
}
