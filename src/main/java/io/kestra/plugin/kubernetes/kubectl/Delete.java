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

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Delete a list of pods from Kubernetes.",
            full = true,
            code = """
                id: delete_pods
                namespace: company.team
                
                tasks:
                  - id: delete
                    type: io.kestra.plugin.kubernetes.kubectl.Delete
                    connection:
                      masterUrl: "{{ secret('K8S_MASTER_URL') }}"
                      oauthToken: "{{ secret('K8S_TOKEN') }}"
                    namespace: default
                    resourceType: pods
                    resourcesNames:
                      - my-pod
                      - my-pod-2
                """
        ),
        @Example(
            title = "Delete a list of pods from Kubernetes based on inputs.",
            full = true,
            code = """
                id: delete_pods
                namespace: company.team

                inputs:
                  - id: resources
                    type: MULTISELECT
                    allowCustomValue: true
                    values:
                      - my-pod
                      - my-pod-2

                tasks:
                  - id: delete
                    type: io.kestra.plugin.kubernetes.kubectl.Delete
                    connection:
                      masterUrl: "{{ secret('K8S_MASTER_URL') }}"
                      oauthToken: "{{ secret('K8S_TOKEN') }}"
                      trustCerts: true
                    namespace: default
                    resourceType: pods
                    resourcesNames: "{{ inputs.resources }}"
            """
        )
    }
)
@Schema(
    title = "Delete Kubernetes resources by kind and name",
    description = "Deletes the provided resource names of a given kind in a namespace using the specified apiGroup/apiVersion (default v1, core group). Supports only namespaced resources."
)
public class Delete extends AbstractPod implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Resource kind",
        description = "Kubernetes kind (e.g., Pod, Deployment, Service). Case-insensitive."
    )
    @NotNull
    private Property<String> resourceType;

    @Schema(
        title = "Resource names",
        description = "List of resource names to delete in the target namespace."
    )
    @NotNull
    private Property<List<String>> resourcesNames;

    @Schema(
        title = "API group",
        description = "Group for the resource kind (empty for core resources)."
    )
    private Property<String> apiGroup;

    @Schema(
        title = "API version",
        description = "Version for the resource kind. Defaults to v1 when omitted."
    )
    private Property<String> apiVersion;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {

        try (KubernetesClient client = PodService.client(runContext, this.getConnection())) {

            var rNamespace = runContext.render(this.namespace).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("namespace must be provided and rendered."));
            var rResourceType = runContext.render(this.resourceType).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("resourceType must be provided and rendered."));
            var rResourcesNames = runContext.render(this.resourcesNames).asList(String.class);
            var rApiGroup = runContext.render(this.apiGroup).as(String.class).orElse("");
            var rApiVersion = runContext.render(this.apiVersion).as(String.class).orElse("v1");

            runContext.logger().debug("Deleting resource(s) '{}' of kind '{}' in namespace '{}'", rResourcesNames, rResourceType, rNamespace);

            var resourceDefinitionContext = new ResourceDefinitionContext.Builder()
                .withGroup(rApiGroup)
                .withVersion(rApiVersion)
                .withKind(rResourceType)
                .withNamespaced(true) // Assuming resources are namespaced as we take namespace input
                .build();

            rResourcesNames.forEach(name ->
                client.genericKubernetesResources(resourceDefinitionContext)
                    .inNamespace(rNamespace)
                    .withName(name)
                    .delete()
            );
        }

        return null;
    }
}
