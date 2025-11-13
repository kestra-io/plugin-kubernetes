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
            title = "Delete a list of pods from Kubernetes using YAML (<=> kubectl delete pod my-pod my-pod-2).",
            full = true,
            code = """
                id: delete_pods
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.kubernetes.kubectl.Delete
                    namespace: default
                    resourceType: pods
                    resourcesNames:
                      - my-pod
                      - my-pod-2
                """
        )
    }
)
@Schema(
    title = "Delete one or many Kubernetes resources of a kind."
)
public class Delete extends AbstractPod implements RunnableTask<VoidOutput> {

    

    @Schema(
        title = "The Kubernetes resource type (= kind) (e.g., pod, service)"
    )
    @NotNull
    private Property<String> resourceType;

    @Schema(
        title = "The Kubernetes resources names"
    )
    @NotNull
    private Property<List<String>> resourcesNames;

   

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {

        try (KubernetesClient client = PodService.client(runContext, this.getConnection())) {

            var renderedNamespace = runContext.render(this.namespace).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("namespace must be provided and rendered."));
            var renderedKind = runContext.render(this.resourceType).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("resourceType must be provided and rendered."));
            var renderedResourcesNames = runContext.render(this.resourcesNames).asList(String.class);
            var renderedApiGroup = runContext.render(this.apiGroup).as(String.class).orElse("");
            var renderedApiVersion = runContext.render(this.apiVersion).as(String.class).orElse("v1");

            runContext.logger().debug("Deleting resource(s) '{}' of kind '{}' in namespace '{}'", renderedResourcesNames, renderedKind, renderedNamespace);

            var resourceDefinitionContext = new ResourceDefinitionContext.Builder()
                .withGroup(renderedApiGroup)
                .withVersion(renderedApiVersion)
                .withKind(renderedKind)
                .withNamespaced(true) // Assuming resources are namespaced as we take namespace input
                .build();

            renderedResourcesNames.forEach(name ->
                client.genericKubernetesResources(resourceDefinitionContext)
                    .inNamespace(renderedNamespace)
                    .withName(name)
                    .delete()
            );
        }

        return null;
    }
}
