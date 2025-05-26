package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.AbstractPod;
import io.kestra.plugin.kubernetes.models.Metadata;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Get all pods from Kubernetes using YAML (<=> kubectl get pods).",
            full = true,
            code = """
                    id: get_all_pods
                    namespace: company.team

                    tasks:
                      - id: get
                        type: io.kestra.plugin.kubernetes.kubectl.Get
                        namespace: default
                        resourceType: PODS
                """
        ),
        @Example(
            title = "Get one deployment named my-deployment from Kubernetes using YAML (<=> kubectl get deployment my-deployment).",
            full = true,
            code = """
                    id: get_one_deployment
                    namespace: company.team

                    tasks:
                      - id: get
                        type: io.kestra.plugin.kubernetes.kubectl.Get
                        namespace: default
                        resourceType: DEPLOYMENTS
                        resourcesNames:
                            - my-deployment
                """
        ),
        @Example(
            title = "Get two deployment named my-deployment and my-deployment-2 from Kubernetes using YAML (<=> kubectl get deployment my-deployment).",
            full = true,
            code = """
                    id: get_one_deployment
                    namespace: company.team

                    tasks:
                      - id: get
                        type: io.kestra.plugin.kubernetes.kubectl.Get
                        namespace: default
                        resourceType: DEPLOYMENTS
                        resourcesNames:
                            - my-deployment
                            - my-deployment-2
                """
        )
    }
)
@Schema(
    title = "Get one or many Kubernetes resources of a kind."
)
@Slf4j
public class Get extends AbstractPod implements RunnableTask<Get.Output> {

    @Schema(
        title = "The Kubernetes namespace"
    )
    @NotNull
    private Property<String> namespace;

    @Schema(
        title = "The Kubernetes resource type (= kind) (e.g. pod, service)"
    )
    @NotNull
    private Property<KubernetesKind> resourceType;

    @Schema(
        title = "The Kubernetes resources names"
    )
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
    public Output run(RunContext runContext) throws Exception {
        var renderedNamespace = runContext.render(this.namespace).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Namespace must be provided and rendered."));
        var renderedKind = runContext.render(this.resourceType).as(KubernetesKind.class)
            .orElseThrow(() -> new IllegalArgumentException("Kind must be provided and rendered."));
        var renderedResourcesNames = runContext.render(this.resourcesNames).asList(String.class);
        var renderedApiGroup = runContext.render(this.apiGroup).as(String.class).orElse("apps");
        var renderedApiVersion = runContext.render(this.apiVersion).as(String.class).orElse("v1");

        List<Metadata> metadataList = new ArrayList<>();

        try (KubernetesClient client = PodService.client(runContext, this.getConnection())) {

            var resourceDefinitionContext = new ResourceDefinitionContext.Builder()
                .withGroup(renderedApiGroup)
                .withVersion(renderedApiVersion)
                .withKind(renderedKind.name())
                .withPlural(renderedKind.getPlural())
                .withNamespaced(true) // Assuming resources are namespaced as we take namespace input
                .build();

            if (renderedResourcesNames.isEmpty()) {
                runContext.logger().debug("Fetching all resources of kind '{}' in namespace '{}'", renderedKind, renderedNamespace);
                var resources = client.genericKubernetesResources(resourceDefinitionContext)
                    .inNamespace(renderedNamespace)
                    .list()
                    .getItems();

                for (GenericKubernetesResource resource : resources) {
                    if (resource != null && resource.getMetadata() != null) {
                        metadataList.add(Metadata.from(resource.getMetadata()));
                    }
                }
                runContext.logger().info("Fetched {} resource(s) of kind '{}' in namespace '{}'", metadataList.size(), renderedKind, renderedNamespace);

            } else {
                renderedResourcesNames.forEach(name -> {
                        runContext.logger().debug("Fetching resource of kind '{}' with name '{}' in namespace '{}'",
                            renderedKind, name, renderedNamespace);

                        var resource = client.genericKubernetesResources(resourceDefinitionContext)
                            .inNamespace(renderedNamespace)
                            .withName(name)
                            .get();

                        if (resource != null && resource.getMetadata() != null) {
                            metadataList.add(Metadata.from(resource.getMetadata()));
                            runContext.logger().info("Fetched resource of kind '{}' with name '{}' in namespace '{}'",
                                renderedKind, name, renderedNamespace);
                        } else {
                            runContext.logger().warn("Resource of kind '{}' with name '{}' not found in namespace '{}'",
                                renderedKind, name, renderedNamespace);
                        }
                    }
                );
            }

        } catch (KubernetesClientException e) {
            runContext.logger().error("Kubernetes API error while fetching kind '{}' in namespace '{}': {}", renderedKind, renderedNamespace, e.getMessage(), e);
            throw new Exception("Failed to interact with Kubernetes API: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            runContext.logger().error("Configuration error: {}", e.getMessage(), e);
            throw e;
        }

        return Output.builder()
            .metadata(metadataList)
            .build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The resource(s) metadata."
        )
        private final List<Metadata> metadata;
    }
}
