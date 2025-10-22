package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.AbstractPod;
import io.kestra.plugin.kubernetes.models.Metadata;
import io.kestra.plugin.kubernetes.models.PatchStrategy;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Patch a deployment to update container resources using strategic merge (default).",
            full = true,
            code = """
                id: patch_deployment_resources
                namespace: company.team

                tasks:
                  - id: patch
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: deployment
                    resourceName: my-api
                    patch: |
                      {
                        "spec": {
                          "template": {
                            "spec": {
                              "containers": [
                                {
                                  "name": "api",
                                  "resources": {
                                    "limits": {"memory": "2Gi", "cpu": "1000m"},
                                    "requests": {"memory": "1Gi", "cpu": "500m"}
                                  }
                                }
                              ]
                            }
                          }
                        }
                      }
                """
        ),
        @Example(
            title = "Scale a deployment using JSON Patch operations.",
            full = true,
            code = """
                id: scale_deployment
                namespace: company.team

                tasks:
                  - id: scale
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: deployment
                    resourceName: my-api
                    patchStrategy: JSON_PATCH
                    patch: |
                      [
                        {"op": "replace", "path": "/spec/replicas", "value": 5}
                      ]
                """
        ),
        @Example(
            title = "Remove an annotation using JSON Merge Patch.",
            full = true,
            code = """
                id: remove_annotation
                namespace: company.team

                tasks:
                  - id: patch
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: deployment
                    resourceName: my-api
                    patchStrategy: JSON_MERGE
                    patch: |
                      {
                        "metadata": {
                          "annotations": {
                            "deprecated-annotation": null
                          }
                        }
                      }
                """
        ),
        @Example(
            title = "Patch a custom resource.",
            full = true,
            code = """
                id: patch_custom_resource
                namespace: company.team

                tasks:
                  - id: patch
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: shirts
                    resourceName: my-shirt
                    apiGroup: stable.example.com
                    apiVersion: v1
                    patch: |
                      {
                        "spec": {
                          "color": "blue",
                          "size": "L"
                        }
                      }
                """
        ),
        @Example(
            title = "Conditionally update replicas using JSON Patch test operation.",
            full = true,
            code = """
                id: conditional_scale
                namespace: company.team

                tasks:
                  - id: scale
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: deployment
                    resourceName: my-api
                    patchStrategy: JSON_PATCH
                    patch: |
                      [
                        {"op": "test", "path": "/spec/replicas", "value": 3},
                        {"op": "replace", "path": "/spec/replicas", "value": 10}
                      ]
                """
        )
    }
)
@Schema(
    title = "Patch a Kubernetes resource with targeted updates.",
    description = "Unlike kubectl.Apply which requires the full resource specification, " +
        "kubectl.Patch allows targeted updates to specific fields. Supports three patch strategies: " +
        "Strategic Merge (default), JSON Merge, and JSON Patch."
)
@Slf4j
public class Patch extends AbstractPod implements RunnableTask<Patch.Output> {

    @NotNull
    @Schema(
        title = "The Kubernetes namespace"
    )
    private Property<String> namespace;

    @NotNull
    @Schema(
        title = "The Kubernetes resource type (e.g., deployment, statefulset, pod)"
    )
    private Property<String> resourceType;

    @NotNull
    @Schema(
        title = "The name of the Kubernetes resource to patch"
    )
    private Property<String> resourceName;

    @NotNull
    @Schema(
        title = "The patch content",
        description = "The format depends on the patchStrategy. For STRATEGIC_MERGE and JSON_MERGE, " +
            "provide a JSON object with the fields to update. For JSON_PATCH, provide a JSON array " +
            "of operations with 'op', 'path', and 'value' fields."
    )
    private Property<String> patch;

    @Builder.Default
    @Schema(
        title = "The patch strategy to use",
        description = "STRATEGIC_MERGE (default): Kubernetes strategic merge patch, most user-friendly. " +
            "Understands K8s resource structure and intelligently merges lists by merge keys. " +
            "JSON_MERGE: Simple merge with null-deletion semantics (RFC 7386). " +
            "JSON_PATCH: Precision operations with add/remove/replace/test (RFC 6902)."
    )
    private Property<PatchStrategy> patchStrategy = Property.ofValue(PatchStrategy.STRATEGIC_MERGE);

    @Schema(
        title = "The Kubernetes resource apiGroup",
        description = "Required for custom resources. For core resources (pods, services, etc.), leave empty."
    )
    private Property<String> apiGroup;

    @Schema(
        title = "The Kubernetes resource apiVersion",
        description = "Default is 'v1'. For apps resources (deployments, statefulsets), use 'apps/v1'."
    )
    private Property<String> apiVersion;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // Render all properties
        var namespace = runContext.render(this.namespace).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("namespace must be provided"));
        var resourceType = runContext.render(this.resourceType).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("resourceType must be provided"));
        var resourceName = runContext.render(this.resourceName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("resourceName must be provided"));
        var patchContent = runContext.render(this.patch).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("patch content must be provided"));
        var strategy = runContext.render(this.patchStrategy).as(PatchStrategy.class)
            .orElse(PatchStrategy.STRATEGIC_MERGE);
        var apiGroup = runContext.render(this.apiGroup).as(String.class).orElse("");
        var apiVersion = runContext.render(this.apiVersion).as(String.class).orElse(determineApiVersion(resourceType, apiGroup));

        try (var client = PodService.client(runContext, this.getConnection())) {
            // Build resource definition context
            var resourceContext = new ResourceDefinitionContext.Builder()
                .withGroup(apiGroup)
                .withVersion(apiVersion)
                .withKind(resourceType)
                .withNamespaced(true)
                .build();

            // Get resource client
            var resourceClient = client.genericKubernetesResources(resourceContext)
                .inNamespace(namespace)
                .withName(resourceName);

            // Apply patch based on strategy
            log.info("Patching {} '{}' in namespace '{}' using {} strategy",
                resourceType, resourceName, namespace, strategy);
            log.debug("Patch content: {}", patchContent);

            var patchedResource = switch (strategy) {
                case STRATEGIC_MERGE -> resourceClient.patch(patchContent);
                case JSON_MERGE -> resourceClient.patch(
                    PatchContext.of(PatchType.JSON_MERGE),
                    patchContent
                );
                case JSON_PATCH -> resourceClient.patch(
                    PatchContext.of(PatchType.JSON),
                    patchContent
                );
            };

            if (patchedResource == null) {
                throw new Exception("Failed to patch resource: resource not found or patch failed");
            }

            log.info("Successfully patched {} '{}'", resourceType, resourceName);

            return Output.builder()
                .metadata(Metadata.from(patchedResource.getMetadata()))
                .build();
        }
    }

    /**
     * Determines the appropriate API version based on resource type.
     * This is a helper to provide sensible defaults for common resources.
     */
    private String determineApiVersion(String resourceType, String apiGroup) {
        if (!apiGroup.isEmpty()) {
            // Custom resources - let the user specify or use v1 as default
            return "v1";
        }

        // Common resource types that use apps/v1
        var appsV1Resources = java.util.Set.of(
            "deployment", "deployments",
            "statefulset", "statefulsets",
            "daemonset", "daemonsets",
            "replicaset", "replicasets"
        );

        if (appsV1Resources.contains(resourceType.toLowerCase())) {
            return "apps/v1";
        }

        // Default to v1 for core resources (pods, services, configmaps, etc.)
        return "v1";
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The resource metadata after patching"
        )
        private final Metadata metadata;
    }
}
