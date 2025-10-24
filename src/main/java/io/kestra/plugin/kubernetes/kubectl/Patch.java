package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.KubernetesClient;
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
import io.kestra.plugin.kubernetes.models.ResourceStatus;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Patch pod resource limits and wait for it to become ready.",
            full = true,
            code = """
                id: patch_pod_with_wait
                namespace: company.team

                tasks:
                  - id: patch
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: pod
                    resourceName: my-pod
                    waitUntilReady: PT5M
                    patch: |
                      {
                        "spec": {
                          "containers": [
                            {
                              "name": "app",
                              "resources": {
                                "limits": {"memory": "512Mi", "cpu": "500m"},
                                "requests": {"memory": "256Mi", "cpu": "250m"}
                              }
                            }
                          ]
                        }
                      }
                """
        ),
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
                    apiGroup: apps
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
                    apiGroup: apps
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
                    apiGroup: apps
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
                    apiGroup: apps
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
public class Patch extends AbstractPod implements RunnableTask<Patch.Output> {

    @NotNull
    @Schema(
        title = "The Kubernetes namespace"
    )
    private Property<String> namespace;

    @NotNull
    @Schema(
        title = "The Kubernetes resource type (e.g., deployment, statefulset, pod)",
        description = "Note: Currently only namespaced resources are supported. Cluster-scoped resources (e.g., ClusterRole, Node) are not supported."
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
        title = "The Kubernetes resource API version",
        description = "The version part of the API (e.g., 'v1', 'v1beta1'). Default is 'v1'. " +
            "Note: This is just the version, not the full group/version. Use apiGroup for the group part."
    )
    private Property<String> apiVersion;

    @Builder.Default
    @Schema(
        title = "The maximum duration to wait until the patched resource becomes ready",
        description = "When set to a positive duration, waits for the resource to report Ready=True in its status conditions. " +
            "Set to PT0S (zero) to skip waiting. " +
            "Supports Pods, StatefulSets, and custom resources that use the Ready condition. " +
            "Note: Deployments are not supported as they use the Available condition instead of Ready."
    )
    private Property<Duration> waitUntilReady = Property.ofValue(Duration.ZERO);

    @Override
    public Output run(RunContext runContext) throws Exception {
        // Render all properties
        var rNamespace = runContext.render(this.namespace).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("namespace must be provided"));
        var rResourceType = runContext.render(this.resourceType).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("resourceType must be provided"));
        var rResourceName = runContext.render(this.resourceName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("resourceName must be provided"));
        var rPatchContent = runContext.render(this.patch).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("patch content must be provided"));
        var rStrategy = runContext.render(this.patchStrategy).as(PatchStrategy.class)
            .orElse(PatchStrategy.STRATEGIC_MERGE);
        var rApiGroup = runContext.render(this.apiGroup).as(String.class).orElse("");
        var rApiVersion = runContext.render(this.apiVersion).as(String.class).orElse("v1");
        var rWaitUntilReady = runContext.render(this.waitUntilReady).as(Duration.class).orElse(Duration.ZERO);

        try (var client = PodService.client(runContext, this.getConnection())) {
            // Build resource definition context
            var resourceContext = new ResourceDefinitionContext.Builder()
                .withGroup(rApiGroup)
                .withVersion(rApiVersion)
                .withKind(rResourceType)
                .withNamespaced(true)
                .build();

            // Get resource client
            var resourceClient = client.genericKubernetesResources(resourceContext)
                .inNamespace(rNamespace)
                .withName(rResourceName);

            // Apply patch based on strategy
            runContext.logger().info("Patching {} '{}' in namespace '{}' using {} strategy",
                rResourceType, rResourceName, rNamespace, rStrategy);
            runContext.logger().debug("Patch content: {}", rPatchContent);

            var patchedResource = switch (rStrategy) {
                case STRATEGIC_MERGE -> resourceClient.patch(rPatchContent);
                case JSON_MERGE -> resourceClient.patch(
                    PatchContext.of(PatchType.JSON_MERGE),
                    rPatchContent
                );
                case JSON_PATCH -> resourceClient.patch(
                    PatchContext.of(PatchType.JSON),
                    rPatchContent
                );
            };

            if (patchedResource == null) {
                String errorMsg = "Failed to patch resource: resource not found or patch failed";
                runContext.logger().error("{} - Type: {}, Name: {}, Namespace: {}", errorMsg, rResourceType, rResourceName, rNamespace);
                throw new IllegalStateException(errorMsg);
            }

            runContext.logger().info("Successfully patched {} '{}'", rResourceType, rResourceName);
            runContext.logger().debug("Patched resource: {}", patchedResource);

            // Optionally wait for resource to become ready
            if (!rWaitUntilReady.isZero()) {
                runContext.logger().info("Waiting for resource '{}' to become ready (timeout: {})...",
                    rResourceName, rWaitUntilReady);
                waitForResourceReady(client, resourceContext, rNamespace, rResourceName, rWaitUntilReady, runContext);
                runContext.logger().info("Resource '{}' is ready", rResourceName);

                // Re-fetch resource after wait to get current status
                patchedResource = resourceClient.get();
                if (patchedResource != null) {
                    runContext.logger().debug("Refreshed resource status after wait: generation={}, resourceVersion={}",
                        patchedResource.getMetadata().getGeneration(),
                        patchedResource.getMetadata().getResourceVersion());
                }
            }

            return Output.builder()
                .metadata(Metadata.from(patchedResource.getMetadata()))
                .status(ResourceStatus.from(patchedResource))
                .build();
        }
    }

    /**
     * Waits for a Kubernetes resource to become ready after patching.
     * <p>
     * This method checks for a Ready=True condition in the resource status.
     * Works with Pods, StatefulSets, and custom resources that use the Ready condition.
     * Does not work with Deployments (which use the Available condition).
     *
     * @param client the Kubernetes client
     * @param resourceContext the resource definition context
     * @param namespace the namespace of the resource
     * @param resourceName the name of the resource
     * @param timeout the maximum duration to wait
     * @param runContext the run context for logging
     */
    private void waitForResourceReady(
        KubernetesClient client,
        ResourceDefinitionContext resourceContext,
        String namespace,
        String resourceName,
        Duration timeout,
        RunContext runContext
    ) {
        runContext.logger().debug("Checking readiness for resource '{}' in namespace '{}'", resourceName, namespace);

        var resourceClient = client.genericKubernetesResources(resourceContext)
            .inNamespace(namespace)
            .withName(resourceName);

        try {
            // Use waitUntilCondition with a predicate
            resourceClient.waitUntilCondition(
                resource -> resource != null && isResourceReady(resource, runContext),
                timeout.toSeconds(),
                TimeUnit.SECONDS
            );
            runContext.logger().debug("Resource '{}' readiness confirmed", resourceName);
        } catch (Exception e) {
            runContext.logger().error("Failed to wait for resource '{}' to become ready in namespace '{}': {}",
                resourceName, namespace, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Checks if a resource has a Ready=True condition (default check).
     */
    @SuppressWarnings("unchecked")
    private boolean isResourceReady(GenericKubernetesResource resource, RunContext runContext) {
        try {
            var additionalProperties = resource.getAdditionalProperties();
            if (additionalProperties == null) {
                return false;
            }

            var status = additionalProperties.get("status");
            if (!(status instanceof Map)) {
                return false;
            }

            var statusMap = (Map<String, Object>) status;
            var conditions = statusMap.get("conditions");

            if (!(conditions instanceof List)) {
                return false;
            }

            var conditionsList = (List<Object>) conditions;
            for (var conditionObj : conditionsList) {
                if (conditionObj instanceof Map) {
                    var condition = (Map<String, Object>) conditionObj;
                    var type = condition.get("type");
                    var conditionStatus = condition.get("status");

                    if ("Ready".equals(type) && "True".equals(conditionStatus)) {
                        return true;
                    }
                }
            }

            return false;
        } catch (Exception e) {
            runContext.logger().warn("Exception while checking resource readiness: {}", e.getMessage(), e);
            return false;
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The resource metadata after patching"
        )
        private final Metadata metadata;

        @Schema(
            title = "The resource status after patching",
            description = "Contains the current state of the resource including conditions, replicas, phase, etc. " +
                "Useful for validation and conditional logic in workflows."
        )
        private final ResourceStatus status;
    }
}
