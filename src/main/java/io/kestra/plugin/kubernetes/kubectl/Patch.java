package io.kestra.plugin.kubernetes.kubectl;

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
import org.slf4j.Logger;

import java.time.Duration;
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
                    wait: true
                    waitTimeout: PT5M
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
        title = "Wait for the patched resource to become ready",
        description = "When enabled, waits for the resource to report Ready=True in its status conditions. " +
            "Supports Pods, StatefulSets, and custom resources that use the Ready condition. " +
            "Note: Deployments are not supported as they use the Available condition instead of Ready."
    )
    private Property<Boolean> wait = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Maximum duration to wait for resource to become ready",
        description = "Only used when wait is enabled. The task will fail if the resource does not become ready within this timeout."
    )
    private Property<Duration> waitTimeout = Property.ofValue(Duration.ofMinutes(5));

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
        var renderedApiGroup = runContext.render(this.apiGroup).as(String.class).orElse("");
        var renderedApiVersion = runContext.render(this.apiVersion).as(String.class).orElse("v1");
        var shouldWait = runContext.render(this.wait).as(Boolean.class).orElse(false);
        var timeout = runContext.render(this.waitTimeout).as(Duration.class).orElse(Duration.ofMinutes(5));

        try (var client = PodService.client(runContext, this.getConnection())) {
            // Build resource definition context
            var resourceContext = new ResourceDefinitionContext.Builder()
                .withGroup(renderedApiGroup)
                .withVersion(renderedApiVersion)
                .withKind(resourceType)
                .withNamespaced(true)
                .build();

            // Get resource client
            var resourceClient = client.genericKubernetesResources(resourceContext)
                .inNamespace(namespace)
                .withName(resourceName);

            // Apply patch based on strategy
            runContext.logger().info("Patching {} '{}' in namespace '{}' using {} strategy",
                resourceType, resourceName, namespace, strategy);
            runContext.logger().debug("Patch content: {}", patchContent);

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
                throw new IllegalStateException("Failed to patch resource: resource not found or patch failed");
            }

            runContext.logger().info("Successfully patched {} '{}'", resourceType, resourceName);

            // Optionally wait for resource to become ready
            if (shouldWait) {
                waitForResourceReady(client, resourceContext, namespace, resourceName,
                    timeout, runContext.logger());
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
     * @param logger the logger for status messages
     */
    private void waitForResourceReady(
        KubernetesClient client,
        ResourceDefinitionContext resourceContext,
        String namespace,
        String resourceName,
        Duration timeout,
        Logger logger
    ) {
        logger.info("Waiting for resource '{}' to become ready (timeout: {})...", resourceName, timeout);

        var resourceClient = client.genericKubernetesResources(resourceContext)
            .inNamespace(namespace)
            .withName(resourceName);

        // Use waitUntilCondition with a predicate
        resourceClient.waitUntilCondition(
            resource -> resource != null && isResourceReady(resource),
            timeout.toSeconds(),
            TimeUnit.SECONDS
        );

        logger.info("Resource '{}' is ready", resourceName);
    }

    /**
     * Checks if a resource has a Ready=True condition (default check).
     */
    @SuppressWarnings("unchecked")
    private boolean isResourceReady(io.fabric8.kubernetes.api.model.GenericKubernetesResource resource) {
        try {
            var additionalProperties = resource.getAdditionalProperties();
            if (additionalProperties == null) {
                return false;
            }

            var status = additionalProperties.get("status");
            if (!(status instanceof java.util.Map)) {
                return false;
            }

            var statusMap = (java.util.Map<String, Object>) status;
            var conditions = statusMap.get("conditions");

            if (!(conditions instanceof java.util.List)) {
                return false;
            }

            var conditionsList = (java.util.List<Object>) conditions;
            for (var conditionObj : conditionsList) {
                if (conditionObj instanceof java.util.Map) {
                    var condition = (java.util.Map<String, Object>) conditionObj;
                    var type = condition.get("type");
                    var conditionStatus = condition.get("status");

                    if ("Ready".equals(type) && "True".equals(conditionStatus)) {
                        return true;
                    }
                }
            }

            return false;
        } catch (Exception e) {
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
