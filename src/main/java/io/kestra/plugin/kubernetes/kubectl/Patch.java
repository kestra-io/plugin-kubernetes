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
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
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
            title = "Patch deployment memory limits and wait for rollout to complete.",
            full = true,
            code = """
                id: patch_deployment_with_wait
                namespace: company.team

                tasks:
                  - id: patch
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: deployment
                    resourceName: my-api
                    wait: true
                    waitTimeout: PT10M
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
        ),
        @Example(
            title = "Patch custom resource and wait for custom condition.",
            full = true,
            code = """
                id: patch_custom_resource_with_wait
                namespace: company.team

                tasks:
                  - id: patch
                    type: io.kestra.plugin.kubernetes.kubectl.Patch
                    namespace: production
                    resourceType: myresources
                    resourceName: my-custom-resource
                    apiGroup: example.com
                    apiVersion: v1
                    wait: true
                    waitTimeout: PT5M
                    waitCondition: ".status.phase == Processed"
                    patch: |
                      {
                        "spec": {
                          "replicas": 5
                        }
                      }
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

    @Builder.Default
    @Schema(
        title = "Wait for the patched resource to reach a specific condition",
        description = "When enabled, the task will wait for the resource to meet the specified condition after patching. " +
            "Works with any resource type including custom resources. " +
            "Use with waitCondition to specify what to wait for."
    )
    private Property<Boolean> wait = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Maximum duration to wait for the condition",
        description = "Only used when wait is enabled. The task will fail if the resource does not meet the condition within this timeout."
    )
    private Property<Duration> waitTimeout = Property.ofValue(Duration.ofMinutes(5));

    @Schema(
        title = "The condition to wait for",
        description = "A JsonPath expression to evaluate on the resource status. " +
            "The wait succeeds when the expression evaluates to true. " +
            "Default: '.status.conditions[?(@.type==\"Ready\")].status' contains 'True' " +
            "Examples:\n" +
            "- For Deployments: '.status.conditions[?(@.type==\"Progressing\" && @.status==\"True\")]'\n" +
            "- For custom field: '.status.phase == \"Running\"'\n" +
            "- For replica count: '.status.readyReplicas == .spec.replicas'\n" +
            "Leave empty to use the default ready condition."
    )
    private Property<String> waitCondition;

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
        var shouldWait = runContext.render(this.wait).as(Boolean.class).orElse(false);
        var timeout = runContext.render(this.waitTimeout).as(Duration.class).orElse(Duration.ofMinutes(5));

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

            // Optionally wait for resource condition
            if (shouldWait) {
                var condition = runContext.render(this.waitCondition).as(String.class).orElse(null);
                waitForResourceCondition(client, resourceContext, namespace, resourceName,
                    condition, timeout, runContext.logger());
            }

            return Output.builder()
                .metadata(Metadata.from(patchedResource.getMetadata()))
                .build();
        }
    }

    /**
     * Waits for a Kubernetes resource to meet a specific condition after patching.
     * <p>
     * This method works with any resource type including custom resources by using
     * a generic condition predicate. If no custom condition is specified, it defaults
     * to checking for a Ready=True condition in the resource status.
     *
     * @param client the Kubernetes client
     * @param resourceContext the resource definition context
     * @param namespace the namespace of the resource
     * @param resourceName the name of the resource
     * @param customCondition optional custom condition expression (null for default)
     * @param timeout the maximum duration to wait
     * @param logger the logger for status messages
     */
    private void waitForResourceCondition(
        KubernetesClient client,
        ResourceDefinitionContext resourceContext,
        String namespace,
        String resourceName,
        String customCondition,
        Duration timeout,
        Logger logger
    ) {
        logger.info("Waiting for resource '{}' to meet condition (timeout: {})...", resourceName, timeout);

        if (customCondition != null && !customCondition.isBlank()) {
            logger.debug("Using custom wait condition: {}", customCondition);
        } else {
            logger.debug("Using default wait condition: Ready=True");
        }

        var resourceClient = client.genericKubernetesResources(resourceContext)
            .inNamespace(namespace)
            .withName(resourceName);

        // Use waitUntilCondition with a predicate
        resourceClient.waitUntilCondition(
            resource -> {
                if (resource == null) {
                    return false;
                }

                // If custom condition specified, evaluate it
                if (customCondition != null && !customCondition.isBlank()) {
                    return evaluateCustomCondition(resource, customCondition, logger);
                }

                // Default: check for Ready=True condition
                return isResourceReady(resource);
            },
            timeout.toSeconds(),
            TimeUnit.SECONDS
        );

        logger.info("Resource '{}' condition met successfully", resourceName);
    }

    /**
     * Evaluates a custom condition on a resource.
     * <p>
     * Supports simple field path expressions like:
     * - ".status.phase" == "Running"
     * - ".status.ready" == true
     * - ".status.readyReplicas" == ".spec.replicas"
     *
     * @param resource the resource to evaluate
     * @param condition the condition expression
     * @param logger the logger for debug messages
     * @return true if condition is met, false otherwise
     */
    private boolean evaluateCustomCondition(
        io.fabric8.kubernetes.api.model.GenericKubernetesResource resource,
        String condition,
        Logger logger
    ) {
        try {
            // Simple evaluation: check if a specific field path equals a value
            // For now, support basic field checks in status
            // Example: ".status.phase == Running"
            var additionalProperties = resource.getAdditionalProperties();
            if (additionalProperties == null) {
                return false;
            }

            // Parse simple conditions (this is a basic implementation)
            // For production, you'd want a proper expression evaluator
            if (condition.contains("==")) {
                var parts = condition.split("==");
                if (parts.length == 2) {
                    var fieldPath = parts[0].trim();
                    var expectedValue = parts[1].trim().replace("\"", "");

                    var actualValue = getFieldValue(additionalProperties, fieldPath);
                    boolean result = expectedValue.equals(String.valueOf(actualValue));

                    logger.debug("Condition check: {} == {} ? {} (actual: {})",
                        fieldPath, expectedValue, result, actualValue);

                    return result;
                }
            }

            logger.warn("Unable to parse condition: {}", condition);
            return false;
        } catch (Exception e) {
            logger.warn("Error evaluating custom condition '{}': {}", condition, e.getMessage());
            return false;
        }
    }

    /**
     * Extracts a field value from a resource using a simple dot-notation path.
     * Example: ".status.phase" returns the value of resource.status.phase
     */
    private Object getFieldValue(java.util.Map<String, Object> map, String fieldPath) {
        var path = fieldPath.trim();
        if (path.startsWith(".")) {
            path = path.substring(1);
        }

        var parts = path.split("\\.");
        Object current = map;

        for (var part : parts) {
            if (current instanceof java.util.Map) {
                current = ((java.util.Map<?, ?>) current).get(part);
                if (current == null) {
                    return null;
                }
            } else {
                return null;
            }
        }

        return current;
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
