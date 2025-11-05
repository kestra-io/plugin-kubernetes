package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Service for waiting on Kubernetes resource conditions.
 * Provides utilities to wait for resources to reach desired states.
 */
public abstract class ResourceWaitService {

    /**
     * Waits for a Kubernetes resource to become ready.
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
     * @param logger the logger for diagnostic messages
     * @return the resource once it becomes ready
     * @throws IllegalStateException if the resource does not become ready within the timeout
     * @throws KubernetesClientException if there's an error communicating with Kubernetes
     */
    public static GenericKubernetesResource waitForReady(
        KubernetesClient client,
        ResourceDefinitionContext resourceContext,
        String namespace,
        String resourceName,
        Duration timeout,
        Logger logger
    ) {
        logger.info("Waiting up to {} for resource '{}' in namespace '{}' to become ready",
            timeout, resourceName, namespace);

        var resourceClient = client.genericKubernetesResources(resourceContext)
            .inNamespace(namespace)
            .withName(resourceName);

        GenericKubernetesResource readyResource = null;

        try {
            readyResource = resourceClient.waitUntilCondition(
                resource -> resource != null && isResourceReady(resource, logger),
                timeout.toSeconds(),
                TimeUnit.SECONDS
            );
        } catch (KubernetesClientException e) {
            // Retry once in case of transient errors
            logger.debug("Error while waiting for resource, retrying: {}", e.getMessage());
            var currentResource = resourceClient.get();
            if (currentResource != null) {
                try {
                    readyResource = resourceClient.waitUntilCondition(
                        resource -> resource != null && isResourceReady(resource, logger),
                        timeout.toSeconds(),
                        TimeUnit.SECONDS
                    );
                } catch (KubernetesClientException retryException) {
                    throw new IllegalStateException(
                        "Failed to wait for resource '" + resourceName + "' in namespace '" + namespace +
                        "' to become ready: " + retryException.getMessage(),
                        retryException
                    );
                }
            } else {
                throw new IllegalStateException(
                    "Resource '" + resourceName + "' not found in namespace '" + namespace + "' during wait",
                    e
                );
            }
        }

        if (readyResource == null || !isResourceReady(readyResource, logger)) {
            throw new IllegalStateException(
                "Resource '" + resourceName + "' in namespace '" + namespace +
                "' did not become ready within timeout " + timeout
            );
        }

        logger.info("Resource '{}' in namespace '{}' is ready", resourceName, namespace);
        return readyResource;
    }

    /**
     * Checks if a resource has a Ready=True condition.
     * <p>
     * This method inspects the resource's status.conditions array for a condition
     * with type="Ready" and status="True". It works with GenericKubernetesResource
     * by accessing the raw additional properties map.
     *
     * @param resource the resource to check
     * @param logger the logger for diagnostic messages
     * @return true if the resource has Ready=True condition, false otherwise
     */
    @SuppressWarnings("unchecked")
    private static boolean isResourceReady(GenericKubernetesResource resource, Logger logger) {
        if (resource == null) {
            return false;
        }

        try {
            var additionalProperties = resource.getAdditionalProperties();
            if (additionalProperties == null) {
                logger.debug("Resource has no additional properties");
                return false;
            }

            var status = additionalProperties.get("status");
            if (!(status instanceof Map)) {
                logger.debug("Resource has no status map");
                return false;
            }

            var statusMap = (Map<String, Object>) status;
            var conditions = statusMap.get("conditions");

            if (!(conditions instanceof List)) {
                logger.debug("Resource has no conditions list");
                return false;
            }

            var conditionsList = (List<Object>) conditions;
            for (var conditionObj : conditionsList) {
                if (conditionObj instanceof Map) {
                    var condition = (Map<String, Object>) conditionObj;
                    var type = condition.get("type");
                    var conditionStatus = condition.get("status");

                    if ("Ready".equals(type) && "True".equals(conditionStatus)) {
                        logger.debug("Resource has Ready=True condition");
                        return true;
                    }
                }
            }

            logger.debug("Resource does not have Ready=True condition");
            return false;
        } catch (Exception e) {
            logger.warn("Exception while checking resource readiness: {}", e.getMessage(), e);
            return false;
        }
    }
}
