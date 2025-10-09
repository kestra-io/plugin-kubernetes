package io.kestra.plugin.kubernetes.models;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Builder
@Getter
public class ResourceStatus {
    @Schema(
        title = "The status of the Kubernetes resource",
        description = "Contains the current state of the resource as a generic map structure"
    )
    private final Map<String, Object> status;

    /**
     * Extracts the status from a GenericKubernetesResource.
     * The status field will be null if the resource doesn't contain status information.
     *
     * @param resource The Kubernetes resource to extract status from (must not be null)
     * @return ResourceStatus with status map (may be null if no status exists)
     */
    @SuppressWarnings("unchecked")
    public static ResourceStatus from(GenericKubernetesResource resource) {
        Map<String, Object> additionalProperties = resource.getAdditionalProperties();
        if (additionalProperties == null) {
            return ResourceStatus.builder().status(null).build();
        }

        Object statusObj = additionalProperties.get("status");
        Map<String, Object> statusMap = null;

        if (statusObj instanceof Map) {
            statusMap = (Map<String, Object>) statusObj;
        }

        return ResourceStatus.builder()
            .status(statusMap)
            .build();
    }
}
