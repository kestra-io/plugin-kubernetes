package io.kestra.plugin.kubernetes.models;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class SideCar {
    @Schema(
        title = "The image used for the file sidecar container."
    )
    @Builder.Default
    private Property<String> image = Property.ofValue("busybox");

    @Schema(
        title = "The resource requirements applied to the file sidecar container."
    )
    @JsonDeserialize(using = PropertyResourceRequirementsDeserializer.class)
    private Property<ResourceRequirements> resources;
}
