package io.kestra.plugin.kubernetes.runner;

import io.kestra.core.models.annotations.PluginProperty;
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
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String image = "busybox";
}
