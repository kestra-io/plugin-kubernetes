package io.kestra.plugin.kubernetes.models;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;

@Getter
@Builder
@Jacksonized
public class SideCar {
    @Schema(
        title = "The image used for the file sidecar container"
    )
    @Builder.Default
    private Property<String> image = Property.ofValue("busybox");

    @Schema(
        title = "The resource requirements applied to the file sidecar container"
    )
    private Property<Map<String, Object>> resources;

    @Schema(
        title = "The security context applied to the file sidecar and init containers",
        description = """
            Kubernetes security context configuration for the init and sidecar containers used for file transfer.
            This allows setting security policies required by restrictive environments like GovCloud.
            Example configuration:
            ```yaml
            securityContext:
              allowPrivilegeEscalation: false
              capabilities:
                drop:
                - ALL
              readOnlyRootFilesystem: true
              seccompProfile:
                type: RuntimeDefault
            ```
            """
    )
    private Property<Map<String, Object>> securityContext;
}
