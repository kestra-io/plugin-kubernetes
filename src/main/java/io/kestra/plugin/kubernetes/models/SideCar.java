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
        title = "Default container spec for the file sidecar and init containers",
        description = """
            Default container spec fields applied to the init and sidecar containers used for file transfer.
            When set, this overrides containerDefaultSpec for file transfer containers only.

            Supports the same fields as containerDefaultSpec:
            - securityContext: Security settings for file transfer containers
            - volumeMounts: Volume mounts to add to file transfer containers
            - resources: Resource limits/requests (note: also available as top-level 'resources' property)
            - env: Environment variables for file transfer containers

            Example configuration:
            ```yaml
            fileSidecar:
              defaultSpec:
                securityContext:
                  allowPrivilegeEscalation: false
                  capabilities:
                    drop:
                    - ALL
                  readOnlyRootFilesystem: true
                  seccompProfile:
                    type: RuntimeDefault
                volumeMounts:
                  - name: tmp
                    mountPath: /tmp
            ```
            """
    )
    private Property<Map<String, Object>> defaultSpec;
}
