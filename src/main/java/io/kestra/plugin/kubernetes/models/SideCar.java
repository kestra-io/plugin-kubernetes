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
        title = "Image for file sidecar",
        description = "Container image used by the init/sidecar that handles file transfer. Defaults to busybox."
    )
    @Builder.Default
    private Property<String> image = Property.ofValue("busybox");

    @Schema(
        title = "Sidecar resource requirements",
        description = "CPU/memory limits and requests applied to the file sidecar."
    )
    private Property<Map<String, Object>> resources;

    @Schema(
        title = "Default spec for file transfer containers",
        description = """
            Overrides containerDefaultSpec for the init and sidecar containers that move files. Accepts Pod container fields such as securityContext, volumeMounts, resources, and env; useful for hardening or adding mounts used only by file transfer helpers.

            Example:
            fileSidecar:
              defaultSpec:
                securityContext:
                  allowPrivilegeEscalation: false
                  readOnlyRootFilesystem: true
                volumeMounts:
                  - name: tmp
                    mountPath: /tmp
            """
    )
    private Property<Map<String, Object>> defaultSpec;
}
