package io.kestra.plugin.kubernetes.models;

import java.util.Map;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import io.kestra.core.models.annotations.PluginProperty;

@Getter
@Builder
@Jacksonized
public class SideCar {
    @Schema(
        title = "Image for file sidecar",
        description = """
            Container image used by the init container (uploads input files) and the sidecar (downloads output files). Defaults to busybox.

            The image must provide, on its `PATH`: a POSIX shell (`sh`), `test`/`[`, and `sleep` — required by the polling script that waits for the transfer to complete before the container exits. `find` and `wc` are also used, on a best-effort basis, to verify that uploaded files were fully transferred; if they're missing, verification is skipped rather than failing the task.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> image = Property.ofValue("busybox");

    @Schema(
        title = "Configure sidecar resource requests/limits",
        description = "Optional Kubernetes resources block applied to the file transfer sidecar."
    )
    @PluginProperty(group = "advanced")
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
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> defaultSpec;
}
