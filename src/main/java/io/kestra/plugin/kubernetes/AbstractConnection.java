package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.models.Connection;
import io.kestra.plugin.kubernetes.services.ClientService;

import java.time.Duration;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractConnection extends Task {
    @Schema(
        title = "The connection parameters to the Kubernetes cluster",
        description = "If no connection is defined, we try to load the connection from the current context in the following order: \n" +
            "1. System properties\n" +
            "2. Environment variables\n" +
            "3. Kube config file\n" +
            "4. Service account token and a mounted CA certificate.\n" +
            "\n" +
            "You can pass a full configuration with all options if needed."
    )
    private Connection connection;

    @Schema(
        title = "The maximum duration to wait until the job and the pod is created.",
        description = "This timeout is the maximum time that Kubernetes scheduler will take to\n" +
            "* schedule the job\n" +
            "* pull the pod image\n" +
            "* and start the pod."
    )
    @NotNull
    @Builder.Default
    protected final Duration waitUntilRunning = Duration.ofMinutes(10);

    @Schema(
        title = "The maximum duration to wait for the job completion."
    )
    @NotNull
    @Builder.Default
    protected final Duration waitRunning = Duration.ofHours(1);

    protected ListOptions listOptions() {
        return new ListOptionsBuilder()
            .withTimeoutSeconds(this.waitRunning.toSeconds())
            .build();
    }
}
