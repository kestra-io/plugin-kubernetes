package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.models.Connection;

import java.time.Duration;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractConnection extends Task {
    @Schema(
        title = "Kubernetes connection",
        description = "Connection settings for the cluster. If omitted, the client resolves credentials in order: system properties, environment variables, kubeconfig, then in-cluster service account."
    )
    private Connection connection;

    @Schema(
        title = "Wait for pod to reach Running",
        description = "Maximum time to reach Running (defaults to PT10M). Covers scheduling, image pulls, and startup. Used by PodCreate."
    )
    @NotNull
    @Builder.Default
    protected final Property<Duration> waitUntilRunning = Property.ofValue(Duration.ofMinutes(10));

    @Schema(
        title = "Wait for pod completion",
        description = "Maximum run time after reaching Running (defaults to PT1H). PodCreate fails and deletes the pod when exceeded."
    )
    @NotNull
    @Builder.Default
    protected final Property<Duration> waitRunning = Property.ofValue(Duration.ofHours(1));

    protected ListOptions listOptions(RunContext runContext) throws IllegalVariableEvaluationException {
        return new ListOptionsBuilder()
            .withTimeoutSeconds(runContext.render(this.waitRunning).as(Duration.class).orElseThrow().toSeconds())
            .build();
    }
}
