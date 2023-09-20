package io.kestra.plugin.kubernetes;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Slugify;
import io.kestra.plugin.kubernetes.models.Connection;
import io.kestra.plugin.kubernetes.services.ClientService;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import javax.validation.constraints.NotNull;

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

    protected KubernetesClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        KubernetesClient client;

        if (this.connection != null) {
            client = ClientService.of(this.connection.toConfig(runContext));
        } else {
            client = ClientService.of();
        }

        return client;
    }

    private static String normalizedValue(String name) {
        if (name.length() > 63) {
            name = name.substring(0, 63);
        }

        name = StringUtils.stripEnd(name, "-");
        name = StringUtils.stripEnd(name, ".");
        name = StringUtils.stripEnd(name, "_");

        return name;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> metadata(RunContext runContext) {
        Map<String, String> flow = (Map<String, String>) runContext.getVariables().get("flow");
        Map<String, String> task = (Map<String, String>) runContext.getVariables().get("task");
        Map<String, String> execution = (Map<String, String>) runContext.getVariables().get("execution");
        Map<String, String> taskrun = (Map<String, String>) runContext.getVariables().get("taskrun");

        String name = Slugify.of(String.join(
            "-",
            taskrun.get("id"),
            flow.get("id"),
            task.get("id")
        ));

        return ImmutableMap.of(
            "name", normalizedValue(name),
            "labels", ImmutableMap.of(
                "flow.kestra.io/id", normalizedValue(flow.get("id")),
                "flow.kestra.io/namespace", normalizedValue(flow.get("namespace")),
                "task.kestra.io/id", normalizedValue(task.get("id")),
                "execution.kestra.io/id", normalizedValue(execution.get("id")),
                "taskrun.kestra.io/id", normalizedValue(taskrun.get("id"))
            )
        );
    }
}
