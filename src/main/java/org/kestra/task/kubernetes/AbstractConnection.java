package org.kestra.task.kubernetes;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.Slugify;
import org.kestra.task.kubernetes.models.Connection;
import org.kestra.task.kubernetes.services.ClientService;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractConnection extends Task {
    @InputProperty(
        description = "The connection parameters to Kubernetes cluster",
        body = {
            "If no connection is defined, we try to load connection from current context in order below: ",
            "1. System properties",
            "2. Environment variables",
            "3. Kube config file",
            "4. Service account token & mounted CA certificate",
            "",
            "You can pass a full configuration with all option if needed"
        }
    )
    private Connection connection;

    protected DefaultKubernetesClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        DefaultKubernetesClient client;

        if (this.connection != null) {
            client = ClientService.of(this.connection.toConfig(runContext));
        } else {
            client = ClientService.of();
        }

        return client;
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> metadata(RunContext runContext) {
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

        if (name.length() > 63) {
            name = name.substring(0, 63);
        }

        return ImmutableMap.of(
            "name", name,
            "labels", ImmutableMap.of(
                "flow.kestra.io/id", flow.get("id"),
                "flow.kestra.io/namespace", flow.get("namespace"),
                "task.kestra.io/id", task.get("id"),
                "execution.kestra.io/id", execution.get("id"),
                "taskrun.kestra.io/id", taskrun.get("id")
            )
        );
    }
}
