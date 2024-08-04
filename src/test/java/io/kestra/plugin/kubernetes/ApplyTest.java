package io.kestra.plugin.kubernetes;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kubernetes.kubectl.Apply;
import io.kestra.plugin.kubernetes.models.Connection;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
@Slf4j
class ApplyTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        Apply task = Apply.builder()
            .id(Apply.class.getSimpleName())
            .type(Apply.class.getName())
            .connection(getConnection())
            .namespace("default")
            .spec(getSpec())
            .build();

        Apply.Output runOutput = task.run(runContext);

        Thread.sleep(500);

        assertThat(runOutput.getMetadata().get(0).getName(), containsString("my-deployment"));
    }

    private String getSpec() {
        return """
               apiVersion: apps/v1
               kind: Deployment
               metadata:
                 name: my-deployment
                 labels:
                   app: myapp
               spec:
                 replicas: 3
                 selector:
                   matchLabels:
                     app: myapp
                 template:
                   metadata:
                     labels:
                       app: myapp
                   spec:
                     containers:
                     - name: mycontainer
                       image: nginx:latest
                       ports:
                       - containerPort: 80
               """;
    }

    private Connection getConnection() {
        return Connection.builder().build();
    }

}
