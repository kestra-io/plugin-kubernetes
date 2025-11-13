package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
class ApplyTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var task = Apply.builder()
            .id(Apply.class.getSimpleName())
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                """
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
                    """
            ))
            .build();

        var runOutput = task.run(runContext);

        Thread.sleep(500);

        assertThat(runOutput.getMetadata().getFirst().getName(), containsString("my-deployment"));
    }
}
