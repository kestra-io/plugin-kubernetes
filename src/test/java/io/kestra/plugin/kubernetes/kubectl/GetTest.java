package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class GetTest {

    public static final String DEFAULT_NAMESPACE = "default";
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {

        // Given
        var runContext = runContextFactory.of();

        var applyTask = Apply.builder()
            .id(Apply.class.getSimpleName())
            .type(Apply.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
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

        applyTask.run(runContext);
        Thread.sleep(500);

        // When
        var getTask = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(KubernetesKind.DEPLOYMENTS))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();


        // Then
        var output = getTask.run(runContext);
        assertThat(output.getMetadata().size(), is(1));
    }
}
