package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class GetTest {

    public static final String DEFAULT_NAMESPACE = "default";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGetOneResource() throws Exception {

        // Given
        var runContext = runContextFactory.of();

        var expectedDeploymentName = "my-deployment";

        var applyTask = Apply.builder()
            .id(Apply.class.getSimpleName())
            .type(Apply.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .spec(Property.ofValue(
                    String.format(
                        """
                            apiVersion: apps/v1
                            kind: Deployment
                            metadata:
                              name: %s
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
                            """,
                        expectedDeploymentName
                    )
                )
            )
            .build();

        applyTask.run(runContext);
        Thread.sleep(500);

        // When
        var getTask = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(KubernetesKind.DEPLOYMENTS))
            .resourcesNames(Property.ofValue(List.of(expectedDeploymentName)))
            .build();


        // Then
        var output = getTask.run(runContext);
        assertThat(output.getMetadata().size(), is(1));
        assertThat(output.getMetadata().getFirst().getName(), is(expectedDeploymentName));
    }

    @Test
    void shouldGetMultipleResources() throws Exception {

        // Given
        var runContext = runContextFactory.of();

        var deploymentsNames = new ArrayList<>();
        deploymentsNames.add("my-deployment");
        deploymentsNames.add("my-deployment-2");

        for (var deploymentName : deploymentsNames) {
            var applyTask = Apply.builder()
                .id(Apply.class.getSimpleName())
                .type(Apply.class.getName())
                .namespace(Property.ofValue(DEFAULT_NAMESPACE))
                .spec(Property.ofValue(
                    String.format(
                        """
                            apiVersion: apps/v1
                            kind: Deployment
                            metadata:
                              name: %s
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
                            """,
                        deploymentName
                    )
                ))
                .build();

            applyTask.run(runContext);
            Thread.sleep(500);
        }

        // When
        var getTask = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(KubernetesKind.DEPLOYMENTS))
            .build();


        // Then
        var output = getTask.run(runContext);
        assertThat(output.getMetadata().size(), is(2));
        assertThat(output.getMetadata().getFirst().getName(), is("my-deployment"));
        assertThat(output.getMetadata().getLast().getName(), is("my-deployment-2"));
    }
}
