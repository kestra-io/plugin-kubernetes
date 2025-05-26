package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class DeleteTest {

    public static final String DEFAULT_NAMESPACE = "default";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldDeleteOneDeployment() throws Exception {
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
            .resourceName(Property.ofValue("my-deployment"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();


        // Then
        var getTaskOutput = getTask.run(runContext);
        assertThat(getTaskOutput.getMetadata().size(), is(1));

        // When
        var deleteTask = Delete.builder()
            .id(Delete.class.getSimpleName())
            .type(Delete.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(KubernetesKind.DEPLOYMENTS))
            .resourcesNames(Property.ofValue(List.of("my-deployment")))
            .build();

        // Then
        deleteTask.run(runContext);
        Thread.sleep(500);

        // When
        getTaskOutput = getTask.run(runContext);
        assertThat(getTaskOutput.getMetadata().size(), is(0));
    }

    @Test
    void shouldDeleteMultipleDeployment() throws Exception {
        // Given
        var runContext = runContextFactory.of();

        List<String> deploymentsNames = new ArrayList<>();
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
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();


        // Then
        var getTaskOutput = getTask.run(runContext);
        assertThat(getTaskOutput.getMetadata().size(), is(2));

        // When
        var deleteTask = Delete.builder()
            .id(Delete.class.getSimpleName())
            .type(Delete.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(KubernetesKind.DEPLOYMENTS))
            .resourcesNames(Property.ofValue(deploymentsNames))
            .build();

        // Then
        deleteTask.run(runContext);
        Thread.sleep(500);

        // When
        getTaskOutput = getTask.run(runContext);
        assertThat(getTaskOutput.getMetadata().size(), is(0));
    }
}
