package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class RestartTest {

    public static final String DEFAULT_NAMESPACE = "default";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRestartStatefulSet() throws Exception {
        // Given
        var runContext = runContextFactory.of();

        var applyTask = Apply.builder()
            .id(Apply.class.getSimpleName())
            .type(Apply.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .spec(Property.ofValue(
                """
                    apiVersion: apps/v1
                    kind: StatefulSet
                    metadata:
                      name: api
                      labels:
                        app: api
                    spec:
                      serviceName: "api"
                      replicas: 3
                      selector:
                        matchLabels:
                          app: api
                      template:
                        metadata:
                          labels:
                            app: api
                        spec:
                          containers:
                          - name: api
                            image: nginx:latest
                            ports:
                            - containerPort: 80
                    """
            ))
            .build();

        applyTask.run(runContext);
        Thread.sleep(1000); // Give Kubernetes some time to create resources

        // When - Fetch the resource before restart
        var getBeforeRestart = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue("statefulsets"))
            .resourcesNames(Property.ofValue(List.of("api")))
            .apiGroup(Property.ofValue("apps"))
            .apiVersion(Property.ofValue("v1"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var beforeOutput = getBeforeRestart.run(runContext);
        assertThat(beforeOutput.getMetadataItem().getName(), is("api"));

        // When - Execute Restart task (equivalent to `kubectl rollout restart statefulset api`)
        var restartTask = Restart.builder()
            .id(Restart.class.getSimpleName())
            .type(Restart.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(Restart.ResourceType.StatefulSet))
            .resourcesNames(Property.ofValue(List.of("api")))
            .apiGroup(Property.ofValue("apps"))
            .apiVersion(Property.ofValue("v1"))
            .build();

        restartTask.run(runContext);
        Thread.sleep(1000); // Wait for the annotation update to propagate

        // Then - Fetch the resource again and check the annotation
        var getAfterRestart = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue("statefulsets"))
            .resourcesNames(Property.ofValue(List.of("api")))
            .apiGroup(Property.ofValue("apps"))
            .apiVersion(Property.ofValue("v1"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var afterOutput = getAfterRestart.run(runContext);
        var annotations = afterOutput.getMetadataItem().getAnnotations();

        // Assert the restart annotation exists
        assertThat("Restart annotation should be present",
            annotations, hasKey("kubectl.kubernetes.io/restartedAt"));

        // Assert the annotation value is a valid timestamp
        assertThat("Restart timestamp should not be empty",
            annotations.get("kubectl.kubernetes.io/restartedAt"), not(emptyOrNullString()));
    }
}
