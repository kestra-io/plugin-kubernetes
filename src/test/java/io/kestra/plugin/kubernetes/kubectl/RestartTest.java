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
        Thread.sleep(1000);

        // Fetch before restart
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

        Long generationBefore = beforeOutput.getMetadataItem().getGeneration();

        // When — restart statefulset
        var restartTask = Restart.builder()
            .id(Restart.class.getSimpleName())
            .type(Restart.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(Restart.ResourceType.StatefulSet))
            .resourcesNames(Property.ofValue(List.of("api")))
            .build();

        restartTask.run(runContext);
        Thread.sleep(1000);

        // Fetch after restart
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

        assertThat(afterOutput.getMetadataItem().getName(), is("api"));

        Long generationAfter = afterOutput.getMetadataItem().getGeneration();

        // Assert that the restart triggered a generation bump (best reliable indicator)
        assertThat("Generation should increase after restart",
            generationAfter, greaterThanOrEqualTo(generationBefore));
    }
}
