package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kubernetes.services.ClientService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
            .resourceType(Property.ofValue("deployments"))
            .resourcesNames(Property.ofValue(List.of(expectedDeploymentName)))
            .apiGroup(Property.ofValue("apps"))
            .apiVersion(Property.ofValue("v1"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();


        // Then
        var output = getTask.run(runContext);
        assertThat(output.getMetadataItem().getName(), is(expectedDeploymentName));
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
            .resourceType(Property.ofValue("deployments"))
            .apiGroup(Property.ofValue("apps"))
            .apiVersion(Property.ofValue("v1"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();


        // Then
        var output = getTask.run(runContext);
        assertThat(output.getMetadataItems().size(), is(2));
        assertThat(output.getMetadataItems().getFirst().getName(), is("my-deployment"));
        assertThat(output.getMetadataItems().getLast().getName(), is("my-deployment-2"));
    }

    @Test
    void shouldGetCustomResource() throws Exception {
        // Given
        var runContext = runContextFactory.of();

        var apiGroup = "stable.example.com";
        var apiVersion = "v1";
        var shirtKind = "Shirt";
        var shirtsPlural = "shirts";
        var crdName = "my-blue-shirt";

        // 1. Creates the CustomResourceDefinition
        // 2. Creates the CustomResource (Shirt) itself
        var applyCrd = Apply.builder()
            .id("ApplyCRD-" + IdUtils.create())
            .type(Apply.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .spec(Property.ofValue(String.format("""
                    apiVersion: apiextensions.k8s.io/v1
                    kind: CustomResourceDefinition
                    metadata:
                      name: shirts.stable.example.com
                    spec:
                      group: %s
                      scope: Namespaced
                      names:
                        plural: shirts
                        singular: shirt
                        kind: %s
                      versions:
                      - name: %s
                        served: true
                        storage: true
                        schema:
                          openAPIV3Schema:
                            type: object
                            properties:
                              spec:
                                type: object
                                properties:
                                  color: { type: string }
                                  size: { type: string }
                    """, apiGroup, shirtKind, apiVersion)))
            .build();

        applyCrd.run(runContext);

        try(var client = ClientService.of()) {
            client.apiextensions().v1().customResourceDefinitions()
                .withName("shirts.stable.example.com")
                .waitUntilCondition(
                    crd -> crd != null
                        && crd.getStatus() != null
                        && crd.getStatus().getConditions() != null
                        && crd.getStatus().getConditions().stream()
                        .anyMatch(c -> "Established".equals(c.getType()) && "True".equals(c.getStatus())),
                    30, java.util.concurrent.TimeUnit.SECONDS);
        }

        var applyCr = Apply.builder()
            .id("ApplyCR-" + IdUtils.create())
            .type(Apply.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .spec(Property.ofValue(String.format("""
                    apiVersion: "%s/%s"
                    kind: Shirt
                    metadata:
                      name: %s
                      namespace: default
                    spec:
                      color: blue
                      size: L
                    """, apiGroup, apiVersion, crdName)))
            .build();

        applyCr.run(runContext);


        // When
        var getTask = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue(shirtsPlural))
            .apiGroup(Property.ofValue(apiGroup))
            .apiVersion(Property.ofValue(apiVersion))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();


        // Then
        var output = getTask.run(runContext);
        assertThat(output.getMetadataItem().getName(), is(crdName));
    }
}
