package io.kestra.plugin.kubernetes.kubectl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kubernetes.services.ClientService;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@KestraTest
@Slf4j
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

        // Verify status is populated
        assertThat(output.getStatusItem(), org.hamcrest.Matchers.notNullValue());
        assertThat(output.getStatusItem().getStatus(), org.hamcrest.Matchers.notNullValue());

        // Verify Deployment-specific status fields
        var status = output.getStatusItem().getStatus();
        assertThat(status.containsKey("replicas"), is(true));
        assertThat(status.get("replicas"), is(3)); // matches spec.replicas
        assertThat(status.containsKey("conditions"), is(true));
        assertThat(status.get("conditions"), org.hamcrest.Matchers.instanceOf(java.util.List.class));
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

        // Verify status list is populated
        assertThat(output.getStatusItems(), org.hamcrest.Matchers.notNullValue());
        assertThat(output.getStatusItems().size(), is(2));

        // Verify first deployment has valid status
        var firstStatus = output.getStatusItems().getFirst().getStatus();
        assertThat(firstStatus, org.hamcrest.Matchers.notNullValue());
        assertThat(firstStatus.containsKey("replicas"), is(true));
        assertThat(firstStatus.get("replicas"), is(3));

        // Verify second deployment has valid status
        var lastStatus = output.getStatusItems().getLast().getStatus();
        assertThat(lastStatus, org.hamcrest.Matchers.notNullValue());
        assertThat(lastStatus.containsKey("replicas"), is(true));
        assertThat(lastStatus.get("replicas"), is(3));
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
                        additionalPrinterColumns:
                          - jsonPath: .spec.color
                            name: Color
                            type: string
                          - jsonPath: .spec.size
                            name: Size
                            type: string
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

        // Verify status field exists (custom resources may have null/empty status)
        assertThat(output.getStatusItem(), org.hamcrest.Matchers.notNullValue());
        // Status map may be null for custom resources without status subresource
        // but the ResourceStatus wrapper should always be present
    }

    @Test
    void shouldGetResourceWithWaitUntilReadyZero() throws Exception {
        // Test that waitUntilReady with PT0S returns immediately without waiting for pod to be ready
        var runContext = runContextFactory.of();

        var podName = "test-pod-get-no-wait-" + System.currentTimeMillis();

        // Create a pod with readiness probe delay
        var applyTask = Apply.builder()
            .id("apply-pod")
            .type(Apply.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .spec(Property.ofValue(String.format(
                """
                    apiVersion: v1
                    kind: Pod
                    metadata:
                      name: %s
                    spec:
                      containers:
                      - name: nginx
                        image: nginx:latest
                        readinessProbe:
                          httpGet:
                            path: /
                            port: 80
                          initialDelaySeconds: 10
                          periodSeconds: 5
                    """, podName
            )))
            .build();

        applyTask.run(runContext);
        Thread.sleep(500);

        // Get the pod without waiting
        var getTask = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue("pods"))
            .resourcesNames(Property.ofValue(List.of(podName)))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .waitUntilReady(Property.ofValue(Duration.ZERO))  // Don't wait
            .build();

        var output = getTask.run(runContext);
        assertThat(output.getMetadataItem(), notNullValue());
        assertThat(output.getMetadataItem().getName(), is(podName));

        // Verify the pod is NOT ready yet (since we didn't wait and it has 10s initialDelay)
        var status = output.getStatusItem().getStatus();
        assertThat(status, notNullValue());
        assertThat(status.containsKey("conditions"), is(true));

        // Pod should NOT be ready yet
        var conditions = (java.util.List<?>) status.get("conditions");
        boolean isReady = conditions.stream()
            .filter(c -> c instanceof java.util.Map)
            .map(c -> (java.util.Map<?, ?>) c)
            .anyMatch(c -> "Ready".equals(c.get("type")) && "True".equals(c.get("status")));

        assertThat("Pod should NOT be ready immediately after get with PT0S", isReady, is(false));
        log.info("Verified pod {} is NOT ready (as expected with PT0S)", podName);
    }

    @Test
    void shouldGetResourceWithWaitUntilReady() throws Exception {
        // Test that waitUntilReady waits for a Pod to become ready
        var runContext = runContextFactory.of();

        var podName = "test-pod-get-with-wait-" + System.currentTimeMillis();

        // Create a pod with readiness probe
        var applyTask = Apply.builder()
            .id("apply-pod")
            .type(Apply.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .spec(Property.ofValue(String.format(
                """
                    apiVersion: v1
                    kind: Pod
                    metadata:
                      name: %s
                    spec:
                      containers:
                      - name: nginx
                        image: nginx:latest
                        ports:
                        - containerPort: 80
                        readinessProbe:
                          httpGet:
                            path: /
                            port: 80
                          initialDelaySeconds: 5
                          periodSeconds: 3
                    """, podName
            )))
            .build();

        applyTask.run(runContext);
        Thread.sleep(500);

        // Get the pod and wait for it to become ready
        var getTask = Get.builder()
            .id(Get.class.getSimpleName())
            .type(Get.class.getName())
            .namespace(Property.ofValue(DEFAULT_NAMESPACE))
            .resourceType(Property.ofValue("pods"))
            .resourcesNames(Property.ofValue(List.of(podName)))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .waitUntilReady(Property.ofValue(Duration.ofMinutes(2)))
            .build();

        // This should wait for the Pod to become Ready before returning
        var output = getTask.run(runContext);

        assertThat(output.getMetadataItem(), notNullValue());
        assertThat(output.getMetadataItem().getName(), is(podName));

        // Verify the pod IS ready (since we waited)
        var status = output.getStatusItem().getStatus();
        assertThat(status, notNullValue());
        assertThat(status.containsKey("conditions"), is(true));

        // Pod SHOULD be ready now
        var conditions = (java.util.List<?>) status.get("conditions");
        boolean isReady = conditions.stream()
            .filter(c -> c instanceof java.util.Map)
            .map(c -> (java.util.Map<?, ?>) c)
            .anyMatch(c -> "Ready".equals(c.get("type")) && "True".equals(c.get("status")));

        assertThat("Pod should be ready after get with waitUntilReady", isReady, is(true));
        log.info("Verified pod {} IS ready (as expected after waiting)", podName);
    }
}
