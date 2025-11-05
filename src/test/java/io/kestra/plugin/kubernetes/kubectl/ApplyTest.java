package io.kestra.plugin.kubernetes.kubectl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
@Slf4j
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

    @Test
    void runWithWaitUntilReadyZero() throws Exception {
        // Test that waitUntilReady with PT0S returns immediately without waiting for pod to be ready
        var runContext = runContextFactory.of();

        var podName = "test-pod-no-wait-" + System.currentTimeMillis();

        var task = Apply.builder()
            .id(Apply.class.getSimpleName())
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .waitUntilReady(Property.ofValue(Duration.ZERO))  // Don't wait
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
                    """, podName)
            ))
            .build();

        var runOutput = task.run(runContext);

        assertThat(runOutput.getMetadata(), notNullValue());
        assertThat(runOutput.getMetadata().getFirst().getName(), containsString(podName));

        // Verify the pod is NOT ready yet (since we didn't wait and it has 10s initialDelay)
        // Get the pod immediately to check its status
        var getTask = Get.builder()
            .id("check-status")
            .type(Get.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("pods"))
            .resourcesNames(Property.ofValue(List.of(podName)))
            .fetchType(Property.ofValue(io.kestra.core.models.tasks.common.FetchType.FETCH_ONE))
            .build();

        var getOutput = getTask.run(runContext);
        var status = getOutput.getStatusItem().getStatus();

        assertThat(status, notNullValue());
        assertThat(status.containsKey("conditions"), is(true));

        // Pod should NOT be ready yet
        var conditions = (java.util.List<?>) status.get("conditions");
        boolean isReady = conditions.stream()
            .filter(c -> c instanceof java.util.Map)
            .map(c -> (java.util.Map<?, ?>) c)
            .anyMatch(c -> "Ready".equals(c.get("type")) && "True".equals(c.get("status")));

        assertThat("Pod should NOT be ready immediately after apply with PT0S", isReady, is(false));
        log.info("Verified pod {} is NOT ready (as expected with PT0S)", podName);
    }

    @Test
    void runWithWaitUntilReady() throws Exception {
        // Test that waitUntilReady actually waits for a Pod to become ready
        var runContext = runContextFactory.of();

        var podName = "test-pod-with-wait-" + System.currentTimeMillis();

        var task = Apply.builder()
            .id(Apply.class.getSimpleName())
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .waitUntilReady(Property.ofValue(Duration.ofMinutes(2)))
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
                    """, podName)
            ))
            .build();

        // This should wait for the Pod to become Ready before returning
        var runOutput = task.run(runContext);

        assertThat(runOutput.getMetadata(), notNullValue());
        assertThat(runOutput.getMetadata().getFirst().getName(), containsString(podName));

        // Verify the pod IS ready by fetching immediately after apply returns
        // Since Apply waited, the pod should be ready immediately
        var getTask = Get.builder()
            .id("check-status")
            .type(Get.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("pods"))
            .resourcesNames(Property.ofValue(List.of(podName)))
            .fetchType(Property.ofValue(io.kestra.core.models.tasks.common.FetchType.FETCH_ONE))
            .build();

        var getOutput = getTask.run(runContext);
        var status = getOutput.getStatusItem().getStatus();

        assertThat(status, notNullValue());
        assertThat(status.containsKey("conditions"), is(true));

        // Pod SHOULD be ready now (proving Apply waited correctly)
        var conditions = (java.util.List<?>) status.get("conditions");
        boolean isReady = conditions.stream()
            .filter(c -> c instanceof java.util.Map)
            .map(c -> (java.util.Map<?, ?>) c)
            .anyMatch(c -> "Ready".equals(c.get("type")) && "True".equals(c.get("status")));

        assertThat("Pod should be ready after apply with waitUntilReady", isReady, is(true));

        // Verify additional status details to ensure it's truly running
        assertThat(status.get("phase"), is("Running"));
        assertThat(status.containsKey("containerStatuses"), is(true));

        log.info("Verified pod {} IS ready with phase=Running (as expected after waiting)", podName);
    }
}
