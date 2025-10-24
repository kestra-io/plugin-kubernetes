package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.plugin.kubernetes.models.PatchStrategy;
import io.kestra.plugin.kubernetes.services.PodService;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Slf4j
class PatchTest {
    @Inject
    private RunContextFactory runContextFactory;

    // Track created resources for cleanup
    private final List<ResourceToCleanup> resourcesToCleanup = new ArrayList<>();

    @AfterEach
    void cleanup() throws Exception {
        var runContext = runContextFactory.of();
        try (KubernetesClient client = PodService.client(runContext, null)) {
            for (var resource : resourcesToCleanup) {
                try {
                    log.info("Cleaning up {} '{}' in namespace '{}'", resource.type, resource.name, resource.namespace);
                    switch (resource.type.toLowerCase()) {
                        case "deployment" -> client.apps().deployments()
                            .inNamespace(resource.namespace)
                            .withName(resource.name)
                            .delete();
                        case "pod" -> client.pods()
                            .inNamespace(resource.namespace)
                            .withName(resource.name)
                            .delete();
                        default -> log.warn("Unknown resource type for cleanup: {}", resource.type);
                    }
                } catch (Exception e) {
                    log.warn("Failed to cleanup {} '{}': {}", resource.type, resource.name, e.getMessage());
                }
            }
            resourcesToCleanup.clear();
        }
    }

    private void trackForCleanup(String type, String name, String namespace) {
        resourcesToCleanup.add(new ResourceToCleanup(type, name, namespace));
    }

    private record ResourceToCleanup(String type, String name, String namespace) {}

    /**
     * Test strategic merge patch (default strategy).
     * This test patches a deployment's container resources.
     */
    @Test
    void testStrategicMergePatch() throws Exception {
        var runContext = runContextFactory.of();

        String deploymentName = "iokestrapluginkuberneteskubectlpatchtest-strategicmerge-" + System.currentTimeMillis();

        // First, create a deployment to patch
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                String.format("""
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: %s
                      labels:
                        app: patch-test
                    spec:
                      replicas: 1
                      selector:
                        matchLabels:
                          app: patch-test
                      template:
                        metadata:
                          labels:
                            app: patch-test
                        spec:
                          containers:
                          - name: nginx
                            image: nginx:1.19
                    """, deploymentName)
            ))
            .build();

        applyTask.run(runContext);
        trackForCleanup("deployment", deploymentName, "default");

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName(deploymentName)
                    .get();
                return deployment != null;
            }, Duration.ofMillis(100), Duration.ofSeconds(30));
        }

        // Now patch the deployment using strategic merge
        var patchTask = Patch.builder()
            .id("patch-deployment")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("deployment"))
            .resourceName(Property.ofValue(deploymentName))
            .apiGroup(Property.ofValue("apps"))
            .patchStrategy(Property.ofValue(PatchStrategy.STRATEGIC_MERGE))
            .patch(Property.ofValue(
                """
                    {
                      "spec": {
                        "template": {
                          "spec": {
                            "containers": [
                              {
                                "name": "nginx",
                                "resources": {
                                  "limits": {
                                    "memory": "256Mi",
                                    "cpu": "200m"
                                  }
                                }
                              }
                            ]
                          }
                        }
                      }
                    }
                    """
            ))
            .build();

        var output = patchTask.run(runContext);

        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo(deploymentName));
        assertThat(output.getStatus(), notNullValue());
    }

    /**
     * Test JSON Patch strategy with replace operation.
     * This test scales a deployment by changing the replica count.
     */
    @Test
    void testJsonPatch() throws Exception {
        var runContext = runContextFactory.of();

        String deploymentName = "iokestrapluginkuberneteskubectlpatchtest-jsonpatch-" + System.currentTimeMillis();

        // Create a deployment
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                String.format("""
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: %s
                      labels:
                        app: json-patch
                    spec:
                      replicas: 2
                      selector:
                        matchLabels:
                          app: json-patch
                      template:
                        metadata:
                          labels:
                            app: json-patch
                        spec:
                          containers:
                          - name: nginx
                            image: nginx:latest
                    """, deploymentName)
            ))
            .build();

        applyTask.run(runContext);
        trackForCleanup("deployment", deploymentName, "default");

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName(deploymentName)
                    .get();
                return deployment != null;
            }, Duration.ofMillis(100), Duration.ofSeconds(30));
        }

        // Patch using JSON Patch operations
        var patchTask = Patch.builder()
            .id("json-patch")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("deployment"))
            .resourceName(Property.ofValue(deploymentName))
            .apiGroup(Property.ofValue("apps"))
            .patchStrategy(Property.ofValue(PatchStrategy.JSON_PATCH))
            .patch(Property.ofValue(
                """
                    [
                      {"op": "replace", "path": "/spec/replicas", "value": 5}
                    ]
                    """
            ))
            .build();

        var output = patchTask.run(runContext);

        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo(deploymentName));
    }

    /**
     * Test JSON Merge Patch strategy for removing annotations.
     * This demonstrates using null values to delete fields.
     */
    @Test
    void testJsonMergePatch() throws Exception {
        var runContext = runContextFactory.of();

        String deploymentName = "iokestrapluginkuberneteskubectlpatchtest-jsonmerge-" + System.currentTimeMillis();

        // Create a deployment with an annotation
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                String.format("""
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: %s
                      labels:
                        app: json-merge
                      annotations:
                        deprecated: "true"
                        test-annotation: "value"
                    spec:
                      replicas: 1
                      selector:
                        matchLabels:
                          app: json-merge
                      template:
                        metadata:
                          labels:
                            app: json-merge
                        spec:
                          containers:
                          - name: nginx
                            image: nginx:latest
                    """, deploymentName)
            ))
            .build();

        applyTask.run(runContext);
        trackForCleanup("deployment", deploymentName, "default");

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName(deploymentName)
                    .get();
                return deployment != null;
            }, Duration.ofMillis(100), Duration.ofSeconds(30));
        }

        // Use JSON Merge to remove the deprecated annotation
        var patchTask = Patch.builder()
            .id("json-merge-patch")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("deployment"))
            .resourceName(Property.ofValue(deploymentName))
            .apiGroup(Property.ofValue("apps"))
            .patchStrategy(Property.ofValue(PatchStrategy.JSON_MERGE))
            .patch(Property.ofValue(
                """
                    {
                      "metadata": {
                        "annotations": {
                          "deprecated": null
                        }
                      }
                    }
                    """
            ))
            .build();

        var output = patchTask.run(runContext);

        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo(deploymentName));
    }

    /**
     * Test patch with wait functionality on a Pod.
     * This verifies that the wait mechanism works correctly with Ready condition.
     */
    @Test
    void testPatchWithWait() throws Exception {
        var runContext = runContextFactory.of();

        String podName = "iokestrapluginkuberneteskubectlpatchtest-patchwithwait-" + System.currentTimeMillis();

        // Create a simple pod
        var applyTask = Apply.builder()
            .id("create-pod")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                String.format("""
                    apiVersion: v1
                    kind: Pod
                    metadata:
                      name: %s
                      labels:
                        app: patch-wait-test
                    spec:
                      containers:
                      - name: nginx
                        image: nginx:latest
                        ports:
                        - containerPort: 80
                    """, podName)
            ))
            .build();

        applyTask.run(runContext);
        trackForCleanup("pod", podName, "default");

        // Wait for pod to be ready before patching
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var pod = client.pods()
                    .inNamespace("default")
                    .withName(podName)
                    .get();
                if (pod == null || pod.getStatus() == null) {
                    return false;
                }
                var conditions = pod.getStatus().getConditions();
                if (conditions == null) {
                    return false;
                }
                return conditions.stream()
                    .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
            }, Duration.ofMillis(200), Duration.ofMinutes(2));
        }

        // Patch pod with wait enabled - just add a label (won't trigger restart)
        var patchTask = Patch.builder()
            .id("patch-pod-with-wait")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("pod"))
            .resourceName(Property.ofValue(podName))
            .patch(Property.ofValue(
                """
                    {
                      "metadata": {
                        "labels": {
                          "patched": "true"
                        }
                      }
                    }
                    """
            ))
            .waitUntilReady(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var output = patchTask.run(runContext);

        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo(podName));

        // Verify status output contains pod status information
        assertThat(output.getStatus(), notNullValue());
        assertThat(output.getStatus().getStatus(), notNullValue());

        // Pods should have a phase in their status
        var statusMap = output.getStatus().getStatus();
        assertThat(statusMap.get("phase"), notNullValue());
        assertThat(statusMap.get("phase"), equalTo("Running"));

        // Verify conditions exist (Ready, Initialized, etc.)
        assertThat(statusMap.get("conditions"), notNullValue());
        assertThat(statusMap.get("conditions"), instanceOf(java.util.List.class));

        // Verify containerStatuses exist
        assertThat(statusMap.get("containerStatuses"), notNullValue());
        assertThat(statusMap.get("containerStatuses"), instanceOf(java.util.List.class));

        // Verify the label was added
        try (KubernetesClient client = PodService.client(runContext, null)) {
            var pod = client.pods()
                .inNamespace("default")
                .withName(podName)
                .get();
            assertThat(pod.getMetadata().getLabels().get("patched"), equalTo("true"));
        }
    }

    /**
     * Test patching deployment replicas with JSON Patch.
     * This verifies the task works correctly with JSON Patch operations.
     */
    @Test
    void testPatchDeploymentReplicas() throws Exception {
        var runContext = runContextFactory.of();

        String deploymentName = "iokestrapluginkuberneteskubectlpatchtest-replicas-" + System.currentTimeMillis();

        // Create a deployment
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                String.format("""
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: %s
                      labels:
                        app: replica-test
                    spec:
                      replicas: 1
                      selector:
                        matchLabels:
                          app: replica-test
                      template:
                        metadata:
                          labels:
                            app: replica-test
                        spec:
                          containers:
                          - name: nginx
                            image: nginx:latest
                    """, deploymentName)
            ))
            .build();

        applyTask.run(runContext);
        trackForCleanup("deployment", deploymentName, "default");

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName(deploymentName)
                    .get();
                return deployment != null;
            }, Duration.ofMillis(100), Duration.ofSeconds(30));
        }

        // Patch to scale deployment
        var patchTask = Patch.builder()
            .id("patch-replicas")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("deployment"))
            .resourceName(Property.ofValue(deploymentName))
            .apiGroup(Property.ofValue("apps"))
            .patchStrategy(Property.ofValue(PatchStrategy.JSON_PATCH))
            .patch(Property.ofValue(
                """
                    [
                      {"op": "replace", "path": "/spec/replicas", "value": 3}
                    ]
                    """
            ))
            .build();

        var output = patchTask.run(runContext);

        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo(deploymentName));

        // Verify status output is present
        assertThat(output.getStatus(), notNullValue());
        // Status map might be null initially for deployments, but the object should exist

        // Verify replicas were updated
        try (KubernetesClient client = PodService.client(runContext, null)) {
            var deployment = client.apps().deployments()
                .inNamespace("default")
                .withName(deploymentName)
                .get();
            assertThat(deployment.getSpec().getReplicas(), equalTo(3));
        }
    }

    /**
     * Test error handling when patching non-existent resource.
     */
    @Test
    void testPatchNonExistentResource() {
        var runContext = runContextFactory.of();

        var patchTask = Patch.builder()
            .id("patch-nonexistent")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("deployment"))
            .resourceName(Property.ofValue("does-not-exist"))
            .patch(Property.ofValue(
                """
                    {
                      "spec": {
                        "replicas": 5
                      }
                    }
                    """
            ))
            .build();

        // Should throw an exception
        try {
            patchTask.run(runContext);
            throw new AssertionError("Expected exception for non-existent resource");
        } catch (Exception e) {
            assertThat(e.getMessage(), anyOf(
                containsString("not found"),
                containsString("Not Found"),
                containsString("404")
            ));
        }
    }

    /**
     * Test that output status is refreshed after wait completes.
     * Creates a pod with a broken image (won't start), then patches it to a good image
     * and waits for it to become ready. Verifies the output reflects the final ready state.
     */
    @Test
    void testOutputRefreshedAfterWaitFromBrokenToWorking() throws Exception {
        var runContext = runContextFactory.of();

        String podName = "iokestrapluginkuberneteskubectlpatchtest-refresh-" + System.currentTimeMillis();

        // Create a pod with a non-existent image that will never be ready
        var applyTask = Apply.builder()
            .id("create-broken-pod")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                String.format("""
                    apiVersion: v1
                    kind: Pod
                    metadata:
                      name: %s
                      labels:
                        app: refresh-test
                    spec:
                      containers:
                      - name: app
                        image: nonexistent-image-that-will-never-exist:v999
                    """, podName)
            ))
            .build();

        applyTask.run(runContext);
        trackForCleanup("pod", podName, "default");

        // Wait a bit to ensure pod is in ImagePullBackOff/ErrImagePull state
        Thread.sleep(2000);

        // Verify pod is NOT ready before patching
        try (KubernetesClient client = PodService.client(runContext, null)) {
            var pod = client.pods()
                .inNamespace("default")
                .withName(podName)
                .get();

            assertThat(pod, notNullValue());
            assertThat(pod.getStatus(), notNullValue());

            // Pod should not be ready
            if (pod.getStatus().getConditions() != null) {
                var readyCondition = pod.getStatus().getConditions().stream()
                    .filter(c -> "Ready".equals(c.getType()))
                    .findFirst();

                if (readyCondition.isPresent()) {
                    assertThat(readyCondition.get().getStatus(), not(equalTo("True")));
                }
            }
        }

        // Patch pod to use a working image and wait for it to become ready
        var patchTask = Patch.builder()
            .id("patch-to-working-image")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("pod"))
            .resourceName(Property.ofValue(podName))
            .patch(Property.ofValue(
                """
                {
                  "spec": {
                    "containers": [
                      {
                        "name": "app",
                        "image": "nginx:latest"
                      }
                    ]
                  }
                }
                """
            ))
            .waitUntilReady(Property.ofValue(Duration.ofMinutes(1)))
            .build();

        var output = patchTask.run(runContext);

        // Verify output contains current metadata
        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo(podName));

        // Output should reflect the UPDATED ready state after wait
        assertThat(output.getStatus(), notNullValue());
        assertThat(output.getStatus().getStatus(), notNullValue());

        var statusMap = output.getStatus().getStatus();

        // Pod should now be Running (not Pending)
        assertThat(statusMap.get("phase"), equalTo("Running"));

        // Verify Ready condition exists and is True
        assertThat(statusMap.get("conditions"), notNullValue());
        assertThat(statusMap.get("conditions"), instanceOf(java.util.List.class));

        @SuppressWarnings("unchecked")
        var conditions = (java.util.List<java.util.Map<String, Object>>) statusMap.get("conditions");
        var readyCondition = conditions.stream()
            .filter(c -> "Ready".equals(c.get("type")))
            .findFirst();

        assertThat(readyCondition.isPresent(), equalTo(true));
        assertThat(readyCondition.get().get("status"), equalTo("True"));

        // Container should be running with the new image
        assertThat(statusMap.get("containerStatuses"), notNullValue());
        assertThat(statusMap.get("containerStatuses"), instanceOf(java.util.List.class));

        @SuppressWarnings("unchecked")
        var containerStatuses = (java.util.List<java.util.Map<String, Object>>) statusMap.get("containerStatuses");
        assertThat(containerStatuses.size(), equalTo(1));

        var containerStatus = containerStatuses.get(0);
        assertThat(containerStatus.get("ready"), equalTo(true));

        // Verify the image was actually changed to nginx
        String imageUsed = (String) containerStatus.get("image");
        assertThat(imageUsed, containsString("nginx"));
    }
}
