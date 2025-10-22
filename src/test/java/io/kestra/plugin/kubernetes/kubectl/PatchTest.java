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
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Slf4j
class PatchTest {
    @Inject
    private RunContextFactory runContextFactory;

    /**
     * Test strategic merge patch (default strategy).
     * This test patches a deployment's container resources.
     */
    @Test
    void testStrategicMergePatch() throws Exception {
        var runContext = runContextFactory.of();

        // First, create a deployment to patch
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                """
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: patch-test-deployment
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
                            resources:
                              limits:
                                memory: "128Mi"
                                cpu: "100m"
                    """
            ))
            .build();

        applyTask.run(runContext);

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName("patch-test-deployment")
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
            .resourceName(Property.ofValue("patch-test-deployment"))
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
        assertThat(output.getMetadata().getName(), equalTo("patch-test-deployment"));
    }

    /**
     * Test JSON Patch strategy with replace operation.
     * This test scales a deployment by changing the replica count.
     */
    @Test
    void testJsonPatch() throws Exception {
        var runContext = runContextFactory.of();

        // Create a deployment
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                """
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: json-patch-test
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
                    """
            ))
            .build();

        applyTask.run(runContext);

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName("json-patch-test")
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
            .resourceName(Property.ofValue("json-patch-test"))
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
        assertThat(output.getMetadata().getName(), equalTo("json-patch-test"));
    }

    /**
     * Test JSON Merge Patch strategy for removing annotations.
     * This demonstrates using null values to delete fields.
     */
    @Test
    void testJsonMergePatch() throws Exception {
        var runContext = runContextFactory.of();

        // Create a deployment with an annotation
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                """
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: json-merge-test
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
                    """
            ))
            .build();

        applyTask.run(runContext);

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName("json-merge-test")
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
            .resourceName(Property.ofValue("json-merge-test"))
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
        assertThat(output.getMetadata().getName(), equalTo("json-merge-test"));
    }

    /**
     * Test patch with wait functionality.
     * This verifies that the wait mechanism doesn't cause errors.
     */
    @Test
    void testPatchWithWait() throws Exception {
        var runContext = runContextFactory.of();

        // Create a deployment
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                """
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: wait-test
                      labels:
                        app: wait-test
                    spec:
                      replicas: 1
                      selector:
                        matchLabels:
                          app: wait-test
                      template:
                        metadata:
                          labels:
                            app: wait-test
                        spec:
                          containers:
                          - name: nginx
                            image: nginx:latest
                    """
            ))
            .build();

        applyTask.run(runContext);

        // Wait for deployment to be ready before patching
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName("wait-test")
                    .get();
                if (deployment == null) {
                    return false;
                }
                // Check if deployment has Ready condition
                var conditions = deployment.getStatus() != null ? deployment.getStatus().getConditions() : null;
                if (conditions == null) {
                    return false;
                }
                return conditions.stream()
                    .anyMatch(c -> "Available".equals(c.getType()) && "True".equals(c.getStatus()));
            }, Duration.ofMillis(200), Duration.ofMinutes(2));
        }

        // Patch with wait enabled (short timeout for test)
        var patchTask = Patch.builder()
            .id("patch-with-wait")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("deployment"))
            .resourceName(Property.ofValue("wait-test"))
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
            .wait(Property.ofValue(true))
            .waitTimeout(Property.ofValue(Duration.ofMinutes(2)))
            .build();

        var output = patchTask.run(runContext);

        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo("wait-test"));
    }

    /**
     * Test patching with Kestra template variables.
     * This ensures that property rendering works correctly.
     */
    @Test
    void testPatchWithTemplating() throws Exception {
        var runContext = runContextFactory.of(java.util.Map.of(
            "deploymentName", "templating-test",
            "newReplicas", 3
        ));

        // Create a deployment
        var applyTask = Apply.builder()
            .id("create-deployment")
            .type(Apply.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(Property.ofValue(
                """
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: templating-test
                      labels:
                        app: templating
                    spec:
                      replicas: 1
                      selector:
                        matchLabels:
                          app: templating
                      template:
                        metadata:
                          labels:
                            app: templating
                        spec:
                          containers:
                          - name: nginx
                            image: nginx:latest
                    """
            ))
            .build();

        applyTask.run(runContext);

        // Wait for deployment to be created
        try (KubernetesClient client = PodService.client(runContext, null)) {
            Await.until(() -> {
                var deployment = client.apps().deployments()
                    .inNamespace("default")
                    .withName("templating-test")
                    .get();
                return deployment != null;
            }, Duration.ofMillis(100), Duration.ofSeconds(30));
        }

        // Patch using template variables
        var patchTask = Patch.builder()
            .id("patch-templating")
            .type(Patch.class.getName())
            .namespace(Property.ofValue("default"))
            .resourceType(Property.ofValue("deployment"))
            .resourceName(Property.ofValue("{{ deploymentName }}"))
            .patchStrategy(Property.ofValue(PatchStrategy.JSON_PATCH))
            .patch(Property.ofValue(
                """
                    [
                      {"op": "replace", "path": "/spec/replicas", "value": {{ newReplicas }}}
                    ]
                    """
            ))
            .build();

        var output = patchTask.run(runContext);

        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), equalTo("templating-test"));
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
}
