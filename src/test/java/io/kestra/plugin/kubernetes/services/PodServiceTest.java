package io.kestra.plugin.kubernetes.services;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;

import io.kestra.core.junit.annotations.KestraTest;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class PodServiceTest {

    @Test
    void withRetriesShouldExhaustAttemptsThenSurfaceDetailedHint() {
        var logger = mock(Logger.class);
        var attempts = new AtomicInteger(0);

        var exception = assertThrows(
            IOException.class, () -> PodService.withRetries(
                logger,
                "uploadMarker",
                () ->
                {
                    attempts.incrementAndGet();
                    return false;
                }
            )
        );

        // A persistent `false` result must exhaust every retry attempt, not fail fast on the first one.
        assertThat("all attempts must be exhausted before giving up", attempts.get(), is(5));
        // On exhaustion, the detailed hint (attempt count, sh/tar troubleshooting pointer) must survive instead
        // of being discarded into a bare RetryFailed with no cause, wrapped as a generic "Failed to call" message.
        assertThat(exception.getMessage(), is(
            "Failed to call 'uploadMarker' after 5/5 attempts — the Kubernetes copy/exec call kept reporting " +
                "failure without further detail; verify connectivity to the pod and that the file-sidecar image provides a working 'sh' and 'tar'"
        ));
    }

    @Test
    void withVerificationRetriesShouldSucceedAfterTransientFailure() throws Exception {
        var logger = mock(Logger.class);
        var attempts = new AtomicInteger(0);

        PodService.withVerificationRetries(logger, "verifyDirectoryUpload", () ->
        {
            if (attempts.incrementAndGet() == 1) {
                throw new IOException("transient exec failure");
            }
        });

        assertThat("the check must have been retried exactly once before succeeding", attempts.get(), is(2));
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    void withVerificationRetriesShouldExhaustAttemptsThenWrapAsIoException() {
        var logger = mock(Logger.class);
        var attempts = new AtomicInteger(0);

        var exception = assertThrows(
            IOException.class, () -> PodService.withVerificationRetries(
                logger,
                "verifyDirectoryUpload",
                () ->
                {
                    attempts.incrementAndGet();
                    throw new IOException("upload verification failed: expected 3 file(s) but found 1");
                }
            )
        );

        // A persistent verification mismatch must exhaust every retry attempt, not fail fast on the first one.
        assertThat("all attempts must be exhausted before giving up", attempts.get(), is(5));
        // On exhaustion, the actionable detail built by the check (e.g. the file-count mismatch) must survive
        // instead of being discarded into a generic "Failed to call" message.
        assertThat(exception.getMessage(), is("upload verification failed: expected 3 file(s) but found 1"));
    }

    @Test
    void shouldNotReturnPodWhenContainersReadyFalse() {
        KubernetesClient client = mock(KubernetesClient.class);

        Pod pod = new PodBuilder()
            .withNewMetadata()
            .withName("test-pod")
            .withNamespace("default")
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("test-container")
            .withImage("busybox")
            .endContainer()
            .endSpec()
            .withNewStatus()
            .addNewCondition()
            .withType("ContainersReady")
            .withStatus("False")
            .endCondition()
            .endStatus()
            .build();

        @SuppressWarnings("rawtypes")
        MixedOperation pods = mock(MixedOperation.class);

        @SuppressWarnings("rawtypes")
        NonNamespaceOperation ns = mock(NonNamespaceOperation.class);

        PodResource podResource = mock(PodResource.class);

        when(client.pods()).thenReturn(pods);
        when(pods.inNamespace("default")).thenReturn(ns);
        when(ns.withName("test-pod")).thenReturn(podResource);

        when(podResource.waitUntilCondition(any(), anyLong(), any()))
            .thenAnswer(invocation ->
            {
                Predicate<Pod> predicate = invocation.getArgument(0);
                return predicate.test(pod) ? pod : null;
            });

        Pod result = PodService.waitForPodReady(client, pod, Duration.ofSeconds(1));

        assertNull(result);
    }

    @Test
    void waitForContainersStartedOrCompletedShouldReturnImmediatelyWhenPodAlreadySucceeded() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-succeeded")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Succeeded")
            .endStatus()
            .build();

        when(podResource.get()).thenReturn(pod);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var result = PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofMinutes(30));

            assertThat(result, is(pod));
            // Fast path: no watch call should have been made
            verify(podResource, never()).waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldReturnImmediatelyWhenPodAlreadyRunning() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-running")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .addNewContainerStatus()
            .withName("main")
            .withNewState().withNewRunning().endRunning().endState()
            .endContainerStatus()
            .endStatus()
            .build();

        when(podResource.get()).thenReturn(pod);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var result = PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofMinutes(30));

            assertThat(result, is(pod));
            // Fast path: no watch call should have been made
            verify(podResource, never()).waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldReturnOnFallbackGetAfterTimeoutWhenConditionSatisfied() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pendingPod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-pending")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();
        var succeededPod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-pending")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Succeeded")
            .endStatus()
            .build();

        // Fast-path GET returns pending (no early return), watch times out, fallback GET returns succeeded
        when(podResource.get()).thenReturn(pendingPod, succeededPod);
        when(podResource.waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS)))
            .thenThrow(new KubernetesClientTimeoutException("Timed out", "Pod", "pod-pending", 10L, TimeUnit.SECONDS));

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pendingPod)).thenReturn(podResource);

            var result = PodService.waitForContainersStartedOrCompleted(client, logger, pendingPod, Duration.ofMinutes(10));

            assertThat(result, is(succeededPod));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldThrowWhenPodAlreadyDeletedOnFastPath() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-1")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();

        // Fast-path GET returns null immediately — pod is already gone before watch is set up
        when(podResource.get()).thenReturn(null);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofMinutes(10))
            );

            assertThat(exception.getMessage(), is("Pod was deleted while waiting for containers to start: pod-1"));
            // Only the fast-path GET should have been called; watch is never reached
            Mockito.verify(podResource, Mockito.times(1)).get();
            verify(podResource, never()).waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldThrowWhenPodDeletedAfterWatch() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pendingPod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-1")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();

        // Fast-path GET returns a non-started pod (watch is entered), watch chunk times out, fallback GET returns null
        when(podResource.get()).thenReturn(pendingPod, (Pod) null);
        when(podResource.waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS)))
            .thenThrow(new KubernetesClientTimeoutException("Timed out", "Pod", "pod-1", 10L, TimeUnit.SECONDS));

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pendingPod)).thenReturn(podResource);

            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForContainersStartedOrCompleted(client, logger, pendingPod, Duration.ofMinutes(10))
            );

            assertThat(exception.getMessage(), is("Pod was deleted while waiting for containers to start: pod-1"));
            // Two GET calls: fast-path (returns pending) + fallback after watch timeout (returns null)
            Mockito.verify(podResource, Mockito.times(2)).get();
        }
    }
}
