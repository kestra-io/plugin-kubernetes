package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link PodService#waitForPodReady} method.
 * 
 * <p>These tests verify that the readiness check correctly validates the ContainersReady
 * condition status must be "True" before considering a Pod as ready.
 */
@ExtendWith(MockitoExtension.class)
class PodServiceTest {

    @Mock
    private KubernetesClient client;

    @Mock
    private MixedOperation<Pod, PodList, PodResource> podsOperation;

    @Mock
    private NonNamespaceOperation<Pod, PodList, PodResource> namespaceOperation;

    @Mock
    private PodResource podResource;

    @BeforeEach
    void setUp() {
        when(client.pods()).thenReturn(podsOperation);
        when(podsOperation.inNamespace("default")).thenReturn(namespaceOperation);
        when(namespaceOperation.withName("test-pod")).thenReturn(podResource);
    }

    @Test
    @SuppressWarnings("unchecked")
    void waitForPodReadyShouldReturnWhenContainersReadyIsTrue() {
        // Arrange: Create a Pod with ContainersReady condition status = "True"
        Pod pod = createPodWithCondition("ContainersReady", "True", null);
        when(podResource.waitUntilCondition(any(Predicate.class), anyLong(), any())).thenReturn(pod);

        // Act
        Pod result = PodService.waitForPodReady(client, pod, Duration.ofSeconds(10));

        // Assert
        assertNotNull(result);
        Predicate<Pod> predicate = capturePredicate();
        assertThat("Predicate should return true for ContainersReady=True", 
            predicate.test(pod), is(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"False", "Unknown", ""})
    @SuppressWarnings("unchecked")
    void waitForPodReadyShouldNotReturnWhenContainersReadyStatusIsNotTrue(String status) {
        // Arrange: Create a Pod with ContainersReady condition but status is not "True"
        Pod pod = createPodWithCondition("ContainersReady", status, null);
        when(podResource.waitUntilCondition(any(Predicate.class), anyLong(), any())).thenReturn(pod);

        // Act
        PodService.waitForPodReady(client, pod, Duration.ofSeconds(10));

        // Assert
        Predicate<Pod> predicate = capturePredicate();
        assertThat("Predicate should return false when ContainersReady status is not 'True'", 
            predicate.test(pod), is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    void waitForPodReadyShouldReturnWhenPodCompleted() {
        // Arrange: Create a Pod with PodCompleted reason (should still be accepted)
        Pod pod = createPodWithCondition("Ready", "False", "PodCompleted");
        when(podResource.waitUntilCondition(any(Predicate.class), anyLong(), any())).thenReturn(pod);

        // Act
        Pod result = PodService.waitForPodReady(client, pod, Duration.ofSeconds(10));

        // Assert
        assertNotNull(result);
        Predicate<Pod> predicate = capturePredicate();
        assertThat("Predicate should return true for PodCompleted reason", 
            predicate.test(pod), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    void waitForPodReadyShouldReturnWhenPhaseIsFailed() {
        // Arrange: Create a Pod with phase = "Failed"
        Pod pod = createPodWithPhase("Failed");
        when(podResource.waitUntilCondition(any(Predicate.class), anyLong(), any())).thenReturn(pod);

        // Act
        Pod result = PodService.waitForPodReady(client, pod, Duration.ofSeconds(10));

        // Assert
        assertNotNull(result);
        Predicate<Pod> predicate = capturePredicate();
        assertThat("Predicate should return true for phase=Failed", 
            predicate.test(pod), is(true));
    }

    /**
     * Helper method to capture and return the predicate passed to waitUntilCondition.
     */
    @SuppressWarnings("unchecked")
    private Predicate<Pod> capturePredicate() {
        ArgumentCaptor<Predicate<Pod>> predicateCaptor = ArgumentCaptor.forClass(Predicate.class);
        verify(podResource).waitUntilCondition(predicateCaptor.capture(), eq(10L), any());
        return predicateCaptor.getValue();
    }

    /**
     * Helper method to create a Pod with a specific condition.
     */
    private Pod createPodWithCondition(String conditionType, String conditionStatus, String conditionReason) {
        PodCondition condition = new PodCondition();
        condition.setType(conditionType);
        condition.setStatus(conditionStatus);
        if (conditionReason != null) {
            condition.setReason(conditionReason);
        }

        PodStatus status = new PodStatus();
        status.setConditions(List.of(condition));
        status.setPhase("Running");

        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("test-pod");
        metadata.setNamespace("default");

        Pod pod = new Pod();
        pod.setMetadata(metadata);
        pod.setStatus(status);

        return pod;
    }

    /**
     * Helper method to create a Pod with a specific phase.
     */
    private Pod createPodWithPhase(String phase) {
        PodStatus status = new PodStatus();
        status.setPhase(phase);
        status.setConditions(List.of());

        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("test-pod");
        metadata.setNamespace("default");

        Pod pod = new Pod();
        pod.setMetadata(metadata);
        pod.setStatus(status);

        return pod;
    }
}

