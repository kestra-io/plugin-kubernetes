package io.kestra.plugin.kubernetes.services;

import java.time.Duration;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class PodServiceTest {

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
}
