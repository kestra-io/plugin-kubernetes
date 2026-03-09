package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class PodServiceTest {
    @Test
    void waitForContainersStartedOrCompletedShouldRetryOnClientTimeoutWhenPodStillExists() {
        var client = Mockito.mock(KubernetesClient.class);
        var podResource = Mockito.mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-1")
            .withNamespace("default")
            .endMetadata()
            .build();

        Mockito.when(podResource.waitUntilCondition(Mockito.any(), Mockito.eq(30L), Mockito.eq(TimeUnit.SECONDS)))
            .thenThrow(new KubernetesClientTimeoutException("Timed out", "Pod", "pod-1", 10L, TimeUnit.SECONDS))
            .thenReturn(pod);
        Mockito.when(podResource.get()).thenReturn(pod);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var result = PodService.waitForContainersStartedOrCompleted(client, pod, Duration.ofSeconds(30));

            assertThat(result, is(pod));
            Mockito.verify(podResource).get();
            Mockito.verify(podResource, Mockito.times(2)).waitUntilCondition(Mockito.any(), Mockito.eq(30L), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void withRetriesShouldWrapRetryFailureAsIoException() {
        var logger = Mockito.mock(Logger.class);

        var exception = assertThrows(IOException.class, () -> PodService.withRetries(
            logger,
            "uploadMarker",
            () -> {
                throw new KubernetesClientException("boom");
            }
        ));

        assertThat(exception.getMessage(), is("Failed to call 'uploadMarker'"));
    }
}
