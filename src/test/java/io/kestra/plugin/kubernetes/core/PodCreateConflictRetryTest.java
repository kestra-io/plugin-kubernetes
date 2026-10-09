package io.kestra.plugin.kubernetes.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

class PodCreateConflictRetryTest {
    private static KubernetesClientException status(int code, String reason) {
        return new KubernetesClientException("boom", code, new StatusBuilder().withCode(code).withReason(reason).build());
    }

    @SuppressWarnings("unchecked")
    private static PodResource podResource(KubernetesClient client) {
        var pods = mock(MixedOperation.class);
        var inNamespace = mock(NonNamespaceOperation.class);
        var resource = mock(PodResource.class);
        when(client.pods()).thenReturn(pods);
        when(pods.inNamespace("ns")).thenReturn(inNamespace);
        when(inNamespace.resource(any(Pod.class))).thenReturn(resource);
        return resource;
    }

    private static final Pod POD = new PodBuilder().withNewMetadata().withName("p").endMetadata().build();

    @Test
    void retriesQuotaConflictThenReturnsCreatedPod() {
        var client = mock(KubernetesClient.class);
        var resource = podResource(client);
        when(resource.create()).thenThrow(status(409, "Conflict")).thenReturn(POD);

        var created = PodCreate.createWithConflictRetry(mock(Logger.class), client, "ns", POD);

        assertThat(created, is(sameInstance(POD)));
        verify(resource, times(2)).create();
    }

    @Test
    void alreadyExistsIsNotRetried() {
        var client = mock(KubernetesClient.class);
        var resource = podResource(client);
        when(resource.create()).thenThrow(status(409, "AlreadyExists"));

        assertThrows(KubernetesClientException.class, () -> PodCreate.createWithConflictRetry(mock(Logger.class), client, "ns", POD));
        verify(resource, times(1)).create();
    }
}
