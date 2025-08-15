package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.ThreadMainFactoryBuilder;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link PodLogService#close()} cleanly stops the internal listener thread.
 *
 * <p>Background (bug this test guards against):
 * Historically, {@code close()} used {@code shutdownNow()} on the scheduler while a fixed-rate task
 * was often waiting for its next run. In that situation the underlying {@code ScheduledFuture}
 * could remain non-terminal (neither done nor cancelled), and the listener thread would block
 * forever on {@code future.get()}, preventing shutdown.
 *
 * <p>Expected behavior after the fix:
 * {@code close()} transitions the scheduled future to a terminal state (or otherwise ensures the
 * listener won't block), so the listener thread terminates shortly after {@code close()}.
 */
@KestraTest
class PodLogServiceTest {

    @Inject
    ThreadMainFactoryBuilder threadMainFactoryBuilder;

    @Test
    void closeStopsK8SListenerThread() {
        // --- Arrange: minimal mocks so the scheduled task runs fast and then waits for the next tick ---
        KubernetesClient client = mock(KubernetesClient.class);
        Pod pod = mock(Pod.class);
        PodSpec spec = mock(PodSpec.class);
        when(pod.getSpec()).thenReturn(spec);
        when(spec.getContainers()).thenReturn(Collections.emptyList()); // keep task body fast

        // Provide required metadata (podRef reads namespace/name)
        when(pod.getMetadata()).thenReturn(
            new ObjectMetaBuilder().withNamespace("default").withName("test-pod").build()
        );

        // Stub Fabric8 fluent chain: client.pods().inNamespace("default").withName("test-pod") -> PodResource
        @SuppressWarnings("unchecked")
        MixedOperation<Pod, PodList, PodResource> podsOp = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<Pod, PodList, PodResource> nsOp = mock(NonNamespaceOperation.class);
        PodResource podResource = mock(PodResource.class);

        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace("default")).thenReturn(nsOp);
        when(nsOp.withName("test-pod")).thenReturn(podResource);
        when(podResource.get()).thenReturn(pod);

        // RunContext/Logger and log consumer can be simple mocks
        RunContext runContext = mock(RunContext.class);
        when(runContext.logger()).thenReturn(mock(Logger.class));
        AbstractLogConsumer logConsumer = mock(AbstractLogConsumer.class);

        PodLogService svc = new PodLogService(threadMainFactoryBuilder);
        // start watch
        svc.watch(client, pod, logConsumer, runContext);

        // --- Grab the listener thread (before close) and assert it started ---
        Thread listener = getListenerThreadOrFail(svc);
        assertTrue(listener != null && listener.isAlive(), "k8s-listener should be started after watch()");

        // --- Act: ensure the first scheduled execution has completed and the task is waiting again.
        // We deliberately wait a short moment so shutdown won't interrupt a running tick and skew the assertion.
        try {
            Thread.sleep(500); // allow first tick to finish; task now waits for the next 30s run
            svc.close();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }

        // --- Assert: listener terminates within a short deadline (otherwise it's stuck on future.get()) ---
        boolean stopped = waitUntilNotAlive(listener);
        Assertions.assertTrue(
            stopped,
            "k8s-listener should terminate after close(); if not, it's stuck waiting on a non-terminal future"
        );
    }

    /** Reflectively obtains the internal listener thread or fails the test with a helpful message. */
    private static Thread getListenerThreadOrFail(PodLogService svc) {
        try {
            Field f = PodLogService.class.getDeclaredField("thread");
            f.setAccessible(true);
            Thread t = (Thread) f.get(svc);
            assertNotNull(t, "listener thread should be initialized by watch()");
            return t;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assertions.fail("PodLogService should expose a 'thread' field for the listener in tests", e);
            return null; // unreachable
        }
    }

    /** Waits until the given thread is not alive, checking every 50ms up to 500ms total. */
    private static boolean waitUntilNotAlive(Thread t) {
        for (int i = 0; i < 10; i++) { // 10 * 50ms = 500ms
            if (!t.isAlive()) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return !t.isAlive();
    }
}