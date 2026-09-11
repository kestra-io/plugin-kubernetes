package io.kestra.plugin.kubernetes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kubernetes.shared.services.PodService;

import io.fabric8.kubernetes.api.model.ContainerStateBuilder;
import io.fabric8.kubernetes.api.model.ContainerStateTerminatedBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.CopyOrReadable;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class AbstractPodTest {

    @Inject
    RunContextFactory runContextFactory;

    public static class TestPod extends AbstractPod {
        public TestPod() {
            super();
        }
    }

    @Test
    void shouldUploadInputFiles() throws Exception {
        PodResource podResource = Mockito.mock(PodResource.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);
        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(podResource.inContainer("init-files"))
            .thenReturn(container);

        Mockito.when(container.withReadyWaitTimeout(0))
            .thenReturn(container);

        Mockito.when(container.file(Mockito.anyString()))
            .thenReturn(fileUploader);

        Mockito.when(fileUploader.upload(Mockito.any(Path.class)))
            .thenReturn(true);

        RunContext runContext = runContextFactory.of(Map.of());
        Path temp = PodService.tempDir(runContext);

        Files.createDirectories(temp);
        Files.writeString(temp.resolve("a.txt"), "AAA");
        Files.writeString(temp.resolve("b.txt"), "BBB");

        Set<String> inputFiles = Set.of("a.txt", "b.txt");

        TestPod pod = new TestPod();

        // This delegate now forwards to the real PodService.uploadInputFiles (see plugin-kubernetes-lib),
        // so let unstubbed statics run for real and only intercept the ones this test needs to control.
        try (MockedStatic<PodService> staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {

            staticMock.when(() -> PodService.tempDir(runContext)).thenReturn(temp);

            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).then(inv -> null);

            pod.uploadInputFiles(runContext, podResource, logger, inputFiles);
        }

        // Pins the fix for #329: init-files uploads must skip the pod-Ready wait, since the pod
        // structurally cannot become Ready while init-files itself is blocked on the ready marker.
        // atLeastOnce (not an exact count): PodService.uploadMarker also builds its own container chain
        // through withReadyWaitTimeout(0), so the call count is an implementation detail — the value 0 is
        // the guard, not how many times it's set.
        Mockito.verify(container, Mockito.atLeastOnce()).withReadyWaitTimeout(0);

        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/a.txt");
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/b.txt");

        Mockito.verify(fileUploader, Mockito.times(2)).upload(Mockito.any(Path.class));
    }

    @Test
    void shouldTolerateMarkerUploadFailureWhenInitContainerSucceeded() throws Exception {
        // Regression test: fabric8's exec WebSocket can close before reporting a clean result even though
        // the init container already consumed the ready marker and exited. That must be tolerated, not
        // surfaced as a task failure.
        PodResource podResource = Mockito.mock(PodResource.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(podResource.inContainer("init-files")).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(0)).thenReturn(container);

        ContainerStatus initFilesStatus = new ContainerStatusBuilder()
            .withName(AbstractPod.INIT_FILES_CONTAINER_NAME)
            .withState(new ContainerStateBuilder()
                .withTerminated(new ContainerStateTerminatedBuilder().withExitCode(0).build())
                .build())
            .build();
        Pod terminatedPod = new PodBuilder()
            .withNewStatus()
                .withInitContainerStatuses(initFilesStatus)
            .endStatus()
            .build();
        Mockito.when(podResource.get()).thenReturn(terminatedPod);

        RunContext runContext = runContextFactory.of(Map.of());
        TestPod pod = new TestPod();

        // inputFiles is empty, so tempDir is computed but never dereferenced — no need to stub it.
        // CALLS_REAL_METHODS: the delegate now forwards to the real PodService.uploadInputFiles, which
        // must actually run (and reach uploadMarker) for this regression test to be meaningful.
        try (MockedStatic<PodService> staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).thenThrow(new IOException("exec WebSocket closed before result"));

            assertDoesNotThrow(() -> pod.uploadInputFiles(runContext, podResource, logger, Set.of()));
        }
    }

    @Test
    void shouldPropagateMarkerUploadFailureWhenInitContainerDidNotSucceed() throws Exception {
        PodResource podResource = Mockito.mock(PodResource.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(podResource.inContainer("init-files")).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(0)).thenReturn(container);

        ContainerStatus initFilesStatus = new ContainerStatusBuilder()
            .withName(AbstractPod.INIT_FILES_CONTAINER_NAME)
            .withState(new ContainerStateBuilder()
                .withTerminated(new ContainerStateTerminatedBuilder().withExitCode(1).build())
                .build())
            .build();
        Pod failedPod = new PodBuilder()
            .withNewStatus()
                .withInitContainerStatuses(initFilesStatus)
            .endStatus()
            .build();
        Mockito.when(podResource.get()).thenReturn(failedPod);

        RunContext runContext = runContextFactory.of(Map.of());
        TestPod pod = new TestPod();

        // inputFiles is empty, so tempDir is computed but never dereferenced — no need to stub it.
        // CALLS_REAL_METHODS: the delegate now forwards to the real PodService.uploadInputFiles, which
        // must actually run (and reach uploadMarker) for this regression test to be meaningful.
        try (MockedStatic<PodService> staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).thenThrow(new IOException("exec WebSocket closed before result"));

            assertThrows(IOException.class, () -> pod.uploadInputFiles(runContext, podResource, logger, Set.of()));
        }
    }
}
