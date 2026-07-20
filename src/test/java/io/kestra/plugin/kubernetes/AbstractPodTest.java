package io.kestra.plugin.kubernetes;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kubernetes.services.PodService;

import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.CopyOrReadable;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.TtyExecErrorable;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

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

        Mockito.when(container.withReadyWaitTimeout(PodService.EXEC_READY_WAIT_TIMEOUT_MS))
            .thenReturn(container);

        Mockito.when(container.file(Mockito.anyString()))
            .thenReturn(fileUploader);

        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class)))
            .thenReturn(true);

        RunContext runContext = runContextFactory.of(Map.of());
        Path temp = PodService.tempDir(runContext);

        Files.createDirectories(temp);
        Files.writeString(temp.resolve("a.txt"), "AAA");
        Files.writeString(temp.resolve("b.txt"), "BBB");

        Set<String> inputFiles = Set.of("a.txt", "b.txt");

        TestPod pod = new TestPod();

        try (MockedStatic<PodService> staticMock = Mockito.mockStatic(PodService.class)) {

            staticMock.when(() -> PodService.tempDir(runContext)).thenReturn(temp);

            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).then(inv -> null);

            staticMock.when(() -> PodService.withRetries(Mockito.any(), Mockito.anyString(), Mockito.any())).thenCallRealMethod();

            pod.uploadInputFiles(runContext, podResource, logger, inputFiles);
        }

        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/a.txt");
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/b.txt");

        Mockito.verify(fileUploader, Mockito.times(2)).upload(Mockito.any(InputStream.class));
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldFallBackToPerFileUploadWhenBulkVerificationDetectsTruncatedTransfer() throws Exception {
        // Regression test: fabric8's dir().upload() can report success even when the tar transfer was
        // truncated (e.g. a Python dependency directory silently missing files). Post-upload verification
        // must catch the mismatch and fall back to re-uploading every file individually.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer("init-files")).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(PodService.EXEC_READY_WAIT_TIMEOUT_MS)).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        // fabric8 falsely reports success even though the transfer was truncated
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);

        // Simulate the verification exec: the directory file-count check reports only 1 file (truncated),
        // while the per-file size checks that follow during the fallback report the correct byte counts.
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                // Mockito expands varargs into individual arguments for InvocationOnMock, regardless of
                // how the real call packed them, so read them via getArguments() rather than getArgument(0).
                Object[] command = execInvocation.getArguments();
                String shellCommand = (String) command[command.length - 1];
                String response = shellCommand.contains("find")
                    ? "1"
                    : shellCommand.contains("pkg1.txt") ? "2" : "3";
                out.write(response.getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        RunContext runContext = runContextFactory.of(Map.of());
        Path temp = PodService.tempDir(runContext);

        Files.createDirectories(temp.resolve("deps"));
        Files.writeString(temp.resolve("deps/pkg1.txt"), "AA");
        Files.writeString(temp.resolve("deps/pkg2.txt"), "BBB");

        Set<String> inputFiles = Set.of("deps/pkg1.txt", "deps/pkg2.txt");

        TestPod pod = new TestPod();

        try (var staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            staticMock.when(() -> PodService.tempDir(runContext)).thenReturn(temp);
            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).thenAnswer(inv -> null);

            pod.uploadInputFiles(runContext, podResource, logger, inputFiles);
        }

        // Bulk directory upload was attempted first...
        Mockito.verify(container, Mockito.times(1)).dir("/kestra/working-dir/deps");
        Mockito.verify(dirUploader, Mockito.times(1)).upload(temp.resolve("deps"));

        // ...but verification detected the truncated transfer, so every file was re-uploaded individually.
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/deps/pkg1.txt");
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/deps/pkg2.txt");
        Mockito.verify(fileUploader, Mockito.times(2)).upload(Mockito.any(InputStream.class));
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldAcceptBulkUploadWhenVerificationCountsMatch() throws Exception {
        // Happy-path companion to shouldFallBackToPerFileUploadWhenBulkVerificationDetectsTruncatedTransfer:
        // the pod-side file count matches the local directory, so verification passes and no per-file
        // fallback upload happens.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer("init-files")).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(PodService.EXEC_READY_WAIT_TIMEOUT_MS)).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);

        // Realistic verification exec: the pod-side file count matches the two files uploaded locally.
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                out.write("2".getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        RunContext runContext = runContextFactory.of(Map.of());
        Path temp = PodService.tempDir(runContext);

        Files.createDirectories(temp.resolve("deps"));
        Files.writeString(temp.resolve("deps/pkg1.txt"), "AA");
        Files.writeString(temp.resolve("deps/pkg2.txt"), "BBB");

        Set<String> inputFiles = Set.of("deps/pkg1.txt", "deps/pkg2.txt");

        TestPod pod = new TestPod();

        try (var staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            staticMock.when(() -> PodService.tempDir(runContext)).thenReturn(temp);
            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).thenAnswer(inv -> null);

            pod.uploadInputFiles(runContext, podResource, logger, inputFiles);
        }

        // Bulk directory upload was attempted, and the verification exec actually ran...
        Mockito.verify(container, Mockito.times(1)).dir("/kestra/working-dir/deps");
        Mockito.verify(dirUploader, Mockito.times(1)).upload(temp.resolve("deps"));
        Mockito.verify(container, Mockito.atLeastOnce()).writingOutput(Mockito.any(OutputStream.class));

        // ...counts matched, so no per-file fallback upload was needed.
        Mockito.verify(container, Mockito.never()).file(Mockito.anyString());
    }
}
