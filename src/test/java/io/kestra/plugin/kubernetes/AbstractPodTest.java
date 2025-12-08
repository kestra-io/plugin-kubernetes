package io.kestra.plugin.kubernetes;

import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.CopyOrReadable;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kubernetes.services.PodService;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

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

            staticMock.when(() ->
                PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).then(inv -> null);

            staticMock.when(() -> PodService.withRetries(Mockito.any(), Mockito.anyString(), Mockito.any())).thenCallRealMethod();

            pod.uploadInputFiles(runContext, podResource, logger, inputFiles);
        }
        
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/a.txt");
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/b.txt");

        Mockito.verify(fileUploader, Mockito.times(2)).upload(Mockito.any(InputStream.class));
    }
}
