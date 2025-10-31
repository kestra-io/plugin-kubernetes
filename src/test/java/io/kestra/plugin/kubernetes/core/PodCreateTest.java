package io.kestra.plugin.kubernetes.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.CharStreams;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.*;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kubernetes.TestUtils;
import io.kestra.plugin.kubernetes.models.SideCar;
import io.kestra.plugin.kubernetes.services.PodService;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Slf4j
class PodCreateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> workerTaskLogQueue;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextInitializer runContextInitializer;

    /**
     * Asserts that a log with the exact message appears exactly once (no missing, no duplicates).
     */
    private void assertLogExactlyOnce(List<LogEntry> logs, String expectedMessage) {
        long count = logs.stream()
            .filter(log -> log.getMessage() != null && log.getMessage().equals(expectedMessage))
            .count();
        assertThat("Missing or duplicate log: " + expectedMessage, count, is(1L));
    }

    @Test
    void run() throws Exception {
        AtomicInteger logCounter = new AtomicInteger(0);
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            if (logEntry.getLeft().getLevel() == Level.INFO) {
                logCounter.incrementAndGet();
            }
        });

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'for i in {1..10}; do echo $i; {{ inputs.command }} 0.1; done; >&2 echo \"error\"'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        PodCreate.Output runOutput = task.run(runContext);

        assertThat(runOutput.getMetadata().getName(), containsString("iokestrapluginkubernetescorepodcreatetest-run-podcreate"));

        // Wait for all logs to be collected (expect 14 INFO logs)
        Await.until(
            () -> logCounter.get() >= 14,
            Duration.ofMillis(100),
            Duration.ofSeconds(5)
        );

        List<LogEntry> logs = receive.collectList().block();

        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).count(), is(14L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().equals("10")).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().contains("is deleted")).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().equals("error")).count(), is(1L));
    }

    @Test
    void failed() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(runContextFinal, null)) {
            assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));

            // Verify pod was deleted after failure
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofSeconds(10));
        }
    }

    @Test
    void failedAfterStartup() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'sleep 1 && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));
    }

    @Test
    void failedWithOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("results.json")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Container failing\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));
    }

    @Test
    void failedWithOutputFilesDeletesPod() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("results.json")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Container failing\" && sleep 1 && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Task failed as expected.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod creation
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty();
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().get(0);
            String podName = createdPod.getMetadata().getName();
            log.info("Test detected pod creation: {}", podName);

            // Wait for pod to be deleted despite the failure
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(2));

            log.info("Pod {} was successfully deleted after failure with outputFiles.", podName);
        } finally {
            taskFuture.cancel(true);
            executorService.shutdownNow();
        }
    }

    @Test
    void sidecarExitsGracefullyOnFailure() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("results.json")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Container failing\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Task failed as expected.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod creation and completion
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                if (pods.isEmpty()) {
                    return false;
                }
                var pod = pods.get(0);
                String phase = pod.getStatus() != null ? pod.getStatus().getPhase() : null;
                return "Failed".equals(phase) || "Succeeded".equals(phase);
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var completedPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().get(0);
            String podName = completedPod.getMetadata().getName();
            log.info("Pod {} completed with phase: {}", podName, completedPod.getStatus().getPhase());

            // Verify sidecar container exists and check its status
            var containerStatuses = completedPod.getStatus().getContainerStatuses();
            var sidecarStatus = containerStatuses.stream()
                .filter(status -> status.getName().equals("out-files"))
                .findFirst();

            assertThat("Sidecar container should exist", sidecarStatus.isPresent(), is(true));

            // Check if sidecar is still running (it shouldn't be if marker was signaled)
            if (sidecarStatus.get().getState().getRunning() != null) {
                log.warn("Sidecar is still running - marker may not have been signaled");
            }

            // Wait for pod deletion and measure time
            long deletionStartTime = System.currentTimeMillis();
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                if (!pods.isEmpty()) {
                    var pod = pods.get(0);
                    var sidecarCurrentStatus = pod.getStatus().getContainerStatuses().stream()
                        .filter(status -> status.getName().equals("out-files"))
                        .findFirst();

                    if (sidecarCurrentStatus.isPresent()) {
                        var state = sidecarCurrentStatus.get().getState();
                        if (state.getTerminated() != null) {
                            String reason = state.getTerminated().getReason();
                            log.info("Sidecar terminated with reason: {}", reason);
                            // Verify sidecar terminated gracefully (Completed), not force-killed (Error/Killed)
                            assertThat("Sidecar should exit gracefully with Completed status",
                                reason, is("Completed"));
                        }
                    }
                }
                return pods.isEmpty();
            }, Duration.ofMillis(200), Duration.ofMinutes(1));
            long deletionDuration = System.currentTimeMillis() - deletionStartTime;

            // Verify pod deletion was fast (< 15 seconds indicates graceful sidecar exit)
            assertThat("Pod deletion should be fast when sidecar exits gracefully (< 15s)",
                deletionDuration, lessThan(15000L));

            log.info("Pod {} deleted in {}ms - sidecar exited gracefully", podName, deletionDuration);
        } finally {
            taskFuture.cancel(true);
            executorService.shutdownNow();
        }
    }

    @Test
    void missingInputFilesFailsFastWithValidation() throws Exception {
        // Test for issue #211: validation prevents pod creation when inputFiles are invalid
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .inputFiles(Map.of("data.txt", "{{ outputs['nonexistent-task']['outputFiles']['data.txt'] }}"))
            .waitUntilRunning(Property.ofValue(Duration.ofSeconds(10)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'cat {{ workingDir }}/data.txt'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        // Should fail fast with validation error before pod creation
        long startTime = System.currentTimeMillis();
        Exception exception = assertThrows(Exception.class, () -> task.run(finalRunContext));
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Verify error message mentions the problematic file reference
        assertThat(exception.getMessage(), containsString("outputs"));
        assertThat(exception.getMessage(), containsString("nonexistent-task"));

        // Should fail in < 2 seconds, not wait 30 seconds for timeout
        assertThat(elapsedTime, lessThan(2000L));

        // Verify no pod was created
        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
            assertThat(pods, empty());
        }

        log.info("Validation prevented pod creation and failed in {}ms", elapsedTime);
    }

    @Test
    void resume() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'for i in {1..10}; do echo $i; {{ inputs.command }} 0.1; done'",
                "restartPolicy: Never"
            ))
            .resume(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());

        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        RunContext finalRunContext = runContext;

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Flux<LogEntry> shutdownReceive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            if (logEntry.getLeft().getMessage().equals("1")) {
                executorService.shutdownNow();
            }
        });

        executorService.execute(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.warn("Exception", e);
            }
        });

        Await.until(executorService::isShutdown, Duration.ofMillis(100), Duration.ofMinutes(1));
        shutdownReceive.blockLast();

        task.run(finalRunContext);

        assertThat(receive.toStream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().equals("10")).count(), greaterThan(0L));
    }

    @RetryingTest(value = 3)
    void inputOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(Arrays.asList("xml", "csv")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .inputFiles(Map.of(
                "files/in/in.txt", "I'm here",
                "main.sh", "sleep 1\n" +
                    "echo '::{\"outputs\": {\"extract\":\"'$(cat files/in/in.txt)'\"}}::'\n" +
                    "echo 1 >> {{ outputFiles.xml }}\n" +
                    "echo 2 >> {{ outputFiles.csv }}\n" +
                    "echo 3 >> {{ outputFiles.xml }}\n" +
                    "sleep 1"
            ))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  workingDir: /kestra/working-dir",
                "  command: ",
                "    - 'bash' ",
                "    - '-c' ",
                "    - 'ls -lh && bash main.sh {{ outputFiles.xml }}'",
                "restartPolicy: Never"
            ))
            .metadata(Map.of("name", "custom-name-" + IdUtils.create().toLowerCase()))
            .resume(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        PodCreate.Output run = task.run(runContext);

        Thread.sleep(500);

        assertThat(run.getVars().get("extract"), is("I'm here"));

        InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("xml"));

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is("1\n3\n")
        );

        get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("csv"));

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is("2\n")
        );
    }

    @Test
    void workingDirCreatedWithOnlyOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("*.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: file-writer",
                "  image: debian:stable-slim",
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - >-",
                "      echo 'hello from pod' > {{ workingDir }}/hello.txt",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker(
            (DefaultRunContext) runContext,
            WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build()
        );

        PodCreate.Output run = task.run(runContext);

        assertThat(run.getOutputFiles(), hasKey("hello.txt"));

        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("hello.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("hello from pod"));
    }

    @Test
    void sidecarResources() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();
        SideCar sidecar = mapper.readValue(
            """
                resources:
                  limits:
                    cpu: 200m
                    memory: 256Mi
                  requests:
                    cpu: 100m
                    memory: 128Mi""",
            SideCar.class);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .fileSidecar(sidecar)
            .inputFiles(Map.of(
                "in.txt", "File content"
            ))
            .outputFiles(Property.ofValue(List.of("out.txt")))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: in-out-files",
                "  image: debian:stable-slim",
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - >-",
                "      cat {{ workingDir }}/in.txt > {{ workingDir }}/out.txt",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Unexpected error.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.getFirst().getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().getFirst();
            assertThat(createdPod.getStatus().getPhase(), is("Running"));
            String podName = createdPod.getMetadata().getName();
            log.info("Test detected pod creation: {}", podName);

            ResourceRequirements initReqs = createdPod.getSpec().getInitContainers().getFirst().getResources();
            assertThat(initReqs.getLimits().get("cpu"), is(Quantity.parse("200m")));
            assertThat(initReqs.getLimits().get("memory"), is(Quantity.parse("256Mi")));
            assertThat(initReqs.getRequests().get("cpu"), is(Quantity.parse("100m")));
            assertThat(initReqs.getRequests().get("memory"), is(Quantity.parse("128Mi")));

            ResourceRequirements sideReqs = createdPod.getSpec().getContainers().getLast().getResources();
            assertThat(sideReqs.getLimits().get("cpu"), is(Quantity.parse("200m")));
            assertThat(sideReqs.getLimits().get("memory"), is(Quantity.parse("256Mi")));
            assertThat(sideReqs.getRequests().get("cpu"), is(Quantity.parse("100m")));
            assertThat(sideReqs.getRequests().get("memory"), is(Quantity.parse("128Mi")));

            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            log.info("Pod {} has successfully completed.", podName);
        }

        assertThat(run[0].getOutputFiles(), hasKey("out.txt"));
        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, run[0].getOutputFiles().get("out.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("File content"));
    }

    @Test
    void outputFilesWithSpecialChars() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("**.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: file-writer",
                "  image: debian:stable-slim",
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - >-",
                "      echo 'I am fulfilled' > {{ workingDir }}/special\\ file.txt &&",
                "      mkdir {{ workingDir }}/sub\\ dir &&",
                "      echo 'I have content' > {{ workingDir }}/sub\\ dir/more\\ special\\ file.txt",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker(
            (DefaultRunContext) runContext,
            WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build()
        );

        PodCreate.Output run = task.run(runContext);

        assertThat(run.getOutputFiles(), hasKey("special file.txt"));
        assertThat(run.getOutputFiles(), hasKey("sub dir/more special file.txt"));

        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("special file.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("I am fulfilled"));

        file = storageInterface.get(TenantService.MAIN_TENANT, null, run.getOutputFiles().get("sub dir/more special file.txt"));
        content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("I have content"));
    }

    @Test
    void kill() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo start kestra task && sleep 60 && echo end kestra test'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                log.debug("Task run interrupted.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.get(0).getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().get(0);
            String podName = createdPod.getMetadata().getName();
            log.info("Test detected pod creation: {}", podName);
            assertThat(createdPod.getStatus().getPhase(), is("Running"));

            task.kill();

            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            log.info("Pod {} has been successfully deleted after kill.", podName);
        } finally {
            taskFuture.cancel(true);
            executorService.shutdownNow();
        }
    }

    @Test
    void parseOutputsWithSpecialChars() throws Exception {
        PodCreate task = PodCreate.builder()
            .id("special-char-test")
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: special-char-container",
                "  image: debian:stable-slim",
                "  command:",
                "    - 'bash'",
                "    - '-c'",
                "    - \"echo '::{\\\"outputs\\\": {\\\"PROJECT_ID\\\": 101, \\\"PROJECT_NAME\\\": \\\"One O One\\\", \\\"LABEL\\\": \\\"4004\\\"}}::'\"",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker(
            (DefaultRunContext) runContext,
            WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build()
        );

        PodCreate.Output runOutput = task.run(runContext);

        // Assert that all special char outputs are parsed and available
        assertThat(runOutput.getVars().get("PROJECT_ID").toString(), is("101"));
        assertThat(runOutput.getVars().get("PROJECT_NAME"), is("One O One"));
        assertThat(runOutput.getVars().get("LABEL").toString(), is("4004"));
    }

    @Test
    void successWithOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Task succeeded\" > {{ workingDir }}/result.txt && exit 0'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        PodCreate.Output output = task.run(runContextFinal);

        assertThat(output.getOutputFiles(), hasKey("result.txt"));
        InputStream file = storageInterface.get(TenantService.MAIN_TENANT, null, output.getOutputFiles().get("result.txt"));
        String content = CharStreams.toString(new InputStreamReader(file));
        assertThat(content.trim(), is("Task succeeded"));
    }

    @Test
    void multipleContainersOneFailsWithOutputFiles() throws Exception {
        AtomicInteger logCounter = new AtomicInteger(0);
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            String message = logEntry.getLeft().getMessage();
            if (message != null && (message.equals("First container succeeded") || message.equals("Second container failing"))) {
                logCounter.incrementAndGet();
            }
        });

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: container-success",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"First container succeeded\" && exit 0'",
                "- name: container-failure",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo \"Second container failing\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));
        assertThat(exception.getMessage(), containsString("container-failure"));
        assertThat(exception.getMessage(), containsString("exit code 1"));

        // Wait for logs from both containers to be collected
        Await.until(
            () -> logCounter.get() >= 2,
            Duration.ofMillis(100),
            Duration.ofSeconds(5)
        );

        List<LogEntry> logs = receive.collectList().block();

        // Verify logs from both containers were collected exactly once (no duplicates)
        assertLogExactlyOnce(logs, "First container succeeded");
        assertLogExactlyOnce(logs, "Second container failing");
    }

    @Test
    void completeLogCollectionAfterQuickTermination() throws Exception {
        AtomicInteger expectedLogCounter = new AtomicInteger(0);
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            String message = logEntry.getLeft().getMessage();
            if (message != null && (message.startsWith("Log line ") || message.equals("FINAL"))) {
                expectedLogCounter.incrementAndGet();
            }
        });

        // Generate exactly 20 identifiable log lines in quick succession, then fail
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(10)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command:",
                "    - 'bash'",
                "    - '-c'",
                "    - 'for i in {1..20}; do echo \"Log line $i\"; done; echo \"FINAL\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());
        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));

        // Wait for all logs to be collected with retry mechanism (expect 20 numbered + 1 FINAL = 21 logs)
        Await.until(
            () -> expectedLogCounter.get() >= 21,
            Duration.ofMillis(100),
            Duration.ofSeconds(5)
        );

        List<LogEntry> logs = receive.collectList().block();

        // Verify all 20 numbered logs + FINAL were collected exactly once (no missing/duplicates)
        for (int i = 1; i <= 20; i++) {
            assertLogExactlyOnce(logs, "Log line " + i);
        }
        assertLogExactlyOnce(logs, "FINAL");
    }

}
