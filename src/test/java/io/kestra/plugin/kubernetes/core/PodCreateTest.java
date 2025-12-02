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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
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

        // Debug: If assertion fails, print all logs and similar messages
        if (count != 1L) {
            System.err.println("=== ASSERTION FAILURE DEBUG ===");
            System.err.println("Expected message: '" + expectedMessage + "'");
            System.err.println("Count: " + count);
            System.err.println("Total logs collected: " + logs.size());
            System.err.println("\nAll collected log messages with timestamps and levels:");
            logs.forEach(log -> {
                String msg = log.getMessage();
                System.err.println("  - [" + log.getTimestamp() + "] [" + log.getLevel() + "] [" + (msg != null ? msg.length() : 0) + " chars] '" + msg + "'");
            });
            System.err.println("\nMessages containing '" + expectedMessage.substring(0, Math.min(10, expectedMessage.length())) + "':");
            logs.stream()
                .filter(log -> log.getMessage() != null && log.getMessage().contains(expectedMessage.substring(0, Math.min(10, expectedMessage.length()))))
                .forEach(log -> System.err.println("  - [" + log.getTimestamp() + "] [" + log.getLevel() + "] '" + log.getMessage() + "'"));
            System.err.println("=== END DEBUG ===\n");
        }

        assertThat("Missing or duplicate log: " + expectedMessage, count, is(1L));
    }

    @Test
    void run() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'seq 1 20 | while read i; do echo \"Log line $i from test pod\"; {{ inputs.command }} 0.05; done; >&2 echo \"error\"'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        PodCreate.Output runOutput = task.run(runContext);

        assertThat(runOutput.getMetadata().getName(), containsString("iokestrapluginkubernetescorepodcreatetest-run-podcreate"));

        List<LogEntry> logs = receive.collectList().block();

        // Verify all 20 log lines are present exactly once (no duplicates, no missing)
        for (int i = 1; i <= 20; i++) {
            assertLogExactlyOnce(logs, "Log line " + i + " from test pod");
        }

        // Verify 'error' log appears exactly once
        assertLogExactlyOnce(logs, "error");
    }

    @Test
    void failed() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .resume(Property.ofValue(false))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
        Logger logger = finalRunContext.logger();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Task failed as expected.", e);
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
            logger.info("Test detected pod creation: {}", podName);

            // Wait for pod to be deleted despite the failure
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(2));

            logger.info("Pod {} was successfully deleted after failure with outputFiles.", podName);
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
        Logger logger = finalRunContext.logger();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Task failed as expected.", e);
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
            logger.info("Pod {} completed with phase: {}", podName, completedPod.getStatus().getPhase());

            // Verify sidecar container exists and check its status
            var containerStatuses = completedPod.getStatus().getContainerStatuses();
            var sidecarStatus = containerStatuses.stream()
                .filter(status -> status.getName().equals("out-files"))
                .findFirst();

            assertThat("Sidecar container should exist", sidecarStatus.isPresent(), is(true));

            // Check if sidecar is still running (it shouldn't be if marker was signaled)
            if (sidecarStatus.get().getState().getRunning() != null) {
                logger.warn("Sidecar is still running - marker may not have been signaled");
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
                            logger.info("Sidecar terminated with reason: {}", reason);
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

            logger.info("Pod {} deleted in {}ms - sidecar exited gracefully", podName, deletionDuration);
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
        Logger logger = finalRunContext.logger();

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

        logger.info("Validation prevented pod creation and failed in {}ms", elapsedTime);
    }

    @Test
    void resume() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
//            .delete(Property.ofValue(false)) // Uncomment for tests if you need to check kubectl logs your_pod
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'seq 1 10 | while read i; do echo \"Resume log line $i\"; {{ inputs.command }} 0.1; done'",
                "restartPolicy: Never"
            ))
            .resume(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());

        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(execution, task)).build());

        RunContext finalRunContext = runContext;
        Logger logger = finalRunContext.logger();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Flux<LogEntry> shutdownReceive = TestsUtils.receive(workerTaskLogQueue, logEntry -> {
            if (logEntry.getLeft().getMessage() != null && logEntry.getLeft().getMessage().equals("Resume log line 1")) {
                executorService.shutdownNow();
            }
        });

        executorService.execute(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                logger.warn("Exception", e);
            }
        });

        Await.until(executorService::isShutdown, Duration.ofMillis(100), Duration.ofMinutes(1));
        shutdownReceive.blockLast();

        task.run(finalRunContext);

        List<LogEntry> logs = receive.toStream().toList();
        assertLogExactlyOnce(logs, "Resume log line 10");
    }

    @Test
    void inputOutputFiles() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(Arrays.asList("xml", "csv")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
        Logger logger = finalRunContext.logger();

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Unexpected error.", e);
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
            logger.info("Test detected pod creation: {}", podName);

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

            logger.info("Pod {} has successfully completed.", podName);
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
        Logger logger = finalRunContext.logger();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Task run interrupted.", e);
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
            logger.info("Test detected pod creation: {}", podName);
            assertThat(createdPod.getStatus().getPhase(), is("Running"));

            task.kill();

            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            logger.info("Pod {} has been successfully deleted after kill.", podName);
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
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

        List<LogEntry> logs = receive.collectList().block();

        // Verify logs from both containers were collected exactly once (no duplicates)
        assertLogExactlyOnce(logs, "First container succeeded");
        assertLogExactlyOnce(logs, "Second container failing");
    }

    @Test
    void completeLogCollectionAfterQuickTermination() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, l -> logs.add(l.getLeft()));

        // Generate exactly 20 identifiable log lines in quick succession, then fail
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .outputFiles(Property.ofValue(List.of("result.txt")))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command:",
                "    - 'bash'",
                "    - '-c'",
                "    - 'seq 1 20 | while read i; do echo \"Quick termination log line $i\"; done; echo \"FINAL\" && exit 1'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());
        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));

        // Wait for all 20 numbered logs (ignores DEBUG/system logs from queue)
        TestsUtils.awaitLogs(logs,
            log -> log.getMessage() != null && log.getMessage().contains("Quick termination log line"),
            20);

        // Wait for Flux completion (ensures FINAL and any remaining logs are processed)
        receive.blockLast();

        // Verify all 20 log lines are present exactly once (no duplicates, no missing)
        for (int i = 1; i <= 20; i++) {
            assertLogExactlyOnce(logs, "Quick termination log line " + i);
        }

        // Verify 'FINAL' log appears exactly once
        assertLogExactlyOnce(logs, "FINAL");
    }

    @Test
    void timeoutDeletesPod() throws Exception {
        // Verify that pod deletion succeeds when the task thread is interrupted.
        // When a thread is interrupted during pod waiting, the cleanup code must
        // still successfully delete the pod, despite the interrupt flag being set.

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue, l -> logs.add(l.getLeft()));

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo start && sleep 120 && echo end'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());
        Logger logger = finalRunContext.logger();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Task interrupted as expected.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod to be running
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.get(0).getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().get(0);
            String podName = createdPod.getMetadata().getName();
            logger.info("Test detected pod creation: {}", podName);
            assertThat(createdPod.getStatus().getPhase(), is("Running"));

            // Interrupt the task thread (simulates timeout or cancellation)
            logger.info("Interrupting task thread");
            taskFuture.cancel(true);

            // Wait for task thread to complete cleanup (poll instead of fixed sleep)
            Await.until(taskFuture::isDone, Duration.ofMillis(100), Duration.ofSeconds(10));

            // Verify that pod deletion was successfully initiated
            // Pod should either be completely removed OR have deletionTimestamp set
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                if (pods.isEmpty()) {
                    logger.info("Pod {} has been completely removed", podName);
                    return true;
                }
                var pod = pods.get(0);
                boolean isTerminating = pod.getMetadata().getDeletionTimestamp() != null;

                if (isTerminating) {
                    logger.info("Pod {} is terminating (deletionTimestamp: {})",
                        podName, pod.getMetadata().getDeletionTimestamp());
                    return true;
                }

                String phase = pod.getStatus() != null ? pod.getStatus().getPhase() : null;
                logger.warn("Pod {} still exists without deletionTimestamp. Phase: {}", podName, phase);
                return false;
            }, Duration.ofMillis(500), Duration.ofSeconds(30));

            logger.info("Pod {} deletion was initiated successfully", podName);

            // Wait for log collection to complete (with timeout)
            receive.blockLast(Duration.ofSeconds(30));

            // Verify no deletion warnings were logged
            long deletionWarnings = logs.stream()
                .filter(log -> log.getMessage() != null && log.getMessage().contains("Unable to delete pod"))
                .count();

            assertThat("Pod deletion should succeed without warnings when thread is interrupted",
                      deletionWarnings, is(0L));

        } finally {
            taskFuture.cancel(true);
            executorService.shutdownNow();
        }
    }

    @Test
    void waitRunningExpirationFailsTask() throws Exception {
        // Verify that waitRunning enforces a maximum duration for pod completion.
        // When a pod runs longer than waitRunning:
        // - Task should fail with an exception after waitRunning duration
        // - Pod should be deleted after failure
        // - Total execution time should be close to waitRunning (not multiple retry cycles)
        //
        // Bug symptom: Task hangs indefinitely, waiting multiple waitRunning cycles
        // instead of failing after the first cycle expires.

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .waitUntilRunning(Property.ofValue(Duration.ofSeconds(60)))  // Allow time for pod startup
            .waitRunning(Property.ofValue(Duration.ofSeconds(3)))        // Short wait to keep test fast
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'echo start && sleep 300'",  // Sleep longer than waitRunning
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());
        Logger logger = finalRunContext.logger();

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        // Run task in background thread
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        long startTime = System.currentTimeMillis();

        Future<?> taskFuture = executorService.submit(() -> {
            try {
                task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Task failed as expected: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        });

        try {
            // Expected behavior (with fix): Task fails with ExecutionException
            // Bug behavior (without fix): Task hangs indefinitely, triggering TimeoutException
            // Timeout set generously to accommodate slow CI (waitUntilRunning + waitRunning + 30s overhead)
            taskFuture.get(100, TimeUnit.SECONDS);

            // If we reach here, task completed successfully (unexpected)
            long elapsedTime = System.currentTimeMillis() - startTime;
            throw new AssertionError(
                String.format("Task unexpectedly completed successfully after %dms. " +
                             "It should have failed because pod runs longer than waitRunning.",
                             elapsedTime));

        } catch (java.util.concurrent.ExecutionException e) {
            // Expected: Task failed because waitRunning expired
            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("Task correctly failed after {}ms (waitRunning: 3s)", elapsedTime);

            // Success - we don't assert on exact timing as it's environment-dependent
            // The important thing is the task failed (not hung indefinitely)

        } catch (java.util.concurrent.TimeoutException e) {
            // Bug detected: Task hung beyond expected failure time
            long elapsedTime = System.currentTimeMillis() - startTime;
            taskFuture.cancel(true);

            throw new AssertionError(
                String.format("Task hung for %dms without failing. " +
                             "This indicates waitRunning is not enforcing a maximum duration. " +
                             "Expected task to fail after waitRunning expired.",
                             elapsedTime));
        } finally {
            executorService.shutdownNow();
        }

        // Verify pod is deleted after failure
        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                if (pods.isEmpty()) {
                    return true;
                }
                // Also accept pods with deletionTimestamp (being deleted)
                return pods.get(0).getMetadata().getDeletionTimestamp() != null;
            }, Duration.ofMillis(500), Duration.ofSeconds(10));

            logger.info("Pod was successfully deleted after waitRunning expiration");
        }
    }

    @Test
    void containerDefaultSpecSecurityContext() throws Exception {
        // Test that containerDefaultSpec.securityContext is applied to all containers
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .containerDefaultSpec(Property.ofValue(Map.of(
                "securityContext", Map.of(
                    "runAsUser", 1000,
                    "runAsGroup", 1000,
                    "allowPrivilegeEscalation", false
                )
            )))
            .inputFiles(Map.of("in.txt", "test content"))
            .outputFiles(Property.ofValue(List.of("out.txt")))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: main",
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
        Logger logger = finalRunContext.logger();

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Unexpected error.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod to be running
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.getFirst().getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().getFirst();
            logger.info("Test detected pod creation: {}", createdPod.getMetadata().getName());

            // Verify security context on main container
            var mainContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("main"))
                .findFirst()
                .orElseThrow();
            assertThat("Main container should have security context", mainContainer.getSecurityContext(), notNullValue());
            assertThat(mainContainer.getSecurityContext().getRunAsUser(), is(1000L));
            assertThat(mainContainer.getSecurityContext().getRunAsGroup(), is(1000L));
            assertThat(mainContainer.getSecurityContext().getAllowPrivilegeEscalation(), is(false));

            // Verify security context on init container
            var initContainer = createdPod.getSpec().getInitContainers().stream()
                .filter(c -> c.getName().equals("init-files"))
                .findFirst()
                .orElseThrow();
            assertThat("Init container should have security context", initContainer.getSecurityContext(), notNullValue());
            assertThat(initContainer.getSecurityContext().getRunAsUser(), is(1000L));
            assertThat(initContainer.getSecurityContext().getAllowPrivilegeEscalation(), is(false));

            // Verify security context on sidecar container
            var sidecarContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("out-files"))
                .findFirst()
                .orElseThrow();
            assertThat("Sidecar container should have security context", sidecarContainer.getSecurityContext(), notNullValue());
            assertThat(sidecarContainer.getSecurityContext().getRunAsUser(), is(1000L));
            assertThat(sidecarContainer.getSecurityContext().getAllowPrivilegeEscalation(), is(false));

            // Wait for pod deletion
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            logger.info("containerDefaultSpec securityContext test passed");
        }

        assertThat(run[0].getOutputFiles(), hasKey("out.txt"));
    }

    @Test
    void containerDefaultSpecVolumeMounts() throws Exception {
        // Test that containerDefaultSpec.volumeMounts is applied to all containers
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .containerDefaultSpec(Property.ofValue(Map.of(
                "volumeMounts", List.of(
                    Map.of("name", "shared-tmp", "mountPath", "/tmp")
                )
            )))
            .inputFiles(Map.of("in.txt", "test content"))
            .outputFiles(Property.ofValue(List.of("out.txt")))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "volumes:",
                "  - name: shared-tmp",
                "    emptyDir: {}",
                "containers:",
                "- name: main",
                "  image: debian:stable-slim",
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - >-",
                "      echo 'temp file' > /tmp/test.txt && cat {{ workingDir }}/in.txt > {{ workingDir }}/out.txt",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());
        Logger logger = finalRunContext.logger();

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Unexpected error.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod to be running
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.getFirst().getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().getFirst();
            logger.info("Test detected pod creation: {}", createdPod.getMetadata().getName());

            // Verify volume mount on main container
            var mainContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("main"))
                .findFirst()
                .orElseThrow();
            var tmpMount = mainContainer.getVolumeMounts().stream()
                .filter(vm -> vm.getMountPath().equals("/tmp"))
                .findFirst();
            assertThat("Main container should have /tmp volume mount", tmpMount.isPresent(), is(true));
            assertThat(tmpMount.get().getName(), is("shared-tmp"));

            // Verify volume mount on init container
            var initContainer = createdPod.getSpec().getInitContainers().stream()
                .filter(c -> c.getName().equals("init-files"))
                .findFirst()
                .orElseThrow();
            var initTmpMount = initContainer.getVolumeMounts().stream()
                .filter(vm -> vm.getMountPath().equals("/tmp"))
                .findFirst();
            assertThat("Init container should have /tmp volume mount", initTmpMount.isPresent(), is(true));

            // Verify volume mount on sidecar container
            var sidecarContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("out-files"))
                .findFirst()
                .orElseThrow();
            var sidecarTmpMount = sidecarContainer.getVolumeMounts().stream()
                .filter(vm -> vm.getMountPath().equals("/tmp"))
                .findFirst();
            assertThat("Sidecar container should have /tmp volume mount", sidecarTmpMount.isPresent(), is(true));

            // Wait for pod deletion
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            logger.info("containerDefaultSpec volumeMounts test passed");
        }

        assertThat(run[0].getOutputFiles(), hasKey("out.txt"));
    }

    @Test
    void containerDefaultSpecContainerOverridesDefaults() throws Exception {
        // Test that container-specific values override containerDefaultSpec defaults
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .containerDefaultSpec(Property.ofValue(Map.of(
                "securityContext", Map.of(
                    "runAsUser", 1000,
                    "runAsGroup", 1000
                )
            )))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: main",
                "  image: debian:stable-slim",
                "  securityContext:",
                "    runAsUser: 2000",  // Override the default runAsUser
                "  command: [\"/bin/sh\"]",
                "  args:",
                "    - -c",
                "    - 'id && echo done'",
                "restartPolicy: Never"
            ))
            .build();

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        TaskRun taskRun = TestsUtils.mockTaskRun(execution, task);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        RunContext finalRunContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(taskRun).build());
        Logger logger = finalRunContext.logger();

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Unexpected error.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod to be running
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.getFirst().getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().getFirst();
            logger.info("Test detected pod creation: {}", createdPod.getMetadata().getName());

            // Verify container-specific runAsUser overrides the default
            var mainContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("main"))
                .findFirst()
                .orElseThrow();
            assertThat("Main container should have security context", mainContainer.getSecurityContext(), notNullValue());
            assertThat("Container-specific runAsUser should override default", mainContainer.getSecurityContext().getRunAsUser(), is(2000L));
            // runAsGroup should come from defaults (deep merge)
            assertThat("Default runAsGroup should be preserved", mainContainer.getSecurityContext().getRunAsGroup(), is(1000L));

            // Wait for pod deletion
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            logger.info("containerDefaultSpec override test passed");
        }
    }

    @Test
    void fileSidecarDefaultSpecOverridesContainerDefaultSpec() throws Exception {
        // Test that fileSidecar.defaultSpec overrides containerDefaultSpec for sidecar containers
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();
        SideCar sidecar = mapper.readValue(
            """
                defaultSpec:
                  securityContext:
                    runAsUser: 3000
                    runAsGroup: 3000""",
            SideCar.class);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .containerDefaultSpec(Property.ofValue(Map.of(
                "securityContext", Map.of(
                    "runAsUser", 1000,  // This should be overridden by fileSidecar.defaultSpec for sidecar
                    "runAsGroup", 1000
                )
            )))
            .fileSidecar(sidecar)
            .inputFiles(Map.of("in.txt", "test content"))
            .outputFiles(Property.ofValue(List.of("out.txt")))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: main",
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
        Logger logger = finalRunContext.logger();

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Unexpected error.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod to be running
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.getFirst().getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().getFirst();
            logger.info("Test detected pod creation: {}", createdPod.getMetadata().getName());

            // Main container should use containerDefaultSpec
            var mainContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("main"))
                .findFirst()
                .orElseThrow();
            assertThat("Main container should use containerDefaultSpec runAsUser", mainContainer.getSecurityContext().getRunAsUser(), is(1000L));

            // Init container should use fileSidecar.defaultSpec (overrides containerDefaultSpec)
            var initContainer = createdPod.getSpec().getInitContainers().stream()
                .filter(c -> c.getName().equals("init-files"))
                .findFirst()
                .orElseThrow();
            assertThat("Init container should use fileSidecar.defaultSpec runAsUser", initContainer.getSecurityContext().getRunAsUser(), is(3000L));
            assertThat("Init container should use fileSidecar.defaultSpec runAsGroup", initContainer.getSecurityContext().getRunAsGroup(), is(3000L));

            // Sidecar container should use fileSidecar.defaultSpec (overrides containerDefaultSpec)
            var sidecarContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("out-files"))
                .findFirst()
                .orElseThrow();
            assertThat("Sidecar container should use fileSidecar.defaultSpec runAsUser", sidecarContainer.getSecurityContext().getRunAsUser(), is(3000L));

            // Wait for pod deletion
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            logger.info("fileSidecar.defaultSpec override test passed");
        }

        assertThat(run[0].getOutputFiles(), hasKey("out.txt"));
    }

    @Test
    void containerDefaultSpecMultipleFields() throws Exception {
        // Test that multiple fields (securityContext + volumeMounts + resources) work together
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.ofValue("default"))
            .waitForLogInterval(Property.ofValue(Duration.ofSeconds(1)))
            .delete(Property.ofValue(true))
            .resume(Property.ofValue(false))
            .containerDefaultSpec(Property.ofValue(Map.of(
                "securityContext", Map.of(
                    "runAsUser", 1000,
                    "allowPrivilegeEscalation", false
                ),
                "volumeMounts", List.of(
                    Map.of("name", "shared-tmp", "mountPath", "/tmp")
                ),
                "resources", Map.of(
                    "limits", Map.of("memory", "128Mi"),
                    "requests", Map.of("memory", "64Mi")
                )
            )))
            .inputFiles(Map.of("in.txt", "test content"))
            .outputFiles(Property.ofValue(List.of("out.txt")))
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "volumes:",
                "  - name: shared-tmp",
                "    emptyDir: {}",
                "containers:",
                "- name: main",
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
        Logger logger = finalRunContext.logger();

        final PodCreate.Output[] run = new PodCreate.Output[1];
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                run[0] = task.run(finalRunContext);
            } catch (Exception e) {
                logger.debug("Unexpected error.", e);
            }
        });

        String labelSelector = "kestra.io/taskrun-id=" + taskRun.getId();

        try (KubernetesClient client = PodService.client(finalRunContext, null)) {
            // Wait for pod to be running
            Await.until(() -> {
                var pods = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems();
                return !pods.isEmpty() && pods.getFirst().getStatus().getPhase().equals("Running");
            }, Duration.ofMillis(200), Duration.ofMinutes(1));

            var createdPod = client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().getFirst();
            logger.info("Test detected pod creation: {}", createdPod.getMetadata().getName());

            // Verify all fields on main container
            var mainContainer = createdPod.getSpec().getContainers().stream()
                .filter(c -> c.getName().equals("main"))
                .findFirst()
                .orElseThrow();

            // Security context
            assertThat(mainContainer.getSecurityContext().getRunAsUser(), is(1000L));
            assertThat(mainContainer.getSecurityContext().getAllowPrivilegeEscalation(), is(false));

            // Volume mounts
            var tmpMount = mainContainer.getVolumeMounts().stream()
                .filter(vm -> vm.getMountPath().equals("/tmp"))
                .findFirst();
            assertThat("Main container should have /tmp volume mount", tmpMount.isPresent(), is(true));

            // Resources
            assertThat(mainContainer.getResources().getLimits().get("memory"), is(Quantity.parse("128Mi")));
            assertThat(mainContainer.getResources().getRequests().get("memory"), is(Quantity.parse("64Mi")));

            // Wait for pod deletion
            Await.until(() -> client.pods().inNamespace("default").withLabelSelector(labelSelector).list().getItems().isEmpty(),
                Duration.ofMillis(200), Duration.ofMinutes(1));

            logger.info("containerDefaultSpec multiple fields test passed");
        }

        assertThat(run[0].getOutputFiles(), hasKey("out.txt"));
    }

}
