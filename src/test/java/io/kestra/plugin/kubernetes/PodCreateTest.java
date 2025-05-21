package io.kestra.plugin.kubernetes;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.RunContextInitializer;
import io.kestra.core.runners.WorkerTask;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
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

    @Test
    void run() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.of("default"))
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
        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(flow, execution, task)).build());

        PodCreate.Output runOutput = task.run(runContext);

        Thread.sleep(500);

        assertThat(runOutput.getMetadata().getName(), containsString("iokestrapluginkubernetespodcreatetest-run-podcreate"));

        List<LogEntry> logs = receive.collectList().block();
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).count(), is(13L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().equals("10")).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().contains("is deleted")).count(), is(1L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).filter(logEntry -> logEntry.getMessage().equals("error")).count(), is(1L));
    }

    @Test
    void failed() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.of("default"))
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
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(flow, execution, task)).build());

        assertThrows( IllegalStateException.class, () -> task.run(runContextFinal));
    }

    @Test
    void failedAfterStartup() throws Exception {
        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.of("default"))
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
        RunContext runContextFinal = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(flow, execution, task)).build());

        assertThrows(IllegalStateException.class, () -> task.run(runContextFinal));
    }

    @Test
    void resume() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(workerTaskLogQueue);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace(Property.of("default"))
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
            .resume(Property.of(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());

        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(flow, execution, task)).build());

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
            .namespace(Property.of("default"))
            .outputFiles(Property.of(Arrays.asList("xml", "csv")))
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
            .resume(Property.of(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of("command", "sleep"));


        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        runContext = runContextInitializer.forWorker((DefaultRunContext) runContext, WorkerTask.builder().task(task).taskRun(TestsUtils.mockTaskRun(flow, execution, task)).build());

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
}
