package org.kestra.task.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.executions.LogEntry;
import org.kestra.core.models.flows.Flow;
import org.kestra.core.queues.QueueFactoryInterface;
import org.kestra.core.queues.QueueInterface;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.serializers.JacksonMapper;
import org.kestra.core.utils.TestsUtils;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Named;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@EnableRuleMigrationSupport
class PodCreateTest {
    private static final ObjectMapper mapper = JacksonMapper.ofYaml();

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> workerTaskLogQueue;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        List<LogEntry> logs = new ArrayList<>();
        workerTaskLogQueue.receive(logs::add);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace("test")
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "containers:",
                "- name: unittest",
                "  image: debian:stable-slim",
                "  command: ",
                "    - 'bash' ",
                "    - '-c'",
                "    - 'for i in {1..10}; do echo $i; sleep 0.1; done'",
                "restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, ImmutableMap.of());
        runContext = runContext.forWorker(applicationContext, TestsUtils.mockTaskRun(flow, execution, task));

        PodCreate.Output runOutput = task.run(runContext);

        assertThat(runOutput.getMetadata().getName(), containsString(((Map<String, String>) runContext.getVariables().get("taskrun")).get("id").toLowerCase()));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).count(), is(11L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).skip(9).findFirst().get().getMessage(), is("10"));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).skip(10).findFirst().get().getMessage(), containsString("is deleted"));
    }

    @Test
    void failed() throws Exception {
        List<LogEntry> logs = new ArrayList<>();
        workerTaskLogQueue.receive(logs::add);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace("test")
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
        Execution execution = TestsUtils.mockExecution(flow, ImmutableMap.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        RunContext runContextFinal = runContext.forWorker(applicationContext, TestsUtils.mockTaskRun(flow, execution, task));

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> task.run(runContextFinal)
        );
        assertThat(exception.getMessage(),  containsString("'Failed', exitcode '1'"));
    }

    @Test
    void failedAfterStartup() throws Exception {
        List<LogEntry> logs = new ArrayList<>();
        workerTaskLogQueue.receive(logs::add);

        PodCreate task = PodCreate.builder()
            .id(PodCreate.class.getSimpleName())
            .type(PodCreate.class.getName())
            .namespace("test")
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
        Execution execution = TestsUtils.mockExecution(flow, ImmutableMap.of());
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        RunContext runContextFinal = runContext.forWorker(applicationContext, TestsUtils.mockTaskRun(flow, execution, task));

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> task.run(runContextFinal)
        );
        assertThat(exception.getMessage(),  containsString("'Failed', exitcode '1'"));
    }
}
