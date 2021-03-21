package io.kestra.plugin.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
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
class JobCreateTest {
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

        JobCreate task = JobCreate.builder()
            .id(JobCreate.class.getSimpleName())
            .type(JobCreate.class.getName())
            .namespace("test")
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "template:",
                "  spec:",
                "    containers:",
                "    - name: unittest",
                "      image: debian:stable-slim",
                "      command: ",
                "        - 'bash' ",
                "        - '-c'",
                "        - 'for i in {1..10}; do echo $i; sleep 0.1; done'",
                "    restartPolicy: Never"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, ImmutableMap.of());
        runContext = runContext.forWorker(applicationContext, TestsUtils.mockTaskRun(flow, execution, task));

        JobCreate.Output runOutput = task.run(runContext);

        Thread.sleep(500);

        assertThat(runOutput.getJobMetadata().getName(), containsString(((Map<String, String>) runContext.getVariables().get("taskrun")).get("id").toLowerCase()));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).count(), is(11L));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).skip(9).findFirst().get().getMessage(), is("10"));
        assertThat(logs.stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).skip(10).findFirst().get().getMessage(), containsString("is deleted"));
    }

    @Test
    void failed() throws Exception {
        List<LogEntry> logs = new ArrayList<>();
        workerTaskLogQueue.receive(logs::add);


        JobCreate task = JobCreate.builder()
            .id(JobCreate.class.getSimpleName())
            .type(JobCreate.class.getName())
            .namespace("test")
            .spec(TestUtils.convert(
                ObjectMeta.class,
                "template:",
                "  spec:",
                "    containers:",
                "    - name: unittest",
                "      image: debian:stable-slim",
                "      command: ",
                "        - 'bash' ",
                "        - '-c'",
                "        - 'exit 1'",
                "    restartPolicy: Never"
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
