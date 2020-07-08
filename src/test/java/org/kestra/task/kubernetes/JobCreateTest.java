package org.kestra.task.kubernetes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.serializers.JacksonMapper;
import org.kestra.core.utils.TestsUtils;
import org.slf4j.event.Level;

import java.util.Map;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
@EnableRuleMigrationSupport
class JobCreateTest {
    private static ObjectMapper mapper = JacksonMapper.ofYaml();

    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        JobCreate task = JobCreate.builder()
            .id(JobCreate.class.getSimpleName())
            .type(JobCreate.class.getName())
            .namespace("default")
            .spec(convert(
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
        JobCreate.Output runOutput = task.run(runContext);

        assertThat(runOutput.getJob().getName(), containsStringIgnoringCase(((Map<String, String>) runContext.getVariables().get("taskrun")).get("id")));
        assertThat(runContext.logs().stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).count(), is(11L));
        assertThat(runContext.logs().stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).skip(9).findFirst().get().getMessage(), is("10"));
        assertThat(runContext.logs().stream().filter(logEntry -> logEntry.getLevel() == Level.INFO).skip(10).findFirst().get().getMessage(), containsString("is deleted"));
    }

    public static <T> Map<String, Object> convert(Class<T> cls, String... yaml) throws JsonProcessingException {
        return JacksonMapper.toMap(mapper.readValue(String.join("\n", yaml), cls));
    }
}
