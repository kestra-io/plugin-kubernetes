package io.kestra.plugin.kubernetes.runner;

import io.kestra.core.models.script.AbstractScriptRunnerTest;
import io.kestra.core.models.script.ScriptRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class KubernetesScriptRunnerTest extends AbstractScriptRunnerTest {

    @Test
    @Disabled("Test issue")
    @Override
    protected void inputAndOutputFiles() {
    }

    @Override
    protected ScriptRunner scriptRunner() {
        return KubernetesScriptRunner.builder().build();
    }
}