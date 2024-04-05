package io.kestra.plugin.kubernetes.runner;

import io.kestra.core.models.script.AbstractScriptRunnerTest;
import io.kestra.core.models.script.ScriptRunner;

class KubernetesScriptRunnerTest extends AbstractScriptRunnerTest {
    @Override
    protected ScriptRunner scriptRunner() {
        return KubernetesScriptRunner.builder().build();
    }
}