package io.kestra.plugin.kubernetes.runner;

import io.kestra.core.models.tasks.runners.AbstractTaskRunnerTest;
import io.kestra.core.models.tasks.runners.TaskRunner;

class KubernetesTest extends AbstractTaskRunnerTest {
    @Override
    protected TaskRunner taskRunner() {
        return Kubernetes.builder().build();
    }
}