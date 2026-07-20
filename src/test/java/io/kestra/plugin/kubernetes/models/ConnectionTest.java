package io.kestra.plugin.kubernetes.models;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

// Mutates JVM-global system properties (kubernetes.master, auth.*); @Isolated keeps it from
// running alongside the concurrent kubectl integration tests, whose client build would otherwise
// read the poisoned values. See junit-platform.properties (parallel execution enabled).
@Isolated
@MicronautTest
class ConnectionTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void inheritClusterConfigSeedsUnsetFieldsFromAutoConfig() throws Exception {
        // true seeds unset fields from ambient auto-config; false (default) starts blank (prior behavior).
        // kube config + service account loading disabled so only the sys-prop master URL feeds auto-config
        // (deterministic on any machine); masterUrl nulled so the seeded value shows through.
        System.setProperty("kubernetes.auth.tryKubeConfig", "false");
        System.setProperty("kubernetes.auth.tryServiceAccount", "false");
        System.setProperty("kubernetes.master", "https://seed-from-autoconfig:6443");
        try {
            RunContext runContext = runContextFactory.of(Map.of());
            var connection = Connection.builder().masterUrl(null).build();

            assertThat(connection.toConfig(runContext, true).getMasterUrl()).contains("seed-from-autoconfig:6443");
            assertThat(connection.toConfig(runContext, false).getMasterUrl()).doesNotContain("seed-from-autoconfig");
        } finally {
            System.clearProperty("kubernetes.master");
            System.clearProperty("kubernetes.auth.tryKubeConfig");
            System.clearProperty("kubernetes.auth.tryServiceAccount");
        }
    }
}
