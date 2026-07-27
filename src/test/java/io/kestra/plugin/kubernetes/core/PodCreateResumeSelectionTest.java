package io.kestra.plugin.kubernetes.core;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

class PodCreateResumeSelectionTest {

    private Pod pod(String phase, boolean terminating) {
        var builder = new PodBuilder()
            .withNewMetadata()
            .withName("selection-test");
        if (terminating) {
            builder = builder.withDeletionTimestamp("2026-07-23T00:00:00Z");
        }
        var metadataDone = builder.endMetadata();
        if (phase == null) {
            return metadataDone.build();
        }
        return metadataDone
            .withNewStatus()
            .withPhase(phase)
            .endStatus()
            .build();
    }

    @Test
    void pendingRunningAndSucceededAreResumable() {
        assertThat(PodCreate.isResumable(pod("Pending", false)), is(true));
        assertThat(PodCreate.isResumable(pod("Running", false)), is(true));
        assertThat(PodCreate.isResumable(pod("Succeeded", false)), is(true));
    }

    @Test
    void failedUnknownAndMissingStatusAreNotResumable() {
        assertThat(PodCreate.isResumable(pod("Failed", false)), is(false));
        assertThat(PodCreate.isResumable(pod("Unknown", false)), is(false));
        assertThat(PodCreate.isResumable(pod(null, false)), is(false));
    }

    @Test
    void terminatingPodIsNeitherResumableNorActive() {
        // A deleted pod keeps its phase until finalized - deletionTimestamp is the only signal
        assertThat(PodCreate.isResumable(pod("Running", true)), is(false));
        assertThat(PodCreate.isActive(pod("Running", true)), is(false));
    }

    @Test
    void terminalPhasesAreNotActive() {
        assertThat(PodCreate.isActive(pod("Succeeded", false)), is(false));
        assertThat(PodCreate.isActive(pod("Failed", false)), is(false));
    }

    @Test
    void nonTerminalAndUnknownPhasesAreActive() {
        assertThat(PodCreate.isActive(pod("Pending", false)), is(true));
        assertThat(PodCreate.isActive(pod("Running", false)), is(true));
        assertThat(PodCreate.isActive(pod("Unknown", false)), is(true));
        assertThat(PodCreate.isActive(pod(null, false)), is(true));
    }

    @Test
    void succeededOutranksRunningOutranksPending() {
        int succeeded = PodCreate.resumePriority(pod("Succeeded", false));
        int running = PodCreate.resumePriority(pod("Running", false));
        int pending = PodCreate.resumePriority(pod("Pending", false));

        assertThat(succeeded, is(greaterThan(running)));
        assertThat(running, is(greaterThan(pending)));
    }

    @Test
    void missingStatusRanksLowest() {
        assertThat(PodCreate.resumePriority(pod(null, false)), is(0));
    }

    @Test
    void succeededPodIsOnlyResumableWithinTheSameAttempt() {
        assertThat(PodCreate.isResumableForAttempt(podWithAttempt("Succeeded", "1"), "1"), is(true));
        assertThat(PodCreate.isResumableForAttempt(podWithAttempt("Succeeded", "0"), "1"), is(false));
        // Live pods stay resumable across attempts - that is the issue #249 reconnect
        assertThat(PodCreate.isResumableForAttempt(podWithAttempt("Running", "0"), "1"), is(true));
        assertThat(PodCreate.isResumableForAttempt(podWithAttempt("Pending", "0"), "1"), is(true));
    }

    private Pod podWithAttempt(String phase, String attempt) {
        return new PodBuilder()
            .withNewMetadata()
            .withName("selection-test")
            .addToLabels("kestra.io/taskrun-attempt", attempt)
            .endMetadata()
            .withNewStatus()
            .withPhase(phase)
            .endStatus()
            .build();
    }
}
