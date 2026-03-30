package io.kestra.plugin.kubernetes.models;

import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class OAuthTokenProviderTest {

    @Mock
    private RunContext runContext;

    /**
     * A mock that is both a {@link Task} (abstract class) and a {@link RunnableTask} —
     * exactly the contract that {@code OAuthTokenProvider.getToken()} expects.
     */
    private Task runnableTask;

    // Minimal Output mock — toMap() returns an empty map via the default interface method.
    private Output emptyOutput;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        runnableTask = mock(Task.class, withSettings().extraInterfaces(RunnableTask.class));
        emptyOutput = mock(Output.class);
        // lenient: toMap() is only needed in tests that invoke getToken(); the defaultCacheIs5Minutes
        // test does not call getToken() at all, so this stub would be flagged as unnecessary otherwise.
        org.mockito.Mockito.lenient().when(emptyOutput.toMap()).thenReturn(Map.of());
    }

    @Test
    @SuppressWarnings("unchecked")
    void cacheHit_tokenIsReusedWithinTTL() throws Exception {
        when(((RunnableTask<Output>) runnableTask).run(runContext)).thenReturn(emptyOutput);
        when(runContext.render(eq("mytoken"), any(Map.class))).thenReturn("mytoken");

        var provider = OAuthTokenProvider.builder()
            .task(runnableTask)
            .output("mytoken")
            .cache(Duration.ofMinutes(5))
            .runContext(runContext)
            .build();

        var first = provider.getToken();
        var second = provider.getToken();

        assertThat(first, is("mytoken"));
        assertThat(second, is("mytoken"));
        // Underlying task must be called only once — second call hits the cache.
        verify((RunnableTask<Output>) runnableTask, times(1)).run(runContext);
    }

    @Test
    @SuppressWarnings("unchecked")
    void cacheMiss_tokenIsReFetchedAfterTTLExpires() throws Exception {
        when(((RunnableTask<Output>) runnableTask).run(runContext)).thenReturn(emptyOutput);
        when(runContext.render(eq("mytoken"), any(Map.class))).thenReturn("mytoken");

        var provider = OAuthTokenProvider.builder()
            .task(runnableTask)
            .output("mytoken")
            .cache(Duration.ofMillis(100)) // very short TTL
            .runContext(runContext)
            .build();

        provider.getToken();
        // Wait for the cache entry to expire.
        Thread.sleep(200);
        provider.getToken();

        // Both calls should have hit the underlying task.
        verify((RunnableTask<Output>) runnableTask, times(2)).run(runContext);
    }

    @Test
    void defaultCacheIs5Minutes() {
        var provider = OAuthTokenProvider.builder()
            .task(runnableTask)
            .output("mytoken")
            .build();

        assertThat(provider.getCache(), is(Duration.ofMinutes(5)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void zeroDurationDisablesCaching() throws Exception {
        when(((RunnableTask<Output>) runnableTask).run(runContext)).thenReturn(emptyOutput);
        when(runContext.render(eq("mytoken"), any(Map.class))).thenReturn("mytoken");

        var provider = OAuthTokenProvider.builder()
            .task(runnableTask)
            .output("mytoken")
            .cache(Duration.ZERO)
            .runContext(runContext)
            .build();

        provider.getToken();
        provider.getToken();

        // Caching is disabled — task must be called on every invocation.
        verify((RunnableTask<Output>) runnableTask, times(2)).run(runContext);
    }

    @Test
    @SuppressWarnings("unchecked")
    void negativeDurationDisablesCaching() throws Exception {
        when(((RunnableTask<Output>) runnableTask).run(runContext)).thenReturn(emptyOutput);
        when(runContext.render(eq("mytoken"), any(Map.class))).thenReturn("mytoken");

        var provider = OAuthTokenProvider.builder()
            .task(runnableTask)
            .output("mytoken")
            .cache(Duration.ofSeconds(-1))
            .runContext(runContext)
            .build();

        provider.getToken();
        provider.getToken();

        // Caching is disabled — task must be called on every invocation.
        verify((RunnableTask<Output>) runnableTask, times(2)).run(runContext);
    }
}
