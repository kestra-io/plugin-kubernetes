package io.kestra.plugin.kubernetes.models;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import java.time.Duration;
import java.time.Instant;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OAuthTokenProvider implements io.fabric8.kubernetes.client.OAuthTokenProvider {
    private Task task;

    private String output;

    @Schema(
        title = "Token cache duration",
        description = """
            How long a fetched token is cached before the underlying task is called again. \
            Defaults to 5 minutes. Set to `PT0S` or a negative duration to disable caching \
            and re-fetch a token on every request.\
            """
    )
    @PluginProperty(group = "advanced")
    @Builder.Default
    private Duration cache = Duration.ofMinutes(5);

    @With
    private transient RunContext runContext;

    private transient String cachedToken;

    private transient Instant cacheExpiry;

    @Override
    @JsonIgnore
    public synchronized String getToken() {
        // Return cached token if caching is enabled and the token has not expired yet
        if (isCachingEnabled() && cachedToken != null && cacheExpiry != null && Instant.now().isBefore(cacheExpiry)) {
            return cachedToken;
        }

        try {
            RunnableTask<?> runnableTask = (RunnableTask<?>) task;
            Output run = runnableTask.run(runContext);
            var token = runContext.render(this.output, run.toMap());

            if (isCachingEnabled()) {
                cachedToken = token;
                cacheExpiry = Instant.now().plus(cache);
            }

            return token;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isCachingEnabled() {
        return cache != null && !cache.isZero() && !cache.isNegative();
    }
}
