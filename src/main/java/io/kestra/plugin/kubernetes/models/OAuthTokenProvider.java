package io.kestra.plugin.kubernetes.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OAuthTokenProvider implements io.fabric8.kubernetes.client.OAuthTokenProvider {
    private Task task;

    private String output;

    @With
    private transient RunContext runContext;

    @Override
    @JsonIgnore
    public String getToken() {
        try {
            RunnableTask<?> runnableTask = (RunnableTask<?>) task;
            Output run = runnableTask.run(runContext);
            return runContext.render(this.output, run.toMap());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
