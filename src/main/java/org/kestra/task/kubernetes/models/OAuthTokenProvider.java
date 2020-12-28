package org.kestra.task.kubernetes.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.kestra.core.models.tasks.Output;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

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
