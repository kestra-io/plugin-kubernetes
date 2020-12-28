package org.kestra.task.kubernetes.models;

import io.fabric8.kubernetes.api.model.batch.JobCondition;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Builder
@Getter
public class JobStatus {
    private final Integer active;
    private final Instant completionTime;
    private final List<JobCondition> conditions;
    private final Integer failed;
    private final Instant startTime;
    private final Integer succeeded;
    private final Map<String, Object> additionalProperties;

    public static JobStatus from(io.fabric8.kubernetes.api.model.batch.JobStatus jobStatus) {
        JobStatusBuilder builder = JobStatus.builder();

        builder.active(jobStatus.getActive());
        builder.completionTime(jobStatus.getCompletionTime() != null ? Instant.parse(jobStatus.getCompletionTime()) : null);
        builder.conditions(jobStatus.getConditions());
        builder.failed(jobStatus.getFailed());
        builder.startTime(jobStatus.getStartTime() != null ? Instant.parse(jobStatus.getStartTime()) : null);
        builder.succeeded(jobStatus.getSucceeded());
        builder.additionalProperties(jobStatus.getAdditionalProperties());

        return builder.build();
    }
}
