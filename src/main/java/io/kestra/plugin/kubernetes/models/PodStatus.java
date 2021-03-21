package io.kestra.plugin.kubernetes.models;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodIP;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Builder
@Getter
public class PodStatus {
    private final List<PodCondition> conditions;
    private final List<ContainerStatus> containerStatuses;
    private final List<ContainerStatus> ephemeralContainerStatuses;
    private final String hostIP;
    private final List<ContainerStatus> initContainerStatuses;
    private final String message;
    private final String nominatedNodeName;
    private final String phase;
    private final String podIP;
    private final List<PodIP> podIPs;
    private final String qosClass;
    private final String reason;
    private final Instant startTime;
    private final Map<String, Object> additionalProperties;

    public static PodStatus from(io.fabric8.kubernetes.api.model.PodStatus podStatus) {
        PodStatusBuilder builder = PodStatus.builder();

        builder.conditions(podStatus.getConditions());
        builder.containerStatuses(podStatus.getContainerStatuses());
        builder.ephemeralContainerStatuses(podStatus.getEphemeralContainerStatuses());
        builder.hostIP(podStatus.getHostIP());
        builder.initContainerStatuses(podStatus.getInitContainerStatuses());
        builder.message(podStatus.getMessage());
        builder.nominatedNodeName(podStatus.getNominatedNodeName());
        builder.phase(podStatus.getPhase());
        builder.podIP(podStatus.getPodIP());
        builder.podIPs(podStatus.getPodIPs());
        builder.qosClass(podStatus.getQosClass());
        builder.reason(podStatus.getReason());
        builder.startTime(podStatus.getStartTime() != null ? Instant.parse(podStatus.getStartTime()) : null);
        builder.additionalProperties(podStatus.getAdditionalProperties());

        return builder.build();
    }
}
