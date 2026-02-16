package io.kestra.plugin.kubernetes.models;

import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Builder
@Getter
public class Metadata {
    @Schema(
            title = "Generated UUID of this resource"
    )
    private String uid;

    @Schema(
            title = "Resource name"
    )
    private final String name;

    @Schema(
            title = "Resource namespace"
    )
    private final String namespace;

    @Schema(
            title = "Cluster name"
    )
    private final String clusterName;

    @Schema(
            title = "Resource annotations"
    )
    private final Map<String, String> annotations;

    @Schema(
            title = "Resource labels"
    )
    private final Map<String, String> labels;

    @Schema(
            title = "Creation timestamp"
    )
    private final Instant creationTimestamp;

    @Schema(
            title = "Deletion grace period in seconds"
    )
    private final Long deletionGracePeriodSeconds;

    @Schema(
            title = "Deletion timestamp"
    )
    private final Instant deletionTimestamp;

    @Schema(
            title = "Finalizers"
    )
    private final List<String> finalizers;

    @Schema(
            title = "Generated name prefix"
    )
    private final String generateName;

    @Schema(
            title = "Generation"
    )
    private final Long generation;

    @Schema(
            title = "Managed fields"
    )
    private final List<ManagedFieldsEntry> managedFields;

    @Schema(
            title = "Owner references"
    )
    private final List<OwnerReference> ownerReferences;

    @Schema(
            title = "Resource version"
    )
    private final String resourceVersion;

    @Schema(
            title = "Self link"
    )
    private final String selfLink;


    public static Metadata from(ObjectMeta meta) {
        MetadataBuilder builder = Metadata.builder();

        builder.annotations(meta.getAnnotations());
        builder.clusterName(meta.getName());
        builder.creationTimestamp(Instant.parse(meta.getCreationTimestamp()));
        builder.deletionGracePeriodSeconds(meta.getDeletionGracePeriodSeconds());
        builder.deletionTimestamp(meta.getDeletionTimestamp() != null ? Instant.parse(meta.getDeletionTimestamp()) : null);
        builder.finalizers(meta.getFinalizers());
        builder.generateName(meta.getGenerateName());
        builder.generation(meta.getGeneration());
        builder.labels(meta.getLabels());
        builder.managedFields(meta.getManagedFields());
        builder.name(meta.getName());
        builder.namespace(meta.getNamespace());
        builder.ownerReferences(meta.getOwnerReferences());
        builder.resourceVersion(meta.getResourceVersion());
        builder.selfLink(meta.getSelfLink());
        builder.uid(meta.getUid());

        return builder.build();
    }
}
