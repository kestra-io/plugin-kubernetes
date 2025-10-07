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
            title = "Name of the resource"
    )
    private final String name;

    @Schema(
            title = "Namespace of the resource"
    )
    private final String namespace;

    @Schema(
            title = "Name of the current cluster"
    )
    private final String clusterName;

    @Schema(
            title = "List of all annotations of the resource"
    )
    private final Map<String, String> annotations;

    @Schema(
            title = "List of labels"
    )
    private final Map<String, String> labels;

    @Schema(
            title = "Creation datetime"
    )
    private final Instant creationTimestamp;

    @Schema(
            title = "Deletetion grace period in seconds"
    )
    private final Long deletionGracePeriodSeconds;

    @Schema(
            title = "Deletion datetime"
    )
    private final Instant deletionTimestamp;

    @Schema(
            title = "List of finalizers"
    )
    private final List<String> finalizers;

    @Schema(
            title = "Generate name of the resource"
    )
    private final String generateName;

    @Schema(
            title = "Generation"
    )
    private final Long generation;

    @Schema(
            title = "List of managed fields"
    )
    private final List<ManagedFieldsEntry> managedFields;

    @Schema(
            title = "List of owner reference"
    )
    private final List<OwnerReference> ownerReferences;

    @Schema(
            title = "Resource version"
    )
    private final String resourceVersion;

    @Schema(
            title = "Direct link to the API of this resource"
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
