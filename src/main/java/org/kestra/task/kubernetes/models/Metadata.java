package org.kestra.task.kubernetes.models;

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
            title = "Generated Uid of this resource"
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
            title = "Deletetion datetime"
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
            title = "Direct link on the api of this resource"
    )
    private final String selfLink;


    public static Metadata fromObjectMeta(ObjectMeta objectMeta) {
        MetadataBuilder builder = Metadata.builder();

        builder.annotations(objectMeta.getAnnotations());
        builder.clusterName(objectMeta.getClusterName());
        builder.creationTimestamp(Instant.parse(objectMeta.getCreationTimestamp()));
        builder.deletionGracePeriodSeconds(objectMeta.getDeletionGracePeriodSeconds());
        builder.deletionTimestamp(objectMeta.getDeletionTimestamp() != null ? Instant.parse(objectMeta.getDeletionTimestamp()) : null);
        builder.finalizers(objectMeta.getFinalizers());
        builder.generateName(objectMeta.getGenerateName());
        builder.generation(objectMeta.getGeneration());
        builder.labels(objectMeta.getLabels());
        builder.managedFields(objectMeta.getManagedFields());
        builder.name(objectMeta.getName());
        builder.namespace(objectMeta.getNamespace());
        builder.ownerReferences(objectMeta.getOwnerReferences());
        builder.resourceVersion(objectMeta.getResourceVersion());
        builder.selfLink(objectMeta.getSelfLink());
        builder.uid(objectMeta.getUid());

        return builder.build();
    }
}
