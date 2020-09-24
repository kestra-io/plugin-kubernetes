package org.kestra.task.kubernetes.models;

import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import lombok.Builder;
import lombok.Getter;
import org.kestra.core.models.annotations.OutputProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Builder
@Getter
public class Metadata {
    @OutputProperty(
        description = "Generated Uid of this resource"
    )
    private String uid;

    @OutputProperty(
        description = "Name of the resource"
    )
    private final String name;

    @OutputProperty(
        description = "Namespace of the resource"
    )
    private final String namespace;

    @OutputProperty(
        description = "Name of the current cluster"
    )
    private final String clusterName;

    @OutputProperty(
        description = "List of all annotations of the resource"
    )
    private final Map<String, String> annotations;

    @OutputProperty(
        description = "List of labels"
    )
    private final Map<String, String> labels;

    @OutputProperty(
        description = "Creation datetime"
    )
    private final Instant creationTimestamp;

    @OutputProperty(
        description = "Deletetion grace period in seconds"
    )
    private final Long deletionGracePeriodSeconds;

    @OutputProperty(
        description = "Deletetion datetime"
    )
    private final Instant deletionTimestamp;

    @OutputProperty(
        description = "List of finalizers"
    )
    private final List<String> finalizers;

    @OutputProperty(
        description = "Generate name of the resource"
    )
    private final String generateName;

    @OutputProperty(
        description = "Generation"
    )
    private final Long generation;

    @OutputProperty(
        description = "List of managed fields"
    )
    private final List<ManagedFieldsEntry> managedFields;

    @OutputProperty(
        description = "List of owner reference"
    )
    private final List<OwnerReference> ownerReferences;

    @OutputProperty(
        description = "Resource version"
    )
    private final String resourceVersion;

    @OutputProperty(
        description = "Direct link on the api of this resource"
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
