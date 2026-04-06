package io.kestra.plugin.kubernetes.models;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import io.kestra.core.models.annotations.PluginProperty;

@Builder
@Getter
public class Metadata {
    @Schema(
        title = "Generated UUID of this resource"
    )
    @PluginProperty(group = "advanced")
    private String uid;

    @Schema(
        title = "Resource name"
    )
    @PluginProperty(group = "advanced")
    private final String name;

    @Schema(
        title = "Resource namespace"
    )
    @PluginProperty(group = "connection")
    private final String namespace;

    @Schema(
        title = "Cluster name"
    )
    @PluginProperty(group = "advanced")
    private final String clusterName;

    @Schema(
        title = "Resource annotations"
    )
    @PluginProperty(group = "advanced")
    private final Map<String, String> annotations;

    @Schema(
        title = "Resource labels"
    )
    @PluginProperty(group = "advanced")
    private final Map<String, String> labels;

    @Schema(
        title = "Creation timestamp"
    )
    @PluginProperty(group = "advanced")
    private final Instant creationTimestamp;

    @Schema(
        title = "Deletion grace period in seconds"
    )
    @PluginProperty(group = "advanced")
    private final Long deletionGracePeriodSeconds;

    @Schema(
        title = "Deletion timestamp"
    )
    @PluginProperty(group = "advanced")
    private final Instant deletionTimestamp;

    @Schema(
        title = "Finalizers"
    )
    @PluginProperty(group = "advanced")
    private final List<String> finalizers;

    @Schema(
        title = "Generated name prefix"
    )
    @PluginProperty(group = "advanced")
    private final String generateName;

    @Schema(
        title = "Generation"
    )
    @PluginProperty(group = "advanced")
    private final Long generation;

    @Schema(
        title = "Managed fields"
    )
    @PluginProperty(group = "advanced")
    private final List<ManagedFieldsEntry> managedFields;

    @Schema(
        title = "Owner references"
    )
    @PluginProperty(group = "advanced")
    private final List<OwnerReference> ownerReferences;

    @Schema(
        title = "Resource version"
    )
    @PluginProperty(group = "advanced")
    private final String resourceVersion;

    @Schema(
        title = "Self link"
    )
    @PluginProperty(group = "advanced")
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
