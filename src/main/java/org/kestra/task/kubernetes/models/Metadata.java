package org.kestra.task.kubernetes.models;

import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Builder
@Getter
public class Metadata {
    private Map<String, String> annotations;
    private String clusterName;
    private String creationTimestamp;
    private Long deletionGracePeriodSeconds;
    private String deletionTimestamp;
    private List<String> finalizers;
    private String generateName;
    private Long generation;
    private Map<String, String> labels;
    private List<ManagedFieldsEntry> managedFields;
    private String name;
    private String namespace;
    private List<OwnerReference> ownerReferences;
    private String resourceVersion;
    private String selfLink;
    private String uid;

    public static Metadata fromObjectMeta(ObjectMeta objectMeta) {
        MetadataBuilder builder = Metadata.builder();

        builder.annotations(objectMeta.getAnnotations());
        builder.clusterName(objectMeta.getClusterName());
        builder.creationTimestamp(objectMeta.getCreationTimestamp());
        builder.deletionGracePeriodSeconds(objectMeta.getDeletionGracePeriodSeconds());
        builder.deletionTimestamp(objectMeta.getDeletionTimestamp());
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
