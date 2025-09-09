package io.kestra.plugin.kubernetes.models;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.kestra.core.models.property.Property;

import java.io.IOException;

public class PropertyResourceRequirementsDeserializer extends JsonDeserializer<Property<ResourceRequirements>> {
    @Override
    public Property<ResourceRequirements> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        ResourceRequirements value = p.getCodec().treeToValue(node, ResourceRequirements.class);
        return Property.ofValue(value);
    }
}
