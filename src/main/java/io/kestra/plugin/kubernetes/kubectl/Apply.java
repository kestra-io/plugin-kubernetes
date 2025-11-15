package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.AbstractPod;
import io.kestra.plugin.kubernetes.models.Metadata;
import io.kestra.plugin.kubernetes.services.PodService;
import io.kestra.plugin.kubernetes.services.ResourceWaitService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Apply a Kubernetes resource, using YAML.",
            full = true,
            code = """
                id: create_or_replace_deployment
                namespace: company.team

                tasks:
                  - id: apply
                    type: io.kestra.plugin.kubernetes.kubectl.Apply
                    namespace: default
                    spec: |-
                      apiVersion: apps/v1
                      kind: Deployment
                      metadata:
                        name: mypod
                """
        ),
        @Example(
            title = "Apply a Kubernetes resource, using a namespace file.",
            full = true,
            code = """
                id: create_or_replace_deployment
                namespace: company.team

                tasks:
                  - id: apply
                    type: io.kestra.plugin.kubernetes.kubectl.Apply
                    namespaceFiles:
                      enabled: true
                    namespace: default
                    spec: "{{ read('deployment.yaml') }}"
                """
        ),
        @Example(
            title = "Apply a Kubernetes custom resource definition, using YAML.",
            full = true,
            code = """
                id: k8s
                namespace: company.name

                tasks:
                  - id: apply
                    type: io.kestra.plugin.kubernetes.kubectl.Apply
                    namespace: default
                    spec: |-
                      apiVersion: apiextensions.k8s.io/v1
                      kind: CustomResourceDefinition
                      metadata:
                        name: shirts.stable.example.com
                      spec:
                        group: stable.example.com
                        scope: Namespaced
                        names:
                          plural: shirts
                          singular: shirt
                          kind: Shirt
                        versions:
                        - name: v1
                          served: true
                          storage: true
                          schema:
                            openAPIV3Schema:
                              type: object
                              properties:
                                apiVersion:
                                  type: string
                                kind:
                                  type: string
                                metadata:
                                  type: object
                                spec:
                                  type: object
                                  x-kubernetes-preserve-unknown-fields: true # Allows any fields in spec
                                  properties:
                                    # You should define your actual Shirt properties here later
                                    # For example:
                                    # color:
                                    #   type: string
                                    # size:
                                    #   type: string
                                    #   enum: ["S", "M", "L", "XL"]
                                status:
                                  type: object
                                  x-kubernetes-preserve-unknown-fields: true # Allows any fields in status
                                  properties:
                                    # Define your status properties here
                                    # message:
                                    #   type: string
                """
        ),
        @Example(
            title = "Apply a custom resource and wait for it to become ready.",
            full = true,
            code = """
                id: apply_and_wait_for_custom_resource
                namespace: company.team

                tasks:
                  - id: apply
                    type: io.kestra.plugin.kubernetes.kubectl.Apply
                    namespace: default
                    waitUntilReady: PT10M
                    spec: |-
                      apiVersion: example.com/v1
                      kind: MyResource
                      metadata:
                        name: my-resource
                      spec:
                        foo: bar
                """
        )
    }
)
@Schema(
    title = "Apply a Kubernetes resource (e.g., a Kubernetes deployment)."
)
public class Apply extends AbstractPod implements RunnableTask<Apply.Output> {

    @NotNull
    @Schema(
        title = "The Kubernetes resource spec"
    )
    private Property<String> spec;

    @Schema(
        title = "The Kubernetes namespace"
    )
    private Property<String> namespace;

    @Override
    public Apply.Output run(RunContext runContext) throws Exception {
        var namespace = runContext.render(this.namespace).as(String.class).orElseThrow();
        var rWaitUntilReady = runContext.render(this.waitUntilReady).as(Duration.class).orElse(Duration.ZERO);

        try (var client = PodService.client(runContext, this.getConnection())) {
            var resources = parseSpec(runContext.render(this.spec).as(String.class).orElseThrow());
            Logger logger = runContext.logger();
            logger.debug("Parsed resources: {}", resources);

            List<Metadata> metadataList = new ArrayList<>();
            for (var resource : resources) {
                var resourceClient = client.resource(resource).inNamespace(namespace);

                try {
                    var hasMetadata = resourceClient.unlock().serverSideApply();
                    metadataList.add(Metadata.from(hasMetadata.getMetadata()));
                    logger.info("Applied resource: {}", hasMetadata);

                    // Optionally wait for resource to become ready
                    if (!rWaitUntilReady.isZero()) {
                        var resourceMetadata = hasMetadata.getMetadata();
                        var resourceName = resourceMetadata.getName();
                        var apiVersion = hasMetadata.getApiVersion();
                        var kind = hasMetadata.getKind();

                        // Parse apiVersion to extract group and version
                        String group = "";
                        String version = apiVersion;
                        if (apiVersion != null && apiVersion.contains("/")) {
                            String[] parts = apiVersion.split("/", 2);
                            group = parts[0];
                            version = parts[1];
                        }

                        var resourceContext = new ResourceDefinitionContext.Builder()
                            .withGroup(group)
                            .withVersion(version)
                            .withKind(kind)
                            .withNamespaced(true)
                            .build();

                        logger.info("Waiting for resource '{}' to become ready (timeout: {})...", resourceName, rWaitUntilReady);
                        ResourceWaitService.waitForReady(client, resourceContext, namespace, resourceName, rWaitUntilReady, logger);
                        logger.info("Resource '{}' is ready", resourceName);
                    }
                } catch (Exception exception) {
                    logger.error("Failed to apply resource: {}", resource, exception);
                    throw new Exception("Failed to apply resource: " + resource, exception);
                }
            }

            return Output.builder()
                .metadata(metadataList)
                .build();
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The resource metadata"
        )
        private final List<Metadata> metadata;
    }
}
