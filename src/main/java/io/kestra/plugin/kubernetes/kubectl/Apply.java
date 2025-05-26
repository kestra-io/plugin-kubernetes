package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kubernetes.AbstractPod;
import io.kestra.plugin.kubernetes.models.Metadata;
import io.kestra.plugin.kubernetes.services.PodService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

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
        )
    }
)
@Schema(
    title = "Apply a Kubernetes resource (e.g., a Kubernetes deployment)."
)
@Slf4j
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

        try (var client = PodService.client(runContext, this.getConnection())) {
            var resources = parseSpec(runContext.render(this.spec).as(String.class).orElseThrow());
            log.debug("Parsed resources: {}", resources);

            List<Metadata> metadataList = new ArrayList<>();
            for (var resource : resources) {
                var resourceClient = client.resource(resource).inNamespace(namespace);

                try {
                    var hasMetadata = resourceClient.unlock().serverSideApply();
                    metadataList.add(Metadata.from(hasMetadata.getMetadata()));
                    log.info("Applied resource: {}", hasMetadata);
                } catch (Exception exception) {
                    log.error("Failed to apply resource: {}", resource, exception);
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
            title = "The resource metadata."
        )
        private final List<Metadata> metadata;
    }
}
