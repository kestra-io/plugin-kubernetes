package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
	title = "Apply a Kubernetes resource"
)
@Slf4j
public class Apply extends AbstractPod implements RunnableTask<Apply.Output> {

	@NotNull
	@Schema(
		title = "The Kubernetes resource spec"
	)
	@PluginProperty(dynamic = true)
	private String spec;

	@Schema(
		title = "The Kubernetes namespace"
	)
	@PluginProperty(dynamic = true)
	private String namespace;

	@Override
	public Apply.Output run(RunContext runContext) throws Exception {
		String namespace = runContext.render(this.namespace);

		try (KubernetesClient client = PodService.client(runContext, this.getConnection())) {
			List<HasMetadata> resources = parseSpec(runContext.render(this.spec));
			log.debug("Parsed resources: {}", resources);

			List<Metadata> metadataList = new ArrayList<>();
			for (HasMetadata resource : resources) {
				Resource<HasMetadata> resourceClient = client.resource(resource).inNamespace(namespace);

				try {
					HasMetadata hasMetadata = resourceClient.unlock().serverSideApply();
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

	private List<HasMetadata> parseSpec(String spec) {
		KubernetesSerialization serialization = new KubernetesSerialization();
		Object resource = serialization.unmarshal(spec);

		List<HasMetadata> resources = new ArrayList<>();
		switch (resource) {
			case List<?> parsed -> resources.addAll((List<? extends HasMetadata>) parsed);
			case HasMetadata parsed -> resources.add(parsed);
			case KubernetesResourceList<?> parsed -> resources.addAll(parsed.getItems());
			case null, default -> throw new IllegalArgumentException("Unknown resource");
		}

		return resources;
	}

	@Getter
	@Builder
	public static class Output implements io.kestra.core.models.tasks.Output {

		@Schema(
			title = "The pod metadata."
		)
		private final List<Metadata> metadata;

	}

}
