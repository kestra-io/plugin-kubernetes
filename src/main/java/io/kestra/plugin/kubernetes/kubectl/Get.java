package io.kestra.plugin.kubernetes.kubectl;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.kubernetes.AbstractPod;
import io.kestra.plugin.kubernetes.models.Metadata;
import io.kestra.plugin.kubernetes.models.ResourceStatus;
import io.kestra.plugin.kubernetes.services.PodService;
import io.kestra.plugin.kubernetes.services.ResourceWaitService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.kestra.core.models.tasks.common.FetchType.NONE;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Get all pods from Kubernetes with a service account.",
            full = true,
            code = """
                id: get_all_pods
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.kubernetes.kubectl.Get
                    namespace: default
                    resourceType: pods
                    fetchType: FETCH
                    connection:
                      masterUrl: "{{ secret('K8S_MASTER_URL') }}"
                      oauthToken: "{{ secret('K8S_TOKEN') }}"
                """
        ),
        @Example(
            title = "Get one deployment named my-deployment from Kubernetes.",
            full = true,
            code = """
                id: get_one_deployment
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.kubernetes.kubectl.Get
                    namespace: default
                    resourceType: deployments
                    resourcesNames:
                      - my-deployment
                    fetchType: FETCH_ONE
                """
        ),
        @Example(
            title = "Get two deployments named my-deployment and my-deployment-2 from Kubernetes and store them in the internal storage.",
            full = true,
            code = """
                id: get_two_deployments
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.kubernetes.kubectl.Get
                    namespace: default
                    resourceType: deployments
                    resourcesNames:
                      - my-deployment
                      - my-deployment-2
                    fetchType: STORE
                """
        ),
        @Example(
            title = "Get one custom resource named Shirt from Kubernetes.",
            full = true,
            code = """
                id: get_one_custom_resource
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.kubernetes.kubectl.Get
                    namespace: default
                    resourceType: shirts # could be Shirt
                    apiGroup: stable.example.com
                    apiVersion: v1
                    fetchType: FETCH_ONE
                """
        ),
        @Example(
            title = "Get a custom resource and wait for it to become ready.",
            full = true,
            code = """
                id: get_and_wait_for_custom_resource
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.kubernetes.kubectl.Get
                    namespace: default
                    resourceType: myresource
                    apiGroup: example.com
                    apiVersion: v1
                    resourcesNames:
                      - my-resource
                    fetchType: FETCH_ONE
                    waitUntilReady: PT10M
                """
        ),
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "records",
            description = "The number of rows fetch."
        ),
    }
)
@Schema(
    title = "Get one or many Kubernetes resources of a kind."
)
public class Get extends AbstractPod implements RunnableTask<Get.Output> {

    @Schema(
        title = "The Kubernetes resource type (= kind) (e.g., pod, service)"
    )
    @NotNull
    private Property<String> resourceType;

    @Schema(
        title = "The Kubernetes resources names"
    )
    private Property<List<String>> resourcesNames;

    @Schema(
        title = "The Kubernetes resource apiGroup"
    )
    private Property<String> apiGroup;

    @Schema(
        title = "The Kubernetes resource apiVersion"
    )
    private Property<String> apiVersion;

    @NotNull
    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(NONE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rNamespace = runContext.render(this.namespace).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("namespace must be provided and rendered."));
        var rResourceType = runContext.render(this.resourceType).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("resourceType must be provided and rendered."));
        var rResourcesNames = runContext.render(this.resourcesNames).asList(String.class);
        var rApiGroup = runContext.render(this.apiGroup).as(String.class).orElse("");
        var rApiVersion = runContext.render(this.apiVersion).as(String.class).orElse("v1");
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(NONE);
        var rWaitUntilReady = runContext.render(this.waitUntilReady).as(Duration.class).orElse(Duration.ZERO);

        List<Metadata> metadataList = new ArrayList<>();
        List<ResourceStatus> statusList = new ArrayList<>();
        Logger logger = runContext.logger();

        try (var client = PodService.client(runContext, this.getConnection())) {

            var resourceDefinitionContext = new ResourceDefinitionContext.Builder()
                // See: https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-uris
                .withGroup(rApiGroup)
                .withVersion(rApiVersion)
                .withKind(rResourceType)
                .withNamespaced(true) // Assuming resources are namespaced as we take namespace input
                .build();

            if (rResourcesNames.isEmpty()) {
                logger.debug("Fetching all resources of kind '{}' in namespace '{}'", rResourceType, rNamespace);
                var resources = client.genericKubernetesResources(resourceDefinitionContext)
                    .inNamespace(rNamespace)
                    .list()
                    .getItems();

                for (var resource : resources) {
                    if (resource != null && resource.getMetadata() != null) {
                        metadataList.add(Metadata.from(resource.getMetadata()));
                        statusList.add(ResourceStatus.from(resource));
                    }
                }
                logger.info("Fetched {} resource(s) of kind '{}' in namespace '{}'", metadataList.size(), rResourceType, rNamespace);

            } else {
                rResourcesNames.forEach(name -> {
                        logger.debug("Fetching resource of kind '{}' with name '{}' in namespace '{}'",
                            rResourceType, name, rNamespace);

                        var resource = client.genericKubernetesResources(resourceDefinitionContext)
                            .inNamespace(rNamespace)
                            .withName(name)
                            .get();

                        if (resource != null && resource.getMetadata() != null) {
                            // Optionally wait for resource to become ready
                            if (!rWaitUntilReady.isZero()) {
                                runContext.logger().info("Waiting for resource '{}' to become ready (timeout: {})...", name, rWaitUntilReady);
                                resource = ResourceWaitService.waitForReady(
                                    client,
                                    resourceDefinitionContext,
                                    rNamespace,
                                    name,
                                    rWaitUntilReady,
                                    runContext.logger()
                                );
                                runContext.logger().info("Resource '{}' is ready", name);
                            }

                            metadataList.add(Metadata.from(resource.getMetadata()));
                            statusList.add(ResourceStatus.from(resource));
                            logger.info("Fetched resource of kind '{}' with name '{}' in namespace '{}'",
                                rResourceType, name, rNamespace);
                        } else {
                            logger.warn("Resource of kind '{}' with name '{}' not found in namespace '{}'",
                                rResourceType, name, rNamespace);
                        }
                    }
                );
            }

        } catch (KubernetesClientException e) {
            logger.error("Kubernetes API error while fetching kind '{}' in namespace '{}': {}", rResourceType, rNamespace, e.getMessage(), e);
            throw new Exception("Failed to interact with Kubernetes API: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            logger.error("Configuration error: {}", e.getMessage(), e);
            throw e;
        }

        Output output;

        int fetchedItemsCount = metadataList.size();
        switch (rFetchType) {
            case NONE:
                output = Output.builder().build();
                runContext.metric(Counter.of("fetch.size", 0, "fetch", "false", "store", "false"));
                break;
            case FETCH:
                output = Output.builder()
                    .metadataItems(metadataList)
                    .statusItems(statusList)
                    .size(fetchedItemsCount)
                    .build();
                runContext.metric(Counter.of("fetch.size", fetchedItemsCount, "fetch", "true", "store", "false"));
                break;
            case FETCH_ONE:
                output = Output.builder()
                    .metadataItem(metadataList.isEmpty() ? null : metadataList.getFirst())
                    .statusItem(statusList.isEmpty() ? null : statusList.getFirst())
                    .size(fetchedItemsCount)
                    .build();
                runContext.metric(Counter.of("fetch.size", fetchedItemsCount, "fetch", "true", "store", "false"));
                break;
            case STORE:
                var result = storeResult(metadataList, statusList, runContext);
                int storedItemsCount = result.getValue().intValue();
                output = Output.builder()
                    .uri(result.getKey())
                    .size(storedItemsCount)
                    .build();
                runContext.metric(Counter.of("fetch.size", storedItemsCount, "fetch", "false", "store", "true"));
                break;
            default:
                throw new IllegalStateException("Unexpected fetchType value: " + fetchType);
        }

        return output;
    }

    @Builder
    public record ResourceInfo(Metadata metadata, ResourceStatus status) {
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The metadata for multiple resources",
            description = "Only available when `fetchType` is set to `FETCH`."
        )
        private final List<Metadata> metadataItems;

        @Schema(
            title = "The metadata for a single resource",
            description = "Only available when `fetchType` is set to `FETCH_ONE`."
        )
        private final Metadata metadataItem;

        @Schema(
            title = "The status for multiple resources",
            description = "Only available when `fetchType` is set to `FETCH`."
        )
        private final List<ResourceStatus> statusItems;

        @Schema(
            title = "The status for a single resource",
            description = "Only available when `fetchType` is set to `FETCH_ONE`."
        )
        private final ResourceStatus statusItem;

        @Schema(
            title = "The output files URI in Kestra's internal storage",
            description = "Only available when `fetchType` is set to `STORE`."
        )
        private final URI uri;

        @Schema(
            title = "The count of the fetched or stored resources"
        )
        private Integer size;
    }

    private Map.Entry<URI, Long> storeResult(List<Metadata> metadataList, List<ResourceStatus> statusList, RunContext runContext) throws IOException {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        // Combine metadata and status into ResourceInfo objects
        List<ResourceInfo> resourceInfoList = new ArrayList<>();
        for (int i = 0; i < metadataList.size(); i++) {
            resourceInfoList.add(ResourceInfo.builder()
                .metadata(metadataList.get(i))
                .status(i < statusList.size() ? statusList.get(i) : null)
                .build());
        }

        try (
            var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            var flowable = Flux
                .create(
                    s -> {
                        resourceInfoList.forEach(s::next);
                        s.complete();
                    },
                    FluxSink.OverflowStrategy.BUFFER
                );

            var count = FileSerde.writeAll(output, flowable);
            var lineCount = count.block();

            output.flush();

            return new AbstractMap.SimpleEntry<>(
                runContext.storage().putFile(tempFile),
                lineCount
            );
        }
    }
}
