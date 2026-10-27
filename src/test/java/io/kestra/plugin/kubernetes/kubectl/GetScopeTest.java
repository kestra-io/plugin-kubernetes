package io.kestra.plugin.kubernetes.kubectl;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.sun.net.httpserver.HttpServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kubernetes.shared.models.Connection;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class GetScopeTest {
    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer api;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private int responseCode;

    @BeforeEach
    void startApi() throws Exception {
        responseCode = 200;
        api = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        api.createContext("/", exchange -> {
            var path = exchange.getRequestURI().getPath();
            requests.add(exchange.getRequestMethod() + " " + path);
            var resource = """
                {"apiVersion":"v1","kind":"Node","metadata":{"name":"retained","uid":"node-id","creationTimestamp":"2026-01-01T00:00:00Z"},
                 "status":{"conditions":[{"type":"Ready","status":"True"}]}}
                """;
            var body = path.endsWith("/retained")
                ? resource
                : "{\"apiVersion\":\"v1\",\"kind\":\"List\",\"metadata\":{\"resourceVersion\":\"1\"},\"items\":[" + resource + "]}";
            if (exchange.getRequestURI().getQuery() != null && exchange.getRequestURI().getQuery().contains("watch=true")) {
                body = "{\"type\":\"ADDED\",\"object\":" + resource + "}\n";
            }
            if (responseCode != 200) {
                body = "{\"apiVersion\":\"v1\",\"kind\":\"Status\",\"status\":\"Failure\",\"reason\":\"Forbidden\",\"message\":\"Denied\",\"code\":403}";
            }
            var bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(responseCode, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        api.start();
    }

    @AfterEach
    void stopApi() {
        api.stop(0);
    }

    private Get.GetBuilder<?, ?> task() {
        return Get.builder()
            .id(IdUtils.create())
            .type(Get.class.getName())
            .connection(Connection.builder()
                .masterUrl(Property.ofValue("http://127.0.0.1:" + api.getAddress().getPort()))
                .build())
            .namespace(Property.ofValue(""))
            .resourceType(Property.ofValue("persistentvolumes"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE));
    }

    @Test
    void shouldReadNamedClusterResourcesWithoutNamespacePaths() throws Exception {
        for (var resourceType : List.of("persistentvolumes", "nodes", "storageclasses", "widgets")) {
            var builder = task()
                .resourceType(Property.ofValue(resourceType))
                .resourcesNames(Property.ofValue(List.of("retained")));
            var prefix = "/api/v1/";
            if (resourceType.equals("storageclasses") || resourceType.equals("widgets")) {
                var group = resourceType.equals("storageclasses") ? "storage.k8s.io" : "example.com";
                builder.apiGroup(Property.ofValue(group));
                prefix = "/apis/" + group + "/v1/";
            }

            var output = builder.build().run(runContextFactory.of());

            assertThat(output.getMetadataItem().getName(), is("retained"));
            assertThat(output.getSize(), is(1));
            assertThat(requests, hasItem("GET " + prefix + resourceType + "/retained"));
        }
        assertThat(requests.stream().anyMatch(path -> path.contains("/namespaces/")), is(false));
    }

    @Test
    void shouldListClusterResourcesAndReturnMetadata() throws Exception {
        var output = task().fetchType(Property.ofValue(FetchType.FETCH)).build().run(runContextFactory.of());

        assertThat(output.getMetadataItems().getFirst().getName(), is("retained"));
        assertThat(output.getStatusItems(), hasSize(1));
        assertThat(requests, hasItem("GET /api/v1/persistentvolumes"));
    }

    @Test
    void shouldStoreClusterResourcesInInternalStorage() throws Exception {
        var runContext = runContextFactory.of();
        var output = task().fetchType(Property.ofValue(FetchType.STORE)).build().run(runContext);

        assertThat(output.getSize(), is(1));
        try (var data = runContext.storage().getFile(output.getUri())) {
            assertThat(new String(data.readAllBytes(), StandardCharsets.UTF_8), containsString("retained"));
        }
    }

    @Test
    void shouldWaitForReadyOnClusterScopedResource() throws Exception {
        var output = task()
            .resourceType(Property.ofValue("nodes"))
            .resourcesNames(Property.ofValue(List.of("retained")))
            .waitUntilReady(Property.ofValue(Duration.ofSeconds(2)))
            .build().run(runContextFactory.of());

        assertThat(output.getMetadataItem().getName(), is("retained"));
        assertThat(requests.stream().anyMatch(path -> path.contains("/namespaces/")), is(false));
    }

    @Test
    void shouldKeepDefaultAndRenderedNamespacesScoped() throws Exception {
        var runContext = runContextFactory.of(Map.of("vars", Map.of("namespace", "team")));
        task().namespace(Property.ofExpression("{{ vars.namespace }}")).build().run(runContext);
        task().namespace(Property.ofValue("default")).build().run(runContext);

        assertThat(requests, hasItems("GET /api/v1/namespaces/team/persistentvolumes", "GET /api/v1/namespaces/default/persistentvolumes"));
    }

    @Test
    void shouldPropagateClusterReadAuthorizationFailures() {
        responseCode = 403;
        var error = assertThrows(Exception.class, () -> task().build().run(runContextFactory.of()));

        assertThat(error.getCause(), instanceOf(KubernetesClientException.class));
        assertThat(((KubernetesClientException) error.getCause()).getCode(), is(403));
    }
}
