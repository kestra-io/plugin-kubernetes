package io.kestra.plugin.kubernetes.models;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;

@Builder
@Getter
public class Connection {
    @Schema(
        title = "Trust all certificates",
        description = "When true, skips TLS cert validation. Use only for testing."
    )
    private final Property<Boolean> trustCerts;

    @Schema(
        title = "Disable hostname verification",
        description = "Disables TLS hostname checks. Avoid in production clusters."
    )
    private final Property<Boolean> disableHostnameVerification;

    @Schema(
        title = "Kubernetes API URL",
        description = "API server endpoint. Default `https://kubernetes.default.svc`."
    )
    @Builder.Default
    private final Property<String> masterUrl = Property.ofValue("https://kubernetes.default.svc");

    @Schema(
        title = "API version",
        description = "API group version used by the client. Default v1."
    )
    @Builder.Default
    private final Property<String> apiVersion = Property.ofValue("v1");

    @Schema(
        title = "Default namespace",
        description = "Namespace used when resources omit a namespace."
    )
    private final Property<String> namespace;

    @Schema(
        title = "CA certificate file",
        description = "Path to a PEM CA bundle."
    )
    private final Property<String> caCertFile;

    @Schema(
        title = "CA certificate data",
        description = "Base64-encoded PEM CA bundle. Whitespace is stripped automatically."
    )
    private final Property<String> caCertData;

    @Schema(
        title = "Client certificate file"
    )
    private final Property<String> clientCertFile;

    @Schema(
        title = "Client certificate data",
        description = "Base64-encoded client cert. Whitespace is stripped automatically."
    )
    private final Property<String> clientCertData;

    @Schema(
        title = "Client key file"
    )
    private final Property<String> clientKeyFile;

    @Schema(
        title = "Client key data",
        description = "Base64-encoded client key. Whitespace is stripped automatically."
    )
    private final Property<String> clientKeyData;

    @Schema(
        title = "Client key algorithm",
        description = "Algorithm for the client key. Default RSA."
    )
    @Builder.Default
    private final Property<String> clientKeyAlgo = Property.ofValue("RSA");

    @Schema(
        title = "Client key passphrase"
    )
    private final Property<String> clientKeyPassphrase;

    @Schema(
        title = "Truststore file"
    )
    private final Property<String> trustStoreFile;

    @Schema(
        title = "Truststore passphrase"
    )
    private final Property<String> trustStorePassphrase;

    @Schema(
        title = "Keystore file"
    )
    private final Property<String> keyStoreFile;

    @Schema(
        title = "Keystore passphrase"
    )
    private final Property<String> keyStorePassphrase;

    @Schema(
        title = "OAuth token"
    )
    private final Property<String> oauthToken;

    @Schema(
        title = "OAuth token provider"
    )
    @PluginProperty
    private final OAuthTokenProvider oauthTokenProvider;

    @Schema(
        title = "Username"
    )
    private Property<String> username;

    @Schema(
        title = "Password"
    )
    private Property<String> password;

    public Config toConfig(RunContext runContext) throws IllegalVariableEvaluationException {
        ConfigBuilder builder = new ConfigBuilder(Config.empty());

        if (trustCerts != null) {
            builder.withTrustCerts(runContext.render(trustCerts).as(Boolean.class).orElseThrow());
        }

        if (disableHostnameVerification != null) {
            builder.withDisableHostnameVerification(runContext.render(disableHostnameVerification).as(Boolean.class).orElseThrow());
        }

        if (masterUrl != null) {
            builder.withMasterUrl(runContext.render(masterUrl).as(String.class).orElseThrow());
        }

        if (apiVersion != null) {
            builder.withApiVersion(runContext.render(apiVersion).as(String.class).orElseThrow());
        }

        if (namespace != null) {
            builder.withNamespace(runContext.render(namespace).as(String.class).orElseThrow());
        }

        if (caCertFile != null) {
            builder.withCaCertFile(runContext.render(caCertFile).as(String.class).orElseThrow());
        }

        if (caCertData != null) {
            builder.withCaCertData(normalizeBase64(runContext, caCertData));
        }

        if (clientCertFile != null) {
            builder.withClientCertFile(runContext.render(clientCertFile).as(String.class).orElseThrow());
        }

        if (oauthToken != null) {
            builder.withOauthToken(runContext.render(oauthToken).as(String.class).orElseThrow());
        }

        if (oauthTokenProvider != null) {
            builder.withOauthTokenProvider(oauthTokenProvider.withRunContext(runContext));
        }

        if (clientCertData != null) {
            builder.withClientCertData(normalizeBase64(runContext, clientCertData));
        }

        if (clientKeyFile != null) {
            builder.withClientKeyFile(runContext.render(clientKeyFile).as(String.class).orElseThrow());
        }

        if (clientKeyData != null) {
            builder.withClientKeyData(normalizeBase64(runContext, clientKeyData));
        }

        if (clientKeyAlgo != null) {
            builder.withClientKeyAlgo(runContext.render(clientKeyAlgo).as(String.class).orElseThrow());
        }

        if (clientKeyPassphrase != null) {
            builder.withClientKeyPassphrase(runContext.render(clientKeyPassphrase).as(String.class).orElseThrow());
        }

        if (trustStoreFile != null) {
            builder.withTrustStoreFile(runContext.render(trustStoreFile).as(String.class).orElseThrow());
        }

        if (trustStorePassphrase != null) {
            builder.withTrustStorePassphrase(runContext.render(trustStorePassphrase).as(String.class).orElseThrow());
        }

        if (keyStoreFile != null) {
            builder.withKeyStoreFile(runContext.render(keyStoreFile).as(String.class).orElseThrow());
        }

        if (keyStorePassphrase != null) {
            builder.withKeyStorePassphrase(runContext.render(keyStorePassphrase).as(String.class).orElseThrow());
        }

        if (username != null) {
            builder.withUsername(runContext.render(username).as(String.class).orElseThrow());
        }

        if (password != null) {
            builder.withPassword(runContext.render(password).as(String.class).orElseThrow());
        }

        return builder.build();
    }

    private String normalizeBase64(RunContext runContext, Property<String> prop) throws IllegalVariableEvaluationException {
        return runContext.render(prop)
            .as(String.class)
            .map(s -> s.replaceAll("\\s", ""))
            .orElseThrow();
    }
}
