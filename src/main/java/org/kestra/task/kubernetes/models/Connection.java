package org.kestra.task.kubernetes.models;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.runners.RunContext;

@Builder
@Getter
public class Connection {
    @Schema(
        title = "Trust all certificates"
    )
    private final Boolean trustCerts;

    @Schema(
        title = "Disable hostname verification"
    )
    private final Boolean disableHostnameVerification;

    @Schema(
        title = "The url to kubernetes API"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String masterUrl = "https://kubernetes.default.svc";

    @Schema(
        title = "The api version of API to use"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String apiVersion = "v1";

    @Schema(
        title = "The namespace used"
    )
    @PluginProperty(dynamic = true)
    private final String namespace;

    @Schema(
        title = "CA certificate as file path"
    )
    @PluginProperty(dynamic = true)
    private final String caCertFile;

    @Schema(
        title = "CA certificate as data ("
    )
    @PluginProperty(dynamic = true)
    private final String caCertData;

    @Schema(
        title = "Client certificate as file path"
    )
    @PluginProperty(dynamic = true)
    private final String clientCertFile;

    @Schema(
        title = "Client certificate as data"
    )
    @PluginProperty(dynamic = true)
    private final String clientCertData;

    @Schema(
        title = "Client Key as file path"
    )
    @PluginProperty(dynamic = true)
    private final String clientKeyFile;

    @Schema(
        title = "Client Key as data"
    )
    @PluginProperty(dynamic = true)
    private final String clientKeyData;

    @Schema(
        title = "Client key encryption algorithm",
        description = "default is RSA"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String clientKeyAlgo = "RSA";

    @Schema(
        title = "Client key passphrase"
    )
    @PluginProperty(dynamic = true)
    private final String clientKeyPassphrase;

    @Schema(
        title = "Truststore file"
    )
    @PluginProperty(dynamic = true)
    private final String trustStoreFile;

    @Schema(
        title = "Truststore passphrase"
    )
    @PluginProperty(dynamic = true)
    private final String trustStorePassphrase;

    @Schema(
        title = "Key store file"
    )
    @PluginProperty(dynamic = true)
    private final String keyStoreFile;

    @Schema(
        title = "Key store passphrase"
    )
    @PluginProperty(dynamic = true)
    private final String keyStorePassphrase;

    @Schema(
        title = "Oauth token"
    )
    @PluginProperty(dynamic = true)
    private final String oauthToken;

    public Config toConfig(RunContext runContext) throws IllegalVariableEvaluationException {
        ConfigBuilder builder = new ConfigBuilder();

        if (trustCerts != null) {
            builder.withTrustCerts(trustCerts);
        }

        if (disableHostnameVerification != null) {
            builder.withDisableHostnameVerification(disableHostnameVerification);
        }

        if (masterUrl != null) {
            builder.withMasterUrl(runContext.render(masterUrl));
        }

        if (apiVersion != null) {
            builder.withApiVersion(runContext.render(apiVersion));
        }

        if (namespace != null) {
            builder.withNamespace(runContext.render(namespace));
        }

        if (caCertFile != null) {
            builder.withCaCertFile(runContext.render(caCertFile));
        }

        if (caCertData != null) {
            builder.withCaCertData(runContext.render(caCertData));
        }

        if (clientCertFile != null) {
            builder.withClientCertFile(runContext.render(clientCertFile));
        }

        if (oauthToken != null) {
            builder.withOauthToken(runContext.render(oauthToken));
        }

        if (clientCertData != null) {
            builder.withClientCertData(runContext.render(clientCertData));
        }

        if (clientKeyFile != null) {
            builder.withClientKeyFile(runContext.render(clientKeyFile));
        }

        if (clientKeyData != null) {
            builder.withClientKeyData(runContext.render(clientKeyData));
        }

        if (clientKeyAlgo != null) {
            builder.withClientKeyAlgo(runContext.render(clientKeyAlgo));
        }

        if (clientKeyPassphrase != null) {
            builder.withClientKeyPassphrase(runContext.render(clientKeyPassphrase));
        }

        if (trustStoreFile != null) {
            builder.withTrustStoreFile(runContext.render(trustStoreFile));
        }

        if (trustStorePassphrase != null) {
            builder.withTrustStorePassphrase(runContext.render(trustStorePassphrase));
        }

        if (keyStoreFile != null) {
            builder.withKeyStoreFile(runContext.render(keyStoreFile));
        }

        if (keyStorePassphrase != null) {
            builder.withKeyStorePassphrase(runContext.render(keyStorePassphrase));
        }

        return builder.build();
    }
}
