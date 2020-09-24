package org.kestra.task.kubernetes.models;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import lombok.Builder;
import lombok.Getter;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.runners.RunContext;

@Builder
@Getter
public class Connection {
    @InputProperty(
        description = "Trust all certificates"
    )
    private final Boolean trustCerts;

    @InputProperty(
        description = "Disable hostname verification"
    )
    private final Boolean disableHostnameVerification;

    @InputProperty(
        description = "The url to kubernetes API",
        dynamic = true
    )
    @Builder.Default
    private final String masterUrl = "https://kubernetes.default.svc";

    @InputProperty(
        description = "The api version of API to use",
        dynamic = true
    )
    @Builder.Default
    private final String apiVersion = "v1";

    @InputProperty(
        description = "The namespace used",
        dynamic = true
    )
    private final String namespace;

    @InputProperty(
        description = "CA certificate as file path",
        dynamic = true
    )
    private final String caCertFile;

    @InputProperty(
        description = "CA certificate as data (",
        dynamic = true
    )
    private final String caCertData;

    @InputProperty(
        description = "Client certificate as file path",
        dynamic = true
    )
    private final String clientCertFile;

    @InputProperty(
        description = "Client certificate as data",
        dynamic = true
    )
    private final String clientCertData;

    @InputProperty(
        description = "Client Key as file path",
        dynamic = true
    )
    private final String clientKeyFile;

    @InputProperty(
        description = "Client Key as data",
        dynamic = true
    )
    private final String clientKeyData;

    @InputProperty(
        description = "Client key encryption algorithm",
        body = "default is RSA",
        dynamic = true
    )
    @Builder.Default
    private final String clientKeyAlgo = "RSA";

    @InputProperty(
        description = "Client key passphrase",
        dynamic = true
    )
    private final String clientKeyPassphrase;

    @InputProperty(
        description = "Truststore file",
        dynamic = true
    )
    private final String trustStoreFile;

    @InputProperty(
        description = "Truststore passphrase",
        dynamic = true
    )
    private final String trustStorePassphrase;

    @InputProperty(
        description = "Key store file",
        dynamic = true
    )
    private final String keyStoreFile;

    @InputProperty(
        description = "Key store passphrase",
        dynamic = true
    )
    private final String keyStorePassphrase;

    @InputProperty(
        description = "Oauth token",
        dynamic = true
    )
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
