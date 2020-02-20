package org.kestra.task.kubernetes.models;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Connection {
    private Boolean trustCerts;
    private Boolean disableHostnameVerification;
    private String masterUrl = "https://kubernetes.default.svc";
    private String apiVersion = "v1";
    private String namespace;
    private String caCertFile;
    private String caCertData;
    private String clientCertFile;
    private String clientCertData;
    private String clientKeyFile;
    private String clientKeyData;
    private String clientKeyAlgo = "RSA";
    private String clientKeyPassphrase;
    private String trustStoreFile;
    private String trustStorePassphrase;
    private String keyStoreFile;
    private String keyStorePassphrase;

    public Config toConfig() {
        ConfigBuilder builder = new ConfigBuilder();

        if (trustCerts != null) {
            builder.withTrustCerts(trustCerts);
        }

        if (disableHostnameVerification != null) {
            builder.withDisableHostnameVerification(disableHostnameVerification);
        }

        if (masterUrl != null) {
            builder.withMasterUrl(masterUrl);
        }

        if (apiVersion != null) {
            builder.withApiVersion(apiVersion);
        }

        if (namespace != null) {
            builder.withNamespace(namespace);
        }

        if (caCertFile != null) {
            builder.withCaCertFile(caCertFile);
        }

        if (caCertData != null) {
            builder.withCaCertData(caCertData);
        }

        if (clientCertFile != null) {
            builder.withClientCertFile(clientCertFile);
        }

        if (clientCertData != null) {
            builder.withClientCertData(clientCertData);
        }

        if (clientKeyFile != null) {
            builder.withClientKeyFile(clientKeyFile);
        }

        if (clientKeyData != null) {
            builder.withClientKeyData(clientKeyData);
        }

        if (clientKeyAlgo != null) {
            builder.withClientKeyAlgo(clientKeyAlgo);
        }

        if (clientKeyPassphrase != null) {
            builder.withClientKeyPassphrase(clientKeyPassphrase);
        }

        if (trustStoreFile != null) {
            builder.withTrustStoreFile(trustStoreFile);
        }

        if (trustStorePassphrase != null) {
            builder.withTrustStorePassphrase(trustStorePassphrase);
        }

        if (keyStoreFile != null) {
            builder.withKeyStoreFile(keyStoreFile);
        }

        if (keyStorePassphrase != null) {
            builder.withKeyStorePassphrase(keyStorePassphrase);
        }

        return builder.build();
    }
}
