package io.kestra.plugin.kubernetes.runner;

import io.kestra.core.models.annotations.PluginProperty;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class Config {
    @PluginProperty(dynamic = true)
    private String masterUrl;

    @PluginProperty
    @Builder.Default
    private Boolean trustCerts = false;

    @PluginProperty(dynamic = true)
    private String username;

    @PluginProperty(dynamic = true)
    private String password;

    @PluginProperty(dynamic = true)
    private String namespace;

    @PluginProperty(dynamic = true)
    private String caCert;

    @PluginProperty(dynamic = true)
    private String clientCert;

    @PluginProperty(dynamic = true)
    private String clientKey;

    @PluginProperty(dynamic = true)
    private String clientKeyAlgo;

    @PluginProperty(dynamic = true)
    private String oAuthToken;
}
