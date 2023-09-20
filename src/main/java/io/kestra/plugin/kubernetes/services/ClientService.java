package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

abstract public class ClientService {
    /**
     * Creates an {@link KubernetesClientBuilder} which is pre-configured from the cluster configuration.
     * loading the in-cluster config, including:
     * 1. System properties
     * 2. Environment variables
     * 3. Kube config file
     * 4. Service account token and a mounted CA certificate
     *
     * @return {@link KubernetesClient} configured from the cluster configuration
     */
    public static KubernetesClient of()  {
        return new KubernetesClientBuilder().build();
    }

    /**
     * Creates an {@link KubernetesClientBuilder}from a {@link Config}.
     *
     * @param config The {@link Config} to configure the builder from.
     * @return {@link DefaultKubernetesClient} configured from the provided {@link Config}
     */
    public static KubernetesClient of(Config config)  {
        return new KubernetesClientBuilder().withConfig(config).build();
    }
}
