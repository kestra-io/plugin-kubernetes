package io.kestra.plugin.kubernetes.services;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;

abstract public class ClientService {
    /**
     * Creates an {@link DefaultKubernetesClient} which is pre-configured from the cluster configuration.
     * loading the in-cluster config, including:
     *   1. System properties
     *   2. Environment variables
     *   3. Kube config file
     *   4. Service account token &amp; mounted CA certificate
     *
     * @return {@link DefaultKubernetesClient} configured from the cluster configuration
     */
    public static DefaultKubernetesClient of()  {
        return new DefaultKubernetesClient();
    }

    /**
     * Creates an {@link DefaultKubernetesClient}from a {@link Config}.
     *
     * @param config The {@link Config} to configure the builder from.
     * @return {@link DefaultKubernetesClient} configured from the provided {@link Config}
     */
    public static DefaultKubernetesClient of(Config config)  {
        return new DefaultKubernetesClient(config);
    }
}
