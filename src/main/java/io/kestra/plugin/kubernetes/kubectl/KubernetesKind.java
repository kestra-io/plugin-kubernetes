package io.kestra.plugin.kubernetes.kubectl;

public enum KubernetesKind {

    // Workloads
    PODS,
    REPLICA_SETS,
    REPLICATION_CONTROLLERS,
    DEPLOYMENTS,
    STATEFUL_SETS,
    DAEMON_SETS,
    JOBS,
    CRON_JOBS,

    // Service Discovery & Load Balancing
    SERVICES,
    ENDPOINTS,
    ENDPOINT_SLICES,
    INGRESSES,
    INGRESS_CLASSES,
    NETWORK_POLICIES,

    // Config & Storage
    CONFIG_MAPS,
    SECRETS,
    PERSISTENT_VOLUMES,
    PERSISTENT_VOLUME_CLAIMS,
    STORAGE_CLASSES,
    VOLUME_ATTACHMENTS,
    CSINODES,
    CSIDRIVERS,
    CSISTORAGECAPACITIES,

    // Cluster Resources
    NAMESPACES,
    NODES,
    EVENTS,
    LIMIT_RANGES,
    RESOURCE_QUOTAS,
    CUSTOM_RESOURCE_DEFINITIONS,

    // RBAC (Role-Based Access Control)
    SERVICE_ACCOUNTS,
    ROLES,
    CLUSTER_ROLES,
    ROLE_BINDINGS,
    CLUSTER_ROLE_BINDINGS,

    // Policy
    POD_DISRUPTION_BUDGETS,
    // POD_SECURITY_POLICIES, // Deprecated in 1.21, removed in 1.25

    // Scheduling
    PRIORITY_CLASSES,

    // API Machinery & Webhooks
    MUTATING_WEBHOOK_CONFIGURATIONS,
    VALIDATING_WEBHOOK_CONFIGURATIONS,
    APISERVICES,
    CONTROLLER_REVISIONS,

    // Certificate Signing
    CERTIFICATE_SIGNING_REQUESTS,

    // Lease (used for leader election)
    LEASES,

    // Autoscaling
    HORIZONTAL_POD_AUTOSCALERS,
}