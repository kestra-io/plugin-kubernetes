package io.kestra.plugin.kubernetes.kubectl;

public enum KubernetesKind {

    // Workloads
    PODS("pods"),
    REPLICA_SETS("replicasets"),
    REPLICATION_CONTROLLERS("replicationcontrollers"),
    DEPLOYMENTS("deployments"),
    STATEFUL_SETS("statefulsets"),
    DAEMON_SETS("daemonsets"),
    JOBS("jobs"),
    CRON_JOBS("cronjobs"),

    // Service Discovery & Load Balancing
    SERVICES("services"),
    ENDPOINTS("endpoints"), // Note: singular is "Endpoint"
    ENDPOINT_SLICES("endpointslices"),
    INGRESSES("ingresses"),
    INGRESS_CLASSES("ingressclasses"),
    NETWORK_POLICIES("networkpolicies"),

    // Config & Storage
    CONFIG_MAPS("configmaps"),
    SECRETS("secrets"),
    PERSISTENT_VOLUMES("persistentvolumes"),
    PERSISTENT_VOLUME_CLAIMS("persistentvolumeclaims"),
    STORAGE_CLASSES("storageclasses"),
    VOLUME_ATTACHMENTS("volumeattachments"),
    CSINODES("csinodes"), // Typically csinodes
    CSIDRIVERS("csidrivers"), // Typically csidrivers
    CSISTORAGECAPACITIES("csistoragecapacities"), // Typically csistoragecapacities

    // Cluster Resources
    NAMESPACES("namespaces"),
    NODES("nodes"),
    EVENTS("events"),
    LIMIT_RANGES("limitranges"),
    RESOURCE_QUOTAS("resourcequotas"),
    CUSTOM_RESOURCE_DEFINITIONS("customresourcedefinitions"),

    // RBAC (Role-Based Access Control)
    SERVICE_ACCOUNTS("serviceaccounts"),
    ROLES("roles"),
    CLUSTER_ROLES("clusterroles"),
    ROLE_BINDINGS("rolebindings"),
    CLUSTER_ROLE_BINDINGS("clusterrolebindings"),

    // Policy
    POD_DISRUPTION_BUDGETS("poddisruptionbudgets"),
    // POD_SECURITY_POLICIES("podsecuritypolicies"), // Deprecated in 1.21, removed in 1.25

    // Scheduling
    PRIORITY_CLASSES("priorityclasses"),

    // API Machinery & Webhooks
    MUTATING_WEBHOOK_CONFIGURATIONS("mutatingwebhookconfigurations"),
    VALIDATING_WEBHOOK_CONFIGURATIONS("validatingwebhookconfigurations"),
    APISERVICES("apiservices"), // Plural of APIService
    CONTROLLER_REVISIONS("controllerrevisions"),

    // Certificate Signing
    CERTIFICATE_SIGNING_REQUESTS("certificatesigningrequests"),

    // Lease (used for leader election)
    LEASES("leases"),

    // Autoscaling
    HORIZONTAL_POD_AUTOSCALERS("horizontalpodautoscalers");

    private final String plural;

    KubernetesKind(String plural) {
        this.plural = plural;
    }

    public String getPlural() {
        return plural;
    }
}