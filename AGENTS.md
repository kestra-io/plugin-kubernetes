# Kestra Kubernetes Plugin

## What

- Provides plugin components under `io.kestra.plugin.kubernetes`.
- Includes classes such as `PodCreate`, `Delete`, `Patch`, `Apply`.

## Why

- What user problem does this solve? Teams need to run workloads and kubectl operations against Kubernetes clusters from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Kubernetes steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Kubernetes.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `kubernetes`

### Key Plugin Classes

- `io.kestra.plugin.kubernetes.core.PodCreate`
- `io.kestra.plugin.kubernetes.kubectl.Apply`
- `io.kestra.plugin.kubernetes.kubectl.Delete`
- `io.kestra.plugin.kubernetes.kubectl.Get`
- `io.kestra.plugin.kubernetes.kubectl.Patch`
- `io.kestra.plugin.kubernetes.kubectl.Restart`

### Project Structure

```
plugin-kubernetes/
├── src/main/java/io/kestra/plugin/kubernetes/watchers/
├── src/test/java/io/kestra/plugin/kubernetes/watchers/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
