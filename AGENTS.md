# Kestra Kubernetes Plugin

## What

- Provides plugin components under `io.kestra.plugin.kubernetes`.
- Includes classes such as `PodCreate`, `Delete`, `Patch`, `Apply`.

## Why

- This plugin integrates Kestra with Kubernetes Core.
- It provides tasks that create pods and manage core workload execution on Kubernetes.

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
