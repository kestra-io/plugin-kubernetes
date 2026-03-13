# Kestra Kubernetes Plugin

## What

Utilize Kubernetes resources for scalable Kestra data orchestration. Exposes 6 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Kubernetes, allowing orchestration of Kubernetes-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
