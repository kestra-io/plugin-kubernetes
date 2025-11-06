@PluginSubGroup(
    description = "This subgroup of plugins contains core tasks for interacting with a Kubernetes cluster through kubectl commands.",
    categories = { PluginSubGroup.PluginCategory.TOOL, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.kubernetes.kubectl;

import io.kestra.core.models.annotations.PluginSubGroup;