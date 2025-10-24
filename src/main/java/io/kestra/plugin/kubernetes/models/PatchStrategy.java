package io.kestra.plugin.kubernetes.models;

/**
 * Defines the patch strategy to use when patching Kubernetes resources.
 * <p>
 * Kubernetes supports three different patch strategies, each with different semantics
 * and use cases. This enum maps to the native Kubernetes patch types.
 *
 * @see <a href="https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/">Kubernetes Patch Documentation</a>
 */
public enum PatchStrategy {
    /**
     * Strategic Merge Patch is the default Kubernetes patch strategy.
     * <p>
     * This strategy understands the structure of Kubernetes resources and performs
     * intelligent merging of fields. For lists, it uses merge keys to identify
     * elements (e.g., matching containers by name). This is the most user-friendly
     * approach and matches the default behavior of {@code kubectl patch}.
     * <p>
     * Example: Updating container resources in a deployment will merge with the
     * existing container definition rather than replacing the entire container list.
     */
    STRATEGIC_MERGE,

    /**
     * JSON Merge Patch (RFC 7386) provides simple merge semantics.
     * <p>
     * This strategy performs a simple merge where null values explicitly delete fields.
     * It's particularly useful for removing annotations, labels, or other fields.
     * Note that this strategy replaces entire arrays rather than merging them.
     * <p>
     * Example use case: Removing an annotation by setting its value to null.
     *
     * @see <a href="https://datatracker.ietf.org/doc/html/rfc7386">RFC 7386</a>
     */
    JSON_MERGE,

    /**
     * JSON Patch (RFC 6902) provides precision operations on resources.
     * <p>
     * This strategy uses an array of operations: {@code add}, {@code remove}, {@code replace},
     * {@code test}, {@code copy}, and {@code move}. It's the most powerful option, allowing
     * conditional updates via the {@code test} operation and precise manipulation of
     * array elements by index.
     * <p>
     * Example: Conditionally updating replicas only if the current value is 3.
     *
     * @see <a href="https://datatracker.ietf.org/doc/html/rfc6902">RFC 6902</a>
     */
    JSON_PATCH
}
