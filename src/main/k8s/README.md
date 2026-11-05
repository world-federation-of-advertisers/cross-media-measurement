# Kubernetes Manifests

This package contains targets that generate Kubernetes manifests for deploying
CMMS components using [CUE](https://cuelang.org/).

See the
[Kubernetes API Reference](https://kubernetes.io/docs/reference/kubernetes-api/)
for background.

## EDP Aggregator rollout diagnostics

When Configure Edp Aggregator Services fails during apply or rollout, inspect
its job summary and the `edp-aggregator-rollout-diagnostics-<attempt>` artifact.
It contains deployment and pod status, relevant events, and bounded current and
previous init-container logs. `errors.json` lists reads that could not complete.

To collect the same diagnostics locally using the current Kubernetes context:

```shell
python3 .github/workflows/scripts/k8s_rollout_diagnostics.py \
  --selector=app.kubernetes.io/component=edp-aggregator \
  --output-dir=/tmp/edp-aggregator-rollout-diagnostics
```

## Useful Labels

All resources defined in these manifests should have the
`app.kubernetes.io/part-of` label set to `halo-cmms`.

The following values are used for `app.kubernetes.io/component`:

*   `kingdom`
*   `duchy`
*   `simulator` - For resources related to an EDP simulator.
*   `testing` - For testing resources, e.g. Cloud emulators.

For example, if you wanted to delete all known CMMS resources that are part of
the Kingdom, you could run

```shell
kubectl delete pods,jobs,services,deployments,networkpolicies,ingresses \
  --selector=app.kubernetes.io/part-of=halo-cmms,app.kubernetes.io/component=kingdom
```

## EDP Aggregator schema migrations

The internal API deployment applies database migrations in the
`update-edp-aggregator-schema` init container before the API process starts.
When a rollout stalls, inspect that container's current and previous logs.
After correcting the cause, resume the rollout through its deployment workflow
so the remaining service updates and worker activation steps can finish.
