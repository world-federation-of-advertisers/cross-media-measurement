# Kubernetes Manifests

This package contains targets that generate Kubernetes manifests for deploying
CMMS components using [CUE](https://cuelang.org/).

See the
[Kubernetes API Reference](https://kubernetes.io/docs/reference/kubernetes-api/)
for background.

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
