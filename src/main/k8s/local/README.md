# Local Kubernetes Deployment

How to deploy system components to a local Kubernetes cluster running in
[KiND](https://kind.sigs.k8s.io/).

## Create Secret

```shell
kubectl apply -k src/main/k8s/testing/secretfiles/
```

The secret name will be printed on creation, but it can also be obtained later
by running

```shell
kubectl get secrets
```

## Deploy Kingdom

Supposing the secret name is `certs-and-configs-2m48c7m6m6`, then the command to
deploy the Kingdom and set up some initial resources would be

```shell
bazel run //src/main/k8s/local:kingdom_kind --define=k8s_secret_name=certs-and-configs-2m48c7m6m6
```

After the resource setup job has completed, you can obtain the created resource
names from its logs.

```shell
kubectl logs -f jobs/resource-setup-job
```
