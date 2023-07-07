# `dev` Kubernetes Environment

K8s manifest generation for the `dev` environment, which is the `halo-cmm-dev`
Google Cloud project. The matching configuration for the Cross-Media Measurement
System is in
https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/k8s/dev.

## Push image

```shell
bazel run -c opt //src/main/docker/panel_exchange_client:push_google_cloud_example_daemon_image \
  --define=container_registry=gcr.io --define=image_repo_prefix=halo-cmm-dev
```

## Create secret

Use [testing `secretfiles`](../testing/secretfiles).

```shell
bazel run //src/main/k8s/panelmatch/testing/secretfiles:apply_kustomization
```

Use the generated K8s secret name when building the `cue_export` targets.
