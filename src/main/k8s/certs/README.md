# Test Certificate for GKE correctness Test

In the GKE correctness test, we pre-generate all root/server/client certificates
and store them inside a Kubernetes Secret, which is then amounted to a volume in
each pod.

The Subject Key Identifier of all root certificates are also hardcoded in the
"src/main/k8s/duchy_rpc_config.textproto".

## Cert generation

These test certificates were generated using the following build rules.

```
load("@wfa_common_jvm//build/wfa:generate_certificate_macros.bzl", "generate_root_certificate", "generate_user_certificate")

generate_root_certificate(
  name = "kingdom_root",
  common_name = "kingdom-ca.com",
  org = "Kingdom",
)

generate_user_certificate(
  name = "kingdom",
  common_name = "kingdom.com",
  org = "kingdom",
  root_certificate = ":kingdom_root.pem",
  root_key = ":kingdom_root.key",
)

generate_root_certificate(
  name = "aggregator_root",
  common_name = "aggregator-ca.com",
  org = "Aggregator",
)

generate_user_certificate(
  name = "aggregator",
  common_name = "aggregator.com",
  org = "Aggregator",
  root_certificate = ":aggregator_root.pem",
  root_key = ":aggregator_root.key",
)

generate_root_certificate(
  name = "worker_1_root",
  common_name = "worker-1-ca.com",
  org = "Worker 1",
)

generate_user_certificate(
  name = "worker_1",
  common_name = "worker-1.com",
  org = "Worker 1",
  root_certificate = ":worker_1_root.pem",
  root_key = ":worker_1_root.key",
)

generate_root_certificate(
  name = "worker_2_root",
  common_name = "worker-2-ca.com",
  org = "Worker 2",
)

generate_user_certificate(
  name = "worker_2",
  common_name = "worker-2.com",
  org = "Worker 2",
  root_certificate = ":worker_2_root.pem",
  root_key = ":worker_2_root.key",
)

genrule(
  name = "all_root_certs",
  srcs = [
    "kingdom_root.pem",
   "aggregator_root.pem",
    "worker_1_root.pem",
    "worker_2_root.pem",
  ],
  outs = ["all_root_certs.pem"],
  cmd = "cat $(SRCS) > $@",
  visibility = ["//visibility:public"],
)
```

## Create the Kubernetes Secret

Make sure the `kustomization.yaml` and all the cert files listed inside are in
the same directory, e.g., "src/main/k8s/certs".

Then run `kubectl apply -k src/main/k8s/certs`.
