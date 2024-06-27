# Local Kubernetes Configuration

Configuration used to deploy a testing version of the CMMS to a single local
cluster, for example using [KinD](https://kind.sigs.k8s.io/),
[K3s](https://k3s.io/), or [minikube](https://minikube.sigs.k8s.io/docs/).

Note: As of this writing, KinD
[does not support resource limits](https://github.com/kubernetes-sigs/kind/issues/2514#issuecomment-951265965).
Therefore, this configuration does not set any.

## Requirements

You will need:

*   A container registry that you can push images to.
*   A running Kubernetes 1.24+ cluster.
    *   Your container registry must be accessible from this cluster.

See [Local Cluster Creation](#local-cluster-creation) in the Appendix for how to
create a local cluster with access to a local private registry.

This guide assumes that you have `kubectl` installed and configured to point to
the cluster you want to use for testing. You should have some familiarity with
Kubernetes and `kubectl`.

Note that some of the targets listed below -- namely, the Duchies and
simulators -- have additional requirements regarding the version of glibc in the
build environment. See [Building](../../../../docs/building.md).

The example commands below assume that your container registry is hosted at
`registry.dev.svc.cluster.local:5001`.

Tip: Add the `--define` options in your `.bazelrc` under the `halo-local` build
configuration so that you can just pass `--config=halo-local`.

## Automated Test Run

If you just want to run the correctness test, you can use the
`//src/test/kotlin/org/wfanet/measurement/integration/k8s:EmptyClusterCorrectnessTest`
Bazel test target and skip the rest of this document.

```shell
bazel test //src/test/kotlin/org/wfanet/measurement/integration/k8s:EmptyClusterCorrectnessTest \
  --define container_registry=registry.dev.svc.cluster.local:5001 \
  --define image_repo_prefix=halo --define image_tag=latest
```

Note that the test assumes that the default namespace of the cluster is empty.
You can clear it out between runs:

```shell
kubectl delete all --namespace=default --all
```

## Building and Pushing Container Images

```shell
tools/bazel-container-run //src/main/docker:push_all_local_images \
  --define container_registry=registry.dev.svc.cluster.local:5001 \
  --define image_repo_prefix=halo --define image_tag=latest
```

## Cluster Population

Population of the test cluster is done in two stages: the first is setting up a
Kingdom with an empty configuration in order to create API resources. The second
is restarting the Kingdom with the resulting configuration and the rest of the
components.

### Initial Setup

Build a tar archive containing the Kustomization directory for Kingdom setup:

```shell
bazel build //src/main/k8s/local:kingdom_setup.tar \
  --define container_registry=registry.dev.svc.cluster.local:5001 \
  --define image_repo_prefix=halo --define image_tag=latest
```

Extract this archive to some directory (e.g. `/tmp/cmms`). For example:

```shell
tar -xf bazel-bin/src/main/k8s/local/kingdom_setup.tar -C /tmp/cmms
```

You can then apply it using `kubectl` from this directory:

```shell
kubectl apply -k /tmp/cmms/src/main/k8s/local/kingdom_setup/
```

This will create the emulators and the Kingdom with an empty configuration.

### Run ResourceSetup

The `ResourceSetup` tool will create API resources for testing. First, build the
tool:

```shell
bazel build //src/main/kotlin/org/wfanet/measurement/loadtest/resourcesetup:ResourceSetup
```

We'll then need to be able to access the public and internal APIs from the host
machine. This can be done by forwarding the service ports to the host machine:

```shell
kubectl port-forward --address=localhost services/v2alpha-public-api-server 8443:8443
```

```shell
kubectl port-forward --address=localhost services/gcp-kingdom-data-server 9443:8443
```

Then run the tool, outputting to some directory (e.g. `/tmp/resource-setup`,
make sure this directory has been created):

```shell
src/main/k8s/testing/resource_setup.sh \
  --kingdom-public-api-target=localhost:8443 \
  --kingdom-internal-api-target=localhost:9443 \
  --bazel-config-name=halo-local \
  --output-dir=/tmp/resource-setup
```

Note: You can stop the port forwarding at this point. Future steps involve
restarting some Deployments, rendering the forwarding invalid.

Tip: The job will output a `resource-setup.bazelrc` file that contains
`--define` options that you can include in your `.bazelrc` file in
`/tmp/resource-setup`. You can then specify `--config=halo-local` to Bazel
commands instead of those individual options.

### Deploy the CMMS

Once resource setup is complete, we can build and apply our final Kustomization.

Note: If you plan on deploying the Reporting system as well, you can skip this
step and follow the one in the Reporting section instead.

Build a tar archive containing the Kustomization directory for the CMMS,
substituting the values from the output of the `ResourceSetup` tool:

```shell
bazel build //src/main/k8s/local:cmms.tar \
  --define container_registry=registry.dev.svc.cluster.local:5001 \
  --define image_repo_prefix=halo --define image_tag=latest \
  --define mc_name=measurementConsumers/ZP5ZJ9sZVXE \
  --define mc_api_key=OzWUxyTmqwg \
  --define mc_cert_name=measurementConsumers/ZP5ZJ9sZVXE/certificates/YCIa5l_vFdo \
  --define edp1_name=dataProviders/UjUpwCTmq0o \
  --define edp1_cert_name=dataProviders/UjUpwCTmq0o/certificates/UjUpwCTmq0o \
  --define edp2_name=dataProviders/cV4YC9sZVKQ \
  --define edp2_cert_name=dataProviders/cV4YC9sZVKQ/certificates/cV4YC9sZVKQ \
  --define edp3_name=dataProviders/DJweaNsZVJY \
  --define edp3_cert_name=dataProviders/DJweaNsZVJY/certificates/DJweaNsZVJY \
  --define edp4_name=dataProviders/JxgZTyTmq3k \
  --define edp4_cert_name=dataProviders/JxgZTyTmq3k/certificates/JxgZTyTmq3k \
  --define edp5_name=dataProviders/f8NzvNsZVHk \
  --define edp5_cert_name=dataProviders/f8NzvNsZVHk/certificates/f8NzvNsZVHk \
  --define edp6_name=dataProviders/QOgxPtsZVGk \
  --define edp6_cert_name=dataProviders/QOgxPtsZVGk/certificates/QOgxPtsZVGk \
  --define aggregator_cert_name=duchies/aggregator/certificates/clMWAdsZVFM \
  --define worker1_cert_name=duchies/worker1/certificates/Lm30i9sZVDo \
  --define worker2_cert_name=duchies/worker2/certificates/BXNL1CTmq9M
```

Extract this archive to some directory (e.g. `/tmp/cmms`).

```shell
tar -xf bazel-bin/src/main/k8s/local/cmms.tar -C /tmp/cmms
```

Copy the `authority_key_identifier_to_principal_map.textproto` output from the
`ResourceSetup` tool to the `src/main/k8s/local/config_files` path within this
directory. This uses the AKIDs from the test certificates in
[secretfiles](../testing/secretfiles). For more information on the file format,
see [Creating Resources](../../../../docs/operations/creating-resources.md).

You can then apply the Kustomization from the directory where you extracted the
archive:

```shell
kubectl apply -k src/main/k8s/local/cmms/
```

This will restart the Kingdom with the updated configuration. It will also
create the Duchies and EDP simulators.

To use the Kingdom CLI tool, you need to forward the port again:

```shell
kubectl port-forward --address=localhost services/v2alpha-public-api-server 8443:8443
```

### Deploy the CMMS and Reporting System

This is an alternate version of the section above. This assumes you've already
done the Initial Setup and have the output from the `ResourceSetup` tool.

Use the command in the above section to build the tar archive, swapping the
target with `//src/main/k8s/local:cmms_with_reporting.tar`. Extract this archive
to some directory (e.g. `/tmp/cmms`).

Copy the `authority_key_identifier_to_principal_map.textproto` output from the
`ResourceSetup` tool to the `src/main/k8s/local/config_files` path within this
directory.

You can then apply the Kustomization from the directory where you extracted the
archive:

```shell
kubectl apply -k src/main/k8s/local/cmms_with_reporting/
```

To deploy Reporting V2, swap out `cmms_with_reporting` with
`cmms_with_reporting_v2`.

To use the Reporting CLI tool, you need to forward the port again. For example:

```shell
kubectl port-forward --address=localhost services/reporting-v2alpha-public-api-server 9000:8443
```

## Enabling Metrics Collection

### Install OpenTelemetry Operator

The code is instrumented with OpenTelemetry. To enable metrics collection in the
cluster, we use the
[OpenTelemetry Operator for Kubernetes](https://github.com/open-telemetry/opentelemetry-operator).
This depends on [cert-manager](https://github.com/cert-manager/cert-manager) so
we install that first:

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.2/cert-manager.yaml
```

Once that is installed, then install the Operator:

```shell
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/download/v0.88.0/opentelemetry-operator.yaml
```

### Deploy OpenTelemetry Resources

Generate the K8s object configuration:

```shell
bazel build //src/main/k8s/local:open_telemetry
```

Apply the generated configuration:

```shell
kubectl apply -f bazel-bin/src/main/k8s/local/open_telemetry.yaml
```

This will create an `opentelemetrycollector` named `default` and enable
automatic instrumentation via the Java agent.

### Restart Deployments

If you enabled metrics collection after you have already populated the cluster,
you will need to restart all of the Deployments to pick up the Java agent
integration.

```shell
for deployment in $(kubectl get deployments -o name); do kubectl rollout restart $deployment; done
```

### Deploy Prometheus Server

If you want to be able to visualize the collected metrics, you can deploy a
Prometheus server to the cluster.

Generate the K8s object configuration:

```shell
bazel build //src/main/k8s/local:prometheus
```

Apply the generated configuration:

```shell
kubectl apply -f bazel-bin/src/main/k8s/local/prometheus.yaml
```

You can forward the Prometheus HTTP port (9090) to your host machine in order to
access it from your browser. For example, to be able to visit Prometheus in the
browser at http://localhost:31111/:

```shell
kubectl port-forward prometheus-pod 31111:9090
```

## Running the Correctness Test

Once you have a running CMMS with EDP simulators, you can run the correctness
test against it.

You'll need access to the public API server. You can do this via port forwarding
as mentioned before:

```shell
kubectl port-forward --address=localhost services/v2alpha-public-api-server 8443:8443
```

Then you can run the test, substituting your own values(e.g. `mc_name` and
`mc_api_key`):

```shell
bazel test //src/test/kotlin/org/wfanet/measurement/integration/k8s:SyntheticGeneratorCorrectnessTest
  --test_output=streamed \
  --define=kingdom_public_api_target=localhost:8443 \
  --define=mc_name=measurementConsumers/Rcn7fKd25C8 \
  --define=mc_api_key=W9q4zad246g
```

## Old Guide

This has instructions that may be outdated.

### Metrics Setup

### Deploy Grafana

Create a ConfigMap containing the example Grafana configuration.

```shell
kubectl create configmap grafana-config \
  --from-file=src/main/k8s/testing/grafana/grafana.ini
```

Create a Secret containing the Grafana basic auth credentials.

You can use `kubectl` to create the `db-auth` secret. To reduce the likelihood
of leaking your password, we read it in from STDIN.

Tip: Ctrl+D is the usual key combination for closing the input stream.

Assuming the database username is `user`, run:

```shell
kubectl create secret generic grafana-auth --type='kubernetes.io/basic/auth' \
  --append-hash --from-file=password=/dev/stdin --from-literal=user=user
```

Deploy the Grafana server using the Secret name.

```shell
bazel run //src/main/k8s/local:grafana_kind \
  --define=grafana_secret_name=grafana-auth-dmg429kb29
```

To be able to visit Grafana in the browser at http://localhost:31112/, start
port-forwarding.

```shell
kubectl port-forward service/grafana 31112:3000
```

## Appendix

### Local Cluster Creation

#### KinD

Simply follow the
[Local Registry](https://kind.sigs.k8s.io/docs/user/local-registry/) guide. The
example there should result in a registry running at `localhost:5001` with the
default kubeconfig pointing at the local cluster.

#### Minikube

To use a local private registry with Minikube, you'll need to have an additional
IP address so that you can distinguish it from `localhost`. Here we'll use
`192.168.50.1`, but you can use any private address. Add this IP as an alias to
the loopback interface using `iproute2`:

```shell
sudo ip addr add 192.168.50.1/32 dev lo
```

Note: This will only last until the next system restart. To make it persistent,
add a virtual interface to `/etc/network/interfaces` instead. For example:

```
auto lo lo:0
iface lo inet loopback

iface lo:0 inet static
  address 192.168.50.1
  netmask 255.255.255.255
```

Start the local registry using Docker, binding it to this new address:

```shell
docker run --detach --publish '192.168.50.1:5001:5001' --restart always --name local-registry registry:2
```

Next, we need a hostname. Here we'll use `registry.dev.svc.cluster.local`. Add
an entry to `/etc/hosts`:

```
192.168.50.1    registry.dev.svc.cluster.local
```

Since our local registry is insecure (using http as opposed to https), we'll
need to specify that when starting Minikube:

```shell
minikube start --insecure-registry='registry.dev.svc.cluster.local:5001'
```

Finally, add the same entry in the `/etc/hosts` file *within* Minikube. You can
do this using `minikube ssh` or using the
[file sync](https://minikube.sigs.k8s.io/docs/handbook/filesync/) feature.

Note: If you are using rootless Docker or rootless Podman, you may need to also
enable IP forwarding on your host machine. See
https://github.com/kubernetes/minikube/issues/18667
