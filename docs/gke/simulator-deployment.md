# Deploying EDP simulators on GKE

The event data provider (EDP) simulator can be used to simulate a `DataProvider`
that fulfills event `Requisition`s.

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

## Before You Start

See [Machine Setup](machine-setup.md).

## Provision Google Cloud Project infrastructure

This can be done using Terraform. See [the guide](terraform.md) to use the
example configuration for the simulators.

Applying the Terraform configuration will create a new cluster. You can use the
`gcloud` CLI to obtain credentials so that you can access the cluster via
`kubectl`. For example:

```shell
gcloud container clusters get-credentials simulators
```

## Build and push container image (not recommended)

If you aren't using pre-built release images, you can build the image yourself
from source and push them to a container registry. For example, if you're using
the [Google Container Registry](https://cloud.google.com/container-registry),
you would specify `gcr.io` as your container registry and your Cloud project
name as your image repository prefix.

The build target to use depends on the event data source. Assuming a project
named `halo-cmm-demo` and an image tag `build-0001`, run the following to build
and push the image:

```shell
bazel run -c opt //src/main/docker:push_legacy_metadata_edp_simulator_runner_image \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-cmm-demo --define image_tag=build-0001
```

## Generate K8s Kustomization

Run the following, substituting your own values:

```shell
bazel build //src/main/k8s/dev:edp_simulators.tar \
--define=kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
--define=worker1_id=worker1
--define=worker1_public_api_target=public.worker1.dev.halo-cmm.org:8443 \
--define=worker2_id=worker2
--define=worker2_public_api_target=public.worker2.dev.halo-cmm.org:8443 \
--define=mc_name=measurementConsumers/TGWOaWehLQ8 \
--define=edp1_name=dataProviders/HRL1wWehTSM \
--define=edp1_cert_name=dataProviders/HRL1wWehTSM/certificates/HRL1wWehTSM \
--define=edp2_name=dataProviders/djQdz2ehSSE \
--define=edp2_cert_name=dataProviders/djQdz2ehSSE/certificates/djQdz2ehSSE \
--define=edp3_name=dataProviders/SQ99TmehSA8 \
--define=edp3_cert_name=dataProviders/SQ99TmehSA8/certificates/SQ99TmehSA8 \
--define=edp4_name=dataProviders/TBZkB5heuL0 \
--define=edp4_cert_name=dataProviders/TBZkB5heuL0/certificates/TBZkB5heuL0 \
--define=edp5_name=dataProviders/HOCBxZheuS8 \
--define=edp5_cert_name=dataProviders/HOCBxZheuS8/certificates/HOCBxZheuS8 \
--define=edp6_name=dataProviders/VGExFmehRhY \
--define=edp6_cert_name=dataProviders/VGExFmehRhY/certificates/VGExFmehRhY \
--define container_registry=ghcr.io \
--define image_repo_prefix=world-federation-of-advertisers \
--define image_tag=0.5.21
```

Extract the generated archive to some directory.

## Customize Behavior

### EventGroups

The simulator ensures that some `EventGroup`s exist for the simulated EDP. These
can be customized using `--event-group-` set of options, where the whole set can
be repeated for each `EventGroup`.

### Synthetic Event Data

Events are generated according to
[simulator synthetic data specifications](../../src/main/proto/wfa/measurement/api/v2alpha/event_group_metadata/testing/simulator_synthetic_data_spec.proto),
consisting of a single `SyntheticPopulationSpec` and a `SyntheticEventGroupSpec`
for each `EventGroup`.

The extracted Kustomization directory will contain a ConfigMap generator under
`src/main/k8s/dev/edp_simulator_config_files/` where you can specify your specs
in protobuf text format. By default, these come with the specs necessary for
running the K8s correctness test.

If you want to use an event message type other than
[`TestEvent`](../../src/main/proto/wfa/measurement/api/v2alpha/event_templates/testing/test_event.proto)
in your`SyntheticPopulationSpec`, you will need to specify path to the
`FileDescriptorSet` using the `--event-message-descriptor-set` option. This can
be specified multiple times if the dependencies span multiple
`FileDescriptorSet`s.

## Apply K8s Kustomization

From the Kustomization directory, run

```shell
kubectl apply -k src/main/k8s/dev/edp_simulators
```
