# Deploying EDP simulators on GKE

The event data provider (EDP) simulator can be used to simulate a `DataProvider`
that fulfills event `Requisition`s.

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

## Before You Start

See [Machine Setup](machine-setup.md).

## Configure event data source

There are two data sources that can be used:

1.  Synthetic generator

    Events are generated according to
    [simulator synthetic data specifications](../../src/main/proto/wfa/measurement/api/v2alpha/event_group_metadata/testing/simulator_synthetic_data_spec.proto),
    consisting of a single `SyntheticPopulationSpec` and a
    `SyntheticEventGroupSpec` for each `EventGroup`. There are default
    specifications included, but you can replace these with your own after
    before you apply the K8s Kustomization.

    This data source supports any event message type.

2.  BigQuery table

    Events are read from a Google Cloud BigQuery table. See the section below on
    how to populate the table.

    This data source currently only supports the `halo_cmm.uk.pilot.Event`
    message type.

### Populate BigQuery table

The BigQuery table schema has the following columns:

*   `date`
*   Type: `DATE`
*   `publisher_id`
*   Type: `INTEGER`
*   `vid`
*   Type: `INTEGER`
*   `digital_video_completion_status`
*   Type: `STRING`
*   Values:
*   `0% - 25%`
*   `25% - 50%`
*   `50% - 75%`
*   `75% - 100%`
*   `100%`
*   `viewability`
*   Type: `STRING`
*   Values:
*   `viewable_0_percent_to_50_percent`
*   `viewable_50_percent_to_100_percent`
*   `viewable_100_percent`

The `dev` configuration expects a table named `labelled_events` in a dataset
named `demo` in the `us-central1` region. The table can be created in the
[Google Cloud Console](https://console.cloud.google.com/bigquery), specifying a
CSV file with automatic schema detection.

The
[`uk-pilot-synthetic-data-gen` script](https://github.com/world-federation-of-advertisers/uk-pilot-synthetic-data-gen)
may be helpful in generating a CSV file with test events.

## Provision Google Cloud Project infrastructure

This can be done using Terraform. See [the guide](terraform.md) to use the
example configuration for the simulators.

Applying the Terraform configuration will create a new cluster. You can use the
`gcloud` CLI to obtain credentials so that you can access the cluster via
`kubectl`. For example:

```shell
gcloud container clusters get-credentials simulators
```

## Build and push container image (optional)

If you aren't using pre-built release images, you can build the image yourself
from source and push them to a container registry. For example, if you're using
the [Google Container Registry](https://cloud.google.com/container-registry),
you would specify `gcr.io` as your container registry and your Cloud project
name as your image repository prefix.

The build target to use depends on the event data source. Assuming a project
named `halo-cmm-demo` and an image tag `build-0001`, run the following to build
and push the image:

*   Synthetic generator

    ```shell
    bazel run -c opt //src/main/docker:push_synthetic_generator_edp_simulator_runner_image \
      --define container_registry=gcr.io \
      --define image_repo_prefix=halo-cmm-demo --define image_tag=build-0001
    ```

*   BigQuery

    ```shell
    bazel run -c opt //src/main/docker:push_bigquery_edp_simulator_runner_image \
      --define container_registry=gcr.io \
      --define image_repo_prefix=halo-cmm-demo --define image_tag=build-0001
    ```

## Generate K8s Kustomization

Run the following, substituting your own values:

*   Synthetic generator

    ```shell
    bazel build //src/main/k8s/dev:synthetic_generator_edp_simulators.tar \
    --define=kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
    --define=worker1_name=worker1 \
    --define=worker1_public_api_target=public.worker1.dev.halo-cmm.org:8443 \
    --define=worker2_name=worker2 \
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
    --define container_registry=gcr.io \
    --define image_repo_prefix=halo-cmm-demo --define image_tag=build-0001
    ```

    The resulting archive will contain `SyntheticEventGroupSpec` messages in
    text format under `src/main/k8s/dev/synthetic_generator_config_files/`.
    These can be replaced in order to customize the synthetic generator.

*   BigQuery

    ```shell
    bazel build //src/main/k8s/dev:bigquery_edp_simulators.tar \
      --define=kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
      --define=worker1_name=worker1 \
      --define=worker1_public_api_target=public.worker1.dev.halo-cmm.org:8443 \
      --define=worker2_name=worker2 \
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
      --define container_registry=gcr.io \
      --define=google_cloud_project=halo-cmm-demo \
      --define=bigquery_dataset=demo \
      --define=bigquery_table=labelled_events \
      --define image_repo_prefix=halo-cmm-demo --define image_tag=build-0001
    ```

Extract the generated archive to some directory.

## Apply K8s Kustomization

From the Kustomization directory, run

*   Synthetic generator

    ```shell
    kubectl apply -k src/main/k8s/dev/synthetic_generator_edp_simulators
    ```

*   BigQuery

    ```shell
    kubectl apply -k src/main/k8s/dev/bigquery_edp_simulators
    ```
