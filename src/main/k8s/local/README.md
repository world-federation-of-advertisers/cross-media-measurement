# Local Kubernetes Configuration

Configuration used to deploy a local version of the CMMS to a local cluster in
[KinD](https://kind.sigs.k8s.io/) and run the correctness test in that cluster.

Tested on minimum versions:

-   KinD: v0.17.0
-   Kubernetes: v1.24

Note: As of this writing, KinD
[does not support resource limits](https://github.com/kubernetes-sigs/kind/issues/2514#issuecomment-951265965).
Therefore, this configuration does not set any.

## Automated Run

If you just want to run the correctness test and not leave the cluster running,
you can use the
`//src/test/kotlin/org/wfanet/measurement/integration/k8s:CorrectnessTest` Bazel
test target.

## Manual Run

This assumes that you have `kubectl` installed and configured to point to a
local KiND cluster. You should have some familiarity with Kubernetes and
`kubectl`.

Use the default `kind` as the KiND cluster name. The corresponding K8s cluster
name is `kind-kind`.

Note that some of the targets listed below -- namely, the Duchies and
simulators -- have requirements regarding the version of glibc in the build
environment. See [Building](../../../../docs/building.md).

### Initial Setup

### Create Empty `config-files` ConfigMap

The `config-files` ConfigMap contains configuration files that depend on API
resource names. These cannot be properly filled in until after
resource-setup-job has completed, but the files are required to start some of
the Kingdom and Duchy services. Therefore, we initially create the ConfigMap
with empty files.

```shell
touch /tmp/authority_key_identifier_to_principal_map.textproto
kubectl create configmap config-files \
  --from-file=/tmp/authority_key_identifier_to_principal_map.textproto
```

### Deploy Emulators

The local test environment uses emulators for Google Cloud infrastructure. This
includes an in-memory Spanner emulator as well as ephemeral blob storage.

```shell
bazel run //src/main/k8s/local:emulators_kind
```

### Metrics Setup

The Open Telemetry Operator adds the creation and management of new Open
Telemetry specific resources. It depends on Cert Manager to run.

#### Deploy Cert Manager

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.9.1/cert-manager.yaml
```

#### Deploy Open Telemetry Operator

```shell
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/download/v0.60.0/opentelemetry-operator.yaml
```

#### Deploy Open Telemetry Resources

Create an operator instrumentation resource fpr instrumenting the application
code and an operator collector sidecar resource for collecting the metrics.

```shell
bazel run //src/main/k8s/local:open_telemetry_kind
```

#### Deploy Prometheus Server

```shell
bazel run //src/main/k8s/local:prometheus_kind
```

To be able to visit Prometheus in the browser at http://localhost:31111/, start
port-forwarding.

```shell
kubectl port-forward prometheus-pod 31111:9090
```

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

### Resource Setup

There is a chicken-and-egg problem with setting up initial resources, in that
resource setup is done by calling Kingdom services but some Kingdom services
depend on resource configuration. Therefore, we have to deploy the Kingdom for
resource setup and then update configurations and restart some Kingdom services.

```shell
bazel run //src/main/k8s/local:kingdom_kind
bazel run //src/main/k8s/local:resource_setup_kind
```

By default, the resource setup job writes all outputs to STDOUT. When the job
has completed, you can read this by viewing the pod logs:

```shell
kubectl logs jobs/resource-setup-job
```

Tip: The job will output a `resource-setup.bazelrc` file with `--define` options
that you can include in your `.bazelrc` file. You can then specify
`--config=halo-kind` to Bazel commands instead of those individual options.

#### Update `config-files` ConfigMap

After the resource setup job has completed, we can fill in the config files and
update the `config-files` ConfigMap.

The resource setup job will output an
`authority_key_identifier_to_principal_map.textproto` file with entries for each
of the test EDPs, using the AKIDs from the test certificates in
[secretfiles](../testing/secretfiles). You can copy this file and use it to
replace the ConfigMap:

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
  --from-file=authority_key_identifier_to_principal_map.textproto \
  | kubectl replace -f -
```

For more information on the file format, see
[Creating Resources](../../../../docs/operations/creating-resources.md).

Note: If also want to deploy the Reporting Server in the same cluster, you will
need more entries in this ConfigMap. See the
[Reporting Server](#reporting-server) section below.

You can then restart the Kingdom deployments that depend on `config-files`. At
the moment, this is just the public API server.

```shell
kubectl rollout restart deployments/v2alpha-public-api-server-deployment
```

### Deploy Duchies

The testing environment uses three Duchies: an aggregator and two workers, named
`aggregator`, `worker1`, and `worker2` respectively. Substitute the appropriate
secret name and Certificate resource names in the command below.

```shell
bazel run //src/main/k8s/local:duchies_kind \
  --define=aggregator_cert_name=duchies/aggregator/certificates/f3yI3aoXukM \
  --define=worker1_cert_name=duchies/worker1/certificates/QtffTVXoRno \
  --define=worker2_cert_name=duchies/worker2/certificates/eIYIf6oXuSM
```

### Deploy EDP Simulators

The testing environment simulates six DataProviders, named `edp1` through
`edp6`. Substitute the appropriate secret name and resource names in the command
below. These should match the resource names specified in
`authority_key_identifier_to_principal_map.textproto` above.

```shell
bazel run //src/main/k8s/local:edp_simulators_kind \
  --define=mc_name=measurementConsumers/FS1n8aTrck0 \
  --define=edp1_name=dataProviders/OljiQHRz-E4 \
  --define=edp2_name=dataProviders/Fegw_3Rz-2Y \
  --define=edp3_name=dataProviders/aeULv4uMBDg \
  --define=edp4_name=dataProviders/d2QIG4uMA8s \
  --define=edp5_name=dataProviders/IjDOL3Rz_PY \
  --define=edp6_name=dataProviders/U8rTiHRz_b4
```

The target `edp_simulators_kind` uses `RandomEventQuery` which will generate
random VIDs for each edp. You can also use target `edp_simulators_csv_kind`
which will use `CsvEventQuery` to query VIDs from a CSV file for each edp.

To use `CsvEventQuery`, you need to copy the CSV files you want to use from your
local machine to the edp containers. After running the `bazel run` command with
the target `edp_simulators_csv_kind`, and each edp deployment is in the status
`1/1 Running`, run the following command for each edp to copy the CSV file from
you local machine to the edp container:

```shell
kubectl cp </path/to/your/file.csv> <edp-podname>:/data/csvfiles/synthetic-labelled-events.csv
```

You can get `<edp-podname>` by running `kubectl get pods`. The default volume
mountPath in the container is `/data/csvfiles`.

### Deploy MC Frontend Simulator

This is a job that tests correctness by creating a Measurement and validating
the result.

```shell
bazel run //src/main/k8s/local:mc_frontend_simulator_kind \
  --define=mc_name=measurementConsumers/FS1n8aTrck0 \
  --define=mc_api_key=He941S1h2XI
```

### Reporting Server

#### Additional Configuration in `config-files`

This is a modification to the [above section](#update-config-files-configmap) on
updating `config-files`.

The `authority_key_identifier_to_principal_map.textproto` needs an additional
entry for the `MeasurementConsumer` (substitute your own resource name):

```prototext
entries {
  authority_key_identifier: "\x7C\xE6\x3F\xEA\x65\xED\x71\x3D\x9E\x59\x79\xA0\xC8\x08\xC9\x57\xAA\xC6\xB1\x6A"
  principal_resource_name: "measurementConsumers/G7laM7LMIAA"
}
```

The ConfigMap also needs an additional file named
`encryption_key_pair_config.textproto` listing key pairs by
`MeasurementConsumer`:

```prototext
# proto-file: wfa/measurement/config/reporting/encryption_key_pair_config.proto
# proto-message: EncryptionKeyPairConfig
principal_key_pairs {
  principal: "measurementConsumers/G7laM7LMIAA"
  key_pairs {
    public_key_file: "mc_enc_public.tink"
    private_key_file: "mc_enc_private.tink"
  }
  key_pairs {
    public_key_file: "mc_enc_public_2.tink"
    private_key_file: "mc_enc_private_2.tink"
  }
}
principal_key_pairs {
  principal: "measurementConsumers/FS1n8aTrck0"
  key_pairs {
    public_key_file: "mc2_enc_public.tink"
    private_key_file: "mc2_enc_private.tink"
  }
}
```

The command to update the ConfigMap then becomes:

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
  --from-file=authority_key_identifier_to_principal_map.textproto \
  --from_file=encryption_key_pair_config.textproto \
  | kubectl replace -f -
```

#### Create Secret for Reporting Server Postgres Database

You can use `kubectl` to create the `db-auth` secret. To reduce the likelihood
of leaking your password, we read it in from STDIN.

Tip: Ctrl+D is the usual key combination for closing the input stream.

Assuming the database username is `db-user`, run:

```shell
kubectl create secret generic db-auth --type='kubernetes.io/basic/auth' \
  --append-hash --from-file=password=/dev/stdin --from-literal=username=db-user
```

Record the secret name for later steps.

#### Deploy Reporting Server Postgres Database

```shell
bazel run //src/main/k8s/local:reporting_database_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=k8s_db_secret_name=db-auth-b286t5fcmt
```

### Create Secret for Reporting API Server

Create the file `/tmp/measurement_consumer_config.textproto` with the content
below, substituting the appropriate MC and certificate resource names, and the
API key.

```prototext
# proto-file: wfa/measurement/config/reporting/measurement_consumer_config.proto
# proto-message: MeasurementConsumerConfigs
configs {
  key: "measurementConsumers/VCTqwV_vFXw"
  value: {
    api_key: "OljiQHRz-E4"
    signing_certificate_name: "measurementConsumers/VCTqwV_vFXw/certificates/YCIa5l_vFdo"
    signing_private_key_path: "mc_cs_private.der"
  }
}
```

Use this file to create a secret:

```shell
kubectl create secret generic mc-config --append-hash \
  --from-file=/tmp/measurement_consumer_config.textproto
```

Record the secret name for later steps.

#### Deploy Reporting Server

```shell
bazel run //src/main/k8s/local:reporting_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=k8s_db_secret_name=db-auth-b286t5fcmt \
  --define=k8s_mc_config_secret_name=mc-config-975k88gktk
```
