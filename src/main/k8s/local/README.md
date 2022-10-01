# Local Kubernetes Deployment

How to deploy system components to a local Kubernetes cluster running in
[KiND](https://kind.sigs.k8s.io/).

This assumes that you have `kubectl` installed and configured to point to a
local KiND cluster. You should have some familiarity with Kubernetes and
`kubectl`.

Minimum Version Required:

-   KiND: v0.13.0
-   kubernetes server: v1.24.0
-   kubectl: compatible with kubernetes server

Use the default `kind` as the KiND cluster name. The corresponding k8s cluster
name is `kind-kind`.

Note that some of the targets listed below -- namely, the Duchies and
simulators -- have requirements regarding the version of glibc in the build
environment. See [Building](../../../../docs/building.md).

## Initial Setup

### Set Default Resource Requirements

```shell
kubectl apply -f src/main/k8s/testing/secretfiles/resource_requirements.yaml
```

### Create Secret

```shell
bazel run //src/main/k8s/testing/secretfiles:apply_kustomization
```

The secret name will be printed on creation, but it can also be obtained later
by running

```shell
kubectl get secrets
```

You will need to substitute the correct secret name in later commands.

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

## Deploy Emulators

The local test environment uses emulators for Google Cloud infrastructure. This
includes an in-memory Spanner emulator as well as ephemeral blob storage.

```shell
bazel run //src/main/k8s/local:emulators_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg
```

## Resource Setup

There is a chicken-and-egg problem with setting up initial resources, in that
resource setup is done by calling Kingdom services but some Kingdom services
depend on resource configuration. Therefore, we have to deploy the Kingdom for
resource setup and then update configurations and restart some Kingdom services.

```shell
bazel run //src/main/k8s/local:kingdom_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg
bazel run //src/main/k8s/local:resource_setup_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg
```

After the resource setup job has completed, you can obtain the created resource
names from its logs.

```shell
kubectl logs jobs/resource-setup-job
```

### Update `config-files` ConfigMap

After resource-setup-job has completed, we can fill in the config files and
update the `config-files` ConfigMap.

Create the file `authority_key_identifier_to_principal_map.textproto` with the
content below, substituting the appropriate resource names. The Authority Key
Identifier(AKID) comes from the EDP certificates in
[secretfiles](../testing/secretfiles). The order of the entries follows the
lexicographic order of the testing EDP root certificates in the secretfiles
folder(edp1 through edp6). For example, the first entry is for `edp1_root.pem`

```prototext
# proto-file: wfa/measurement/config/authority_key_to_principal_map.proto
# proto-message: AuthorityKeyToPrincipalMap
entries {
  authority_key_identifier: "\x90\xC1\xD3\xBD\xE6\x74\x01\x55\xA7\xEF\xE6\x64\x72\xA6\x68\x9C\x41\x5B\x77\x04"
  principal_resource_name: "dataProviders/OljiQHRz-E4"
}
entries {
  authority_key_identifier: "\xF6\xED\xD1\x90\x2E\xF2\x04\x06\xEB\x16\xC4\x40\xCF\x69\x43\x86\x16\xCC\xAE\x08"
  principal_resource_name: "dataProviders/Fegw_3Rz-2Y"
}
entries {
  authority_key_identifier: "\xC8\x03\x73\x90\x9E\xBF\x33\x46\xEA\x94\x44\xC4\xAC\x77\x4D\x47\x67\xA1\x81\x94"
  principal_resource_name: "dataProviders/aeULv4uMBDg"
}
entries {
  authority_key_identifier: "\x95\x42\x02\x4C\xED\x13\x36\xFD\x2E\xB3\xAB\x30\xFE\x2B\x9A\x06\xBE\x19\x17\x54"
  principal_resource_name: "dataProviders/d2QIG4uMA8s"
}
entries {
  authority_key_identifier: "\x84\xEA\x3D\xFE\xD6\x45\x43\x3F\x5C\xC6\xED\x86\xA2\x83\x3D\xF8\x0D\x5D\x6B\xB7"
  principal_resource_name: "dataProviders/IjDOL3Rz_PY"
}
entries {
  authority_key_identifier: "\xBB\x12\x20\xA8\xE6\x04\x95\xCF\xA8\x33\x42\x33\x27\xD2\x07\x69\xC2\xBF\x8A\x5A"
  principal_resource_name: "dataProviders/U8rTiHRz_b4"
}
```

If needed, you can use the following OpenSSL CLI command to inspect the AKID of
each certificate. As a reminder, the byte literals in protobuf text format are
two hex digits of the AKID output from this command escaping with `\x`

```shell
openssl x509 -noout -text -in src/main/k8s/testing/secretfiles/edp1_root.pem
```

Update the ConfigMap, passing the `--from-file` option for each config file.

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
  --from-file=authority_key_identifier_to_principal_map.textproto \
  | kubectl replace -f -
```

Note: If also want to deploy the Reporting Server in the same cluster, you will
need more entries in this ConfigMap. See the
[Reporting Server](#reporting-server) section below.

You can then restart the Kingdom deployments that depend on `config-files`. At
the moment, this is just the public API server.

```shell
kubectl rollout restart deployments/v2alpha-public-api-server-deployment
```

## Re-deploy Kingdom

You now have a fully-deployed Kingdom. If you wish to redeploy the Kingdom, for
example to pick up new changes, you can do so with the following command:

```shell
bazel run //src/main/k8s/local:kingdom_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg
```

## Deploy Duchies

The testing environment uses three Duchies: an aggregator and two workers, named
`aggregator`, `worker1`, and `worker2` respectively. Substitute the appropriate
secret name and Certificate resource names in the command below.

```shell
bazel run //src/main/k8s/local:duchies_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=aggregator_cert_name=duchies/aggregator/certificates/f3yI3aoXukM \
  --define=worker1_cert_name=duchies/worker1/certificates/QtffTVXoRno \
  --define=worker2_cert_name=duchies/worker2/certificates/eIYIf6oXuSM
```

## Deploy EDP Simulators

The testing environment simulates six DataProviders, named `edp1` through
`edp6`. Substitute the appropriate secret name and resource names in the command
below. These should match the resource names specified in
`authority_key_identifier_to_principal_map.textproto` above.

```shell
bazel run //src/main/k8s/local:edp_simulators_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=mc_name=measurementConsumers/FS1n8aTrck0 \
  --define=edp1_name=dataProviders/OljiQHRz-E4 \
  --define=edp2_name=dataProviders/Fegw_3Rz-2Y \
  --define=edp3_name=dataProviders/aeULv4uMBDg \
  --define=edp4_name=dataProviders/d2QIG4uMA8s \
  --define=edp5_name=dataProviders/IjDOL3Rz_PY \
  --define=edp6_name=dataProviders/U8rTiHRz_b4
```

## Deploy MC Frontend Simulator

This is a job that tests correctness by creating a Measurement and validating
the result.

```shell
bazel run //src/main/k8s/local:mc_frontend_simulator_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=mc_name=measurementConsumers/FS1n8aTrck0 \
  --define=mc_api_key=He941S1h2XI
```

## Reporting Server

### Additional Configuration in `config-files`

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
`encryption_key_pair_config.textproto`:

```prototext
# proto-file: wfa/measurement/config/reporting/encryption_key_pair_config.proto
# proto-message: EncryptionKeyPairConfig
key_pairs {
  key: "mc_enc_public.tink"
  value: "mc_enc_private.tink"
}
```

The command to update the ConfigMap then becomes:

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
  --from-file=authority_key_identifier_to_principal_map.textproto \
  --from_file=encryption_key_pair_config.textproto \
  | kubectl replace -f -
```

### Create Secret for Reporting Server Postgres Database

You can use `kubectl` to create the `db-auth` secret. To reduce the likelihood
of leaking your password, we read it in from STDIN.

Tip: Ctrl+D is the usual key combination for closing the input stream.

Assuming the database username is `db-user`, run:

```shell
kubectl create secret generic db-auth --type='kubernetes.io/basic/auth' \
  --append-hash --from-file=password=/dev/stdin --from-literal=username=db-user
```

Record the secret name for later steps.

### Deploy Reporting Server Postgres Database

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

### Deploy Reporting Server

```shell
bazel run //src/main/k8s/local:reporting_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=k8s_db_secret_name=db-auth-b286t5fcmt \
  --define=k8s_mc_config_secret_name=mc-config-975k88gktk
```
