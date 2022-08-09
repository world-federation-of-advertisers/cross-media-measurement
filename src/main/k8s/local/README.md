# Local Kubernetes Deployment

How to deploy system components to a local Kubernetes cluster running in
[KiND](https://kind.sigs.k8s.io/).

This assumes that you have `kubectl` installed and configured to point to a
local KiND cluster. You should have some familiarity with Kubernetes and
`kubectl`.

Minimum Version Required:
- KiND: v0.13.0
- kubernetes server: v1.24.0
- kubectl: compatible with kubernetes server

Note that some of the targets listed below -- namely, the Duchies and
simulators -- have requirements regarding the version of glibc in the build
environment. See [Building](../../../../docs/building.md).

## Initial Setup

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
content below, substituting the appropriate resource names. The AKIDs come from
the EDP certificates in [secretfiles](../testing/secretfiles).

```prototext
# proto-file: src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto
# proto-message: AuthorityKeyToPrincipalMap
entries {
  authority_key_identifier: "\xD6\x65\x86\x86\xD8\x7E\xD2\xC4\xDA\xD8\xDF\x76\x39\x66\x21\x3A\xC2\x92\xCC\xE2"
  principal_resource_name: "dataProviders/OljiQHRz-E4"
}
entries {
  authority_key_identifier: "\x6F\x57\x36\x3D\x7C\x5A\x49\x7C\xD1\x68\x57\xCD\xA0\x44\xDF\x68\xBA\xD1\xBA\x86"
  principal_resource_name: "dataProviders/Fegw_3Rz-2Y"
}
entries {
  authority_key_identifier: "\xEE\xB8\x30\x10\x0A\xDB\x8F\xEC\x33\x3B\x0A\x5B\x85\xDF\x4B\x2C\x06\x8F\x8E\x28"
  principal_resource_name: "dataProviders/aeULv4uMBDg"
}
entries {
  authority_key_identifier: "\x74\x72\x6D\xF6\xC0\x44\x42\x61\x7D\x9F\xF7\x3F\xF7\xB2\xAC\x0F\x9D\xB0\xCA\xCC"
  principal_resource_name: "dataProviders/d2QIG4uMA8s"
}
entries {
  authority_key_identifier: "\xA6\xED\xBA\xEA\x3F\x9A\xE0\x72\x95\xBF\x1E\xD2\xCB\xC8\x6B\x1E\x0B\x39\x47\xE9"
  principal_resource_name: "dataProviders/IjDOL3Rz_PY"
}
entries {
  authority_key_identifier: "\xA7\x36\x39\x6B\xDC\xB4\x79\xC3\xFF\x08\xB6\x02\x60\x36\x59\x84\x3B\xDE\xDB\x93"
  principal_resource_name: "dataProviders/U8rTiHRz_b4"
}
```

Update the ConfigMap, passing the `--from-file` option for each config file.

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
  --from-file=authority_key_identifier_to_principal_map.textproto \
  | kubectl replace -f -
```

If you want to also deploy the Reporting Server, you can add additional files
to the ConfigMap.

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
  --from-file=authority_key_identifier_to_principal_map.textproto \
  --from-file=authority_key_identifier_to_mc_principal_map.textproto \
  --from_file=encryption_key_pair_config.textproto \
  | kubectl replace -f -
```

Create the file
`authority_key_identifier_to_mc_principal_map.textproto` with the appropriate MC
resource name. The AKID come from the MC certificate in
[secretfiles](../testing/secretfiles).

```prototext
# proto-file: src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto
# proto-message: AuthorityKeyToPrincipalMap
entries {
  authority_key_identifier: "\xE6\x3F\xEA\x65\xED\x71\x3D\x9E\x59\x79\xA0\xC8\x08\xC9\x57\xAA\xC6\xB1\x6A"
  principal_resource_name: "measurementConsumers/G7laM7LMIAA"
}
```

Create the file `encryption_key_pair_config.textproto` with the
content below, substituting the appropriate file names. The file names come from
the MC keys in [secretfiles](../testing/secretfiles).

```prototext
# proto-file: src/main/proto/wfa/measurement/config/reporting/encryption_key_pair_config.proto
# proto-message: EncryptionKeyPairConfig
key_pairs {
  key: "mc_enc_public.tink"
  value: "mc_enc_private.tink"
}
key_pairs {
  key: "mc_enc_public.tink"
  value: "mc_enc_private.tink"
}
```

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

## Create Secret for Reporting Server Postgres Database
You can use `kubectl` to create the `db-auth` secret. To reduce the likelihood 
of leaking your password, we read it in from STDIN.

Tip: Ctrl+D is the usual key combination for closing the input stream.

Assuming the database username is `db-user`, run:
```shell
kubectl create secret generic db-auth --type='kubernetes.io/basic/auth' \
  --append-hash \
  --from-file=password=/dev/stdin \
  --from-literal=username=db-user
```
Record the secret name for later steps.

## Deploy Reporting Server Postgres Database
This deploys the database for the Reporting Server.
```shell
bazel run //src/main/k8s/local:reporting_database_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=k8s_db_secret_name=db-auth-b286t5fcmt
```

## Create Secret for Reporting API Server
Create the file `/tmp/measurement_consumer_config.textproto` with the
content below, substituting the appropriate MC and certificate resource names, 
and the API key.

```prototext
# proto-file: src/main/proto/wfa/measurement/config/reporting/measurement_consumer_config.proto
# proto-message: MeasurementConsumerConfigs
configs {
  key: "measurementConsumers/OljiQHRz-E4"
  value: {
    api_key: "OljiQHRz-E4"
    signing_certificate_name: "certificates/OljiQHRz-E4"
    signing_private_key_path: "mc_cs_private.der"
  }
}
```

then use it to create a secret.
```shell
kubectl create secret generic mc-config \
  --append-hash \
  --from-literal=config=/tmp/measurement_consumer_config.textproto
```
Record the secret name for later steps.


## Deploy Reporting Server
This deploys the API server.
```shell
bazel run //src/main/k8s/local:reporting_kind \
  --define=k8s_secret_name=certs-and-configs-k8888kc6gg \
  --define=k8s_db_secret_name=db-auth-b286t5fcmt \
  --define=k8s_mc_config_secret_name=mc-config-975k88gktk
```
