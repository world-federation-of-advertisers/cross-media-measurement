# Local Kubernetes Deployment

How to deploy system components to a local Kubernetes cluster running in
[KiND](https://kind.sigs.k8s.io/).

This assumes that you have `kubectl` installed and configured to point to a
local KiND cluster. You should have some familiarity with Kubernetes and
`kubectl`.

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
Use the following command to save the secret name in an environment variable
for later use.

```shell
export HALO_SECRETNAME=`kubectl get secrets | grep 'certs-and-configs' | awk '{ print $1; }'`
```

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
  --define=k8s_secret_name=$HALO_SECRETNAME
```

## Resource Setup

There is a chicken-and-egg problem with setting up initial resources, in that
resource setup is done by calling Kingdom services but some Kingdom services
depend on resource configuration. Therefore, we have to deploy the Kingdom for
resource setup and then update configurations and restart some Kingdom services.

```shell
bazel run //src/main/k8s/local:kingdom_kind \
  --define=k8s_secret_name=$HALO_SECRETNAME
bazel run //src/main/k8s/local:resource_setup_kind \
  --define=k8s_secret_name=$HALO_SECRETNAME
```

After the resource setup job has completed, you can obtain the created resource
names from its logs.

```shell
kubectl logs jobs/resource-setup-job
```

After this step is complete, execute the `get_resource_ids` script to extract
the resource names into environment variables:

```shell
source src/main/k8s/local/get_resource_ids.sh
```

Check that the resource ids were properly extracted with the following command
```shell
export -p | grep HALO
```

You should see output similar to the following:

```
declare -x HALO_AGGREGATORCERT="duchies/aggregator/certificates/ZTOFCs6PiNU"
declare -ax HALO_DATAPROVIDERS=([0]="dataProviders/cav1ejFwcuQ" [1]="dataProviders/ayLuhs6PjFo" [2]="dataProviders/S6pNOjFwdG0" [3]="dataProviders/BCFda86Pisw" [4]="dataProviders/UNZeJDFwdf8" [5]="dataProviders/UjjosTFwdmI")
declare -x HALO_MC="measurementConsumers/PFU08s6PkWU"
declare -x HALO_MC_APIKEY="EapJic6Pjxs"
declare -x HALO_SECRETNAME="certs-and-configs-7kgbg7g2t7"
declare -x HALO_WORKER1CERT="duchies/worker1/certificates/KEPD-c6PiG0"
declare -x HALO_WORKER2CERT="duchies/worker2/certificates/OGAPF86PiAk"
```

If any of these environment variables are missing or empty, you will need to go back
and figure out what went wrong.

### Update `config-files` ConfigMap

After resource-setup-job has completed, we can fill in the config files and
update the `config-files` ConfigMap.  Use the following script:

```shell
bazel run //src/main/k8s/local:build_authority_key_identifier_to_principal_map
```

Now update the ConfigMap:

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
  --from-file=/tmp/authority_key_identifier_to_principal_map.textproto \
  | kubectl replace -f -
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
  --define=k8s_secret_name=$HALO_SECRETNAME
```

## Deploy Duchies

The testing environment uses three Duchies: an aggregator and two workers, named
`aggregator`, `worker1`, and `worker2` respectively. The encryption libraries require
a specific version of glibc, and so the `tools/bazel-container-run` script must be
used.  The full command line is as follows:

```shell
tools/bazel-container-run //src/main/k8s/local:duchies_kind \
  --define=k8s_secret_name=$HALO_SECRETNAME \
  --define=aggregator_cert_name=$HALO_AGGREGATORCERT \
  --define=worker1_cert_name=$HALO_WORKER1CERT \
  --define=worker2_cert_name=$HALO_WORKER2CERT
```

## Deploy EDP Simulators

The testing environment simulates six DataProviders, named `edp1` through
`edp6`. Here too `tools/bazel-container-run` must be used:

```shell
tools/bazel-container-run //src/main/k8s/local:edp_simulators_kind \
  --define=k8s_secret_name=$HALO_SECRETNAME \
  --define=mc_name=$HALO_MC \
  --define=edp1_name=${HALO_DATAPROVIDERS[0]} \
  --define=edp2_name=${HALO_DATAPROVIDERS[1]} \
  --define=edp3_name=${HALO_DATAPROVIDERS[2]} \
  --define=edp4_name=${HALO_DATAPROVIDERS[3]} \
  --define=edp5_name=${HALO_DATAPROVIDERS[4]} \
  --define=edp6_name=${HALO_DATAPROVIDERS[5]}
```

## Deploy MC Frontend Simulator

This is a job that tests correctness by creating a Measurement and validating
the result.

```shell
tools/bazel-container-run //src/main/k8s/local:mc_frontend_simulator_kind \
  --define=k8s_secret_name=$HALO_SECRETNAME \
  --define=mc_name=$HALO_MC \
  --define=mc_api_key=$HALO_MC_APIKEY
```

To see the results of the correctness test:

```shell
kubectl logs -f jobs/frontend-simulator-job
```
