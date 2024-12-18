# Correctness Test on GKE

How to run the Kubernetes correctness test against a CMMS using simulators on
GKE.

## Overview

In order to run the correctness test, it is assumed that the CMMS instance has a
Kingdom as well as Duchies named `worker1`, `worker2`, and `aggregator`.

See the [Kingdom deployment guide](kingdom-deployment.md) and
[Duchy deployment guide](duchy-deployment.md).

Note: The test currently also assumes that the CMMS instance is using the
[testing secret files](kingdom-deployment.md#secret-files-for-testing).
Therefore, the correctness test cannot be run on a production CMMS instance.

## Run ResourceSetup

The `ResourceSetup` tool will create API resources for testing. If you have not
yet run the `ResourceSetup` tool against this CMMS instance, you will need to do
so. Note that this can only be done once per instance, and requires access to
the Kingdom cluster.

First, build the tool:

```shell
bazel build //src/main/kotlin/org/wfanet/measurement/loadtest/resourcesetup:ResourceSetup
```

We'll then need to be able to access the internal API from the host machine.
This can be done by forwarding the service port:

```shell
kubectl port-forward --address=localhost services/gcp-kingdom-data-server 9443:8443
```

Then run the tool, outputting to some directory (e.g. `/tmp/resource-setup`):

```shell
src/main/k8s/testing/resource_setup.sh \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  --kingdom-internal-api-target=localhost:9443 \
  --bazel-config-name=halo-dev \
  --output-dir=/tmp/resource-setup
```

Tip: The job will output a `resource-setup.bazelrc` file with `--define` options
that you can include in your `.bazelrc` file. You can then specify
`--config=halo-dev` to Bazel commands instead of those individual options.

### Update the Kingdom

After running the `ResourceSetup` tool, you will need to update the Kingdom
using its output. Copy the entries from the
`authority_key_identifier_to_principal_map.textproto` file output by the
`ResourceSetup` tool into your Kingdom Kustomization directory. You can then
apply the Kustomization to update the running Kingdom.

Assuming your KUBECONFIG is pointing at the Kingdom cluster, run the following
from the Kustomization directory:

```shell
kubectl apply -k src/main/k8s/dev/kingdom
```

## Deploy EDP simulators

See the [simulator deployment guide](simulator-deployment.md). The test assumes
that there are valid events in the range `[2021-03-15, 2021-03-17]`. The test
assumes that the event message type is
`wfa.measurement.api.v2alpha.event_templates.testing.TestEvent`.

## Run the correctness test

Run the following, substituting your own values:

```shell
bazel test //src/test/kotlin/org/wfanet/measurement/integration/k8s:SyntheticGeneratorCorrectnessTest \
--test_output=streamed \
--define=kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
--define=mc_name=measurementConsumers/Rcn7fKd25C8 \
--define=mc_api_key=W9q4zad246g \
--define=reporting_public_api_target=v2alpha.reporting.dev.halo-cmm.org:8443
```

The time the test takes depends on the size of the data set. With the default
synthetic generator configuration, this is about an hour. Eventually, you should
see logs like this:

```
Jan 27, 2022 12:47:01 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Created measurement measurementConsumers/TGWOaWehLQ8/measurements/Y6gTFpj__3g.
Jan 27, 2022 12:47:02 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Computation not done yet, wait for another 30 seconds.
Jan 27, 2022 12:47:32 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
...
...
Jan 27, 2022 12:52:33 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Got computed result from Kingdom: reach {
  value: 11542
}
frequency {
  relative_frequency_distribution {
    key: 1
    value: 0.2601439790575916
  }
  relative_frequency_distribution {
    key: 2
    value: 0.17981020942408377
  }
  ...
}
Jan 27, 2022 12:52:39 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Expected result: reach {
  value: 11570
}
frequency {
  relative_frequency_distribution {
    key: 1
    value: 0.25174145472217724
  }
  relative_frequency_distribution {
    key: 2
    value: 0.18078729953021222
  }
  ...
}
Jan 27, 2022 12:52:40 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Computed result is equal to the expected result. Correctness Test passes.
```

## How to monitor the computation after a measurement is created?

There are two places you can monitor the process of a measurement. The log of
various pods and the Kingdom Spanner table.

For monitoring purposes, we will mainly use the GCloud Spanner UI to query the
databases. If something is wrong, we will see logs to debug.

1.  Visit the GCloud console
    [spanner](https://console.cloud.google.com/spanner/instances) page.
2.  Select your instance
3.  Select the `kingdom` database.
4.  Click Query on the left

### Query the measurement status

```
SELECT
  MeasurementId,
  CASE State
    WHEN 1 THEN "PENDING_REQUISITION_PARAMS"
    WHEN 2 THEN "PENDING_REQUISITION_FULFILLMENT"
    WHEN 3 THEN "PENDING_PARTICIPANT_CONFIRMATION"
    WHEN 4 THEN "PENDING_COMPUTATION"
    WHEN 5 THEN "SUCCEEDED"
    WHEN 6 THEN "FAILED"
    WHEN 7 THEN "CANCELLED"
    ELSE "MEASUREMENT_STATE_UNKNOWN"
  END AS State,
  CAST(JSON_VALUE(MeasurementDetailsJson, '$.encryptedResult') AS STRING) AS Result,
FROM
  Measurements
```

Example result

![query-1](query-1.png)

Note that the final result is encrypted, so you won't be able to see the reach
and frequency result in plaintext.

### Query the requisition status

```
SELECT
  MeasurementId,
  RequisitionId,
  CASE State
    WHEN 1 THEN "PENDING_PARAMS"
    WHEN 2 THEN "UNFULFILLED"
    WHEN 3 THEN "FULFILLED"
    WHEN 4 THEN "REFUSED"
    ELSE "STATE_UNKNOWN"
  END AS State,
FROM
  Requisitions
```

Example result ![query-2](query-2.png)

If all requisitions are stuck at `UNFULFILLED` state for more than 1 minute,
then something is wrong.

### Query the MPC protocol progress

```
SELECT
  MeasurementId,
  FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%E2S", CreateTime) as CreateTime,
  CASE DuchyId
    WHEN 1234 THEN "Aggregator"
    WHEN 2345 THEN "Worker 1"
    WHEN 3456 THEN "Worker 2"
    ELSE "UNKNOWN"
  END as MpcWoker,
  JSON_VALUE(DuchyMeasurementLogDetailsJson,'$.stageAttempt.stageName') AS StageName,
  JSON_VALUE(DuchyMeasurementLogDetailsJson,'$.stageAttempt.attemptNumber') AS Attempt,
FROM DuchyMeasurementLogEntries
ORDER BY CreateTime DESC
```

Example result

![query-3](query-3.png)

## Troubleshooting

If anything is wrong, first check

1.  if the resource name in the commands is correct.
2.  if you have created the secret in all clusters and configmap in all clusters
    but the simulator cluster
3.  if you have set the DNS record for all kingdom and duchies public and system
    APIs. (In total, there are 8 of them).

### Requisition can not be fulfilled

Check the log of any EDP simulator, if the FulfillRequisition RPC fails, it is
highly likely that the IP address the simulator sends traffic to is not correct.
If you are reusing the same subdomain and are updating its IP address, the
update may not be effective for a long time. So the best practice is to create a
new Type A record instead of updating an existing one.
