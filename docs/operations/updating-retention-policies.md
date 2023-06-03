# Updating Retention Policies

How to update retention policies within a CMMS deployment.

This guide assumes familiarity with Kubernetes and an existing CMMS instance
with a Kingdom either deployed locally
or through GKE.

See the [Local Kubernetes Configuration](../../src/main/k8s/local/README.md) or
the [Kingdom Deployment Guide](../../docs/gke/kingdom-deployment.md).

## Retention Policy Cronjobs

Retention policies are enforced through configured cronjobs which are enabled by
default on Kingdom deployments.

To get all configured cronjobs you can run the command:

```shell
kubectl get cronjobs 
```

You should see something like the following:

```
NAME                                      SCHEDULE    SUSPEND ACTIVE LAST SCHEDULE AGE
completed-measurements-deletion-cronjob   15 * * * *  False   0      52m           5h32m
exchanges-deletion-cronjob                40 6 * * *  False   0      10h27m        5h32m
pending-measurements-cancellation-cronjob 45 * * * *  False   0      22m           5h32m
```

The `schedule` specifies the frequency at which jobs are created for each
configuration and follows the
[Cron syntax](https://en.wikipedia.org/wiki/Cron). For example from above, the
cronjob `completed-measurements-deletion-cronjob` will create a Job hourly at 15
minutes past the hour.

Each cronjob manifest has two flags which control the retention behavior, a
`dry-run` flag and `time-to-live` or `days-to-live` flag. The `dry-run` flag
controls whether the jobs created execute the retention operations, i.e. when
`dry-run` is `true` a job will only log the operations that would have been
performed. The time to live flags control how long a Measurement or Exchange
must have been in specified state before qualifying for a retention operation.
The `time-to-live` flag is a ISO-8601 duration formatted string, while the
`days-to-live` flag is an int.

### Current Kingdom Retention Cronjobs

- **completed-measurements-deletion-cronjob**

  Measurements will be deleted if they are in a terminal state and the time
  since their last update time exceeds the `time-to-live` duration set.


- **exchanges-deletion-cronjob**

  Exchanges will be deleted if the days since their exchange date exceeds the
  amount of days set in `days-to-live` flag.


- **pending-measurements-cancellation-cronjob**

  Measurements will be transitioned to a `CANCELLED` state if they are in a
  non-terminal state and the time since their created time exceeds the
  `time-to-live` duration set.

## Modifying Retention Cronjobs

Modifications to a Cronjob will only update future Jobs created, but not any
currently running Jobs.

If you wish to modify a schedule, you can patch a cronjob with a command
like:

```shell
kubectl patch cronjob exchanges-deletion-cronjob  -p '''{"spec":{"schedule": "15 * * * *"}}'''
```

The args for setting a Job's `time-to-live` and `dry run` statuses are specified
as part of a list. Updating an arg requires finding its index and using a `JSON`
patch instead of the default `strategic merge` used above for scheduling
updates. This is recommended to avoid having to respecify all args while
desiring to
change only one.

See other [patching strategies](pending-measurements-cancellation-cronjob)
available.

The index of the arg to be modified for the specified cronjob can be identified
through the output of the command:

```shell
kubectl get cronjob completed-measurements-deletion-cronjob -o yaml
```

The resulting output should contain the args in a section like:

```
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - args:
            - --internal-api-target=$(GCP_KINGDOM_DATA_SERVER_SERVICE_HOST):$(GCP_KINGDOM_DATA_SERVER_SERVICE_PORT)
            - --internal-api-cert-host=localhost
            - --tls-cert-file=/var/run/secrets/files/kingdom_tls.pem
            - --tls-key-file=/var/run/secrets/files/kingdom_tls.key
            - --cert-collection-file=/var/run/secrets/files/all_root_certs.pem
            - --time-to-live=180d
            - --dry-run=false
            - --debug-verbose-grpc-client-logging=true
            - --otel-exporter-otlp-endpoint=http://default-collector-headless.default.svc:4317
            - --otel-service-name=completed-measurements-deletion-cronjob
```

To update the `time-to-live` argument, note the path to the argument and its
index - 5.

You can then apply a `JSON` patch using a command like (adding the `-o yaml`
flag to verify the changes):

```shell
kubectl patch --type=json cronjob completed-measurements-deletion-cronjob -p \
'''
[{ "op": "replace", 
"path":"/spec/jobTemplate/spec/template/spec/containers/0/args/5", 
"value":"--time-to-live=25d"}]
''' -o yaml
```
