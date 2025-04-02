# Updating Retention Policies

How to update retention policies within a CMMS deployment.

This guide assumes familiarity with Kubernetes and an existing CMMS instance
with a Kingdom either deployed locally or through GKE.

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
configuration and follows the [Cron syntax](https://en.wikipedia.org/wiki/Cron).
For example from above, the cronjob `completed-measurements-deletion-cronjob`
will create a Job hourly at 15 minutes past the hour.

Each cronjob manifest has two flags which control the retention behavior, a
`dry-run` flag and `time-to-live` or `days-to-live` flag. The `dry-run` flag
controls whether the jobs created execute the retention operations, i.e. when
`dry-run` is `true` a job will only log the operations that would have been
performed. The time to live flags control how long a Measurement or Exchange
must have been in specified state before qualifying for a retention operation.

### Current Kingdom Retention Cronjobs

-   **completed-measurements-deletion-cronjob**

    Measurements will be deleted if they are in a terminal state and the time
    since their last update time exceeds the `time-to-live` duration set.

-   **exchanges-deletion-cronjob**

    Exchanges will be deleted if the days since their exchange date exceeds the
    amount of days set in `days-to-live` flag.

-   **pending-measurements-cancellation-cronjob**

    Measurements will be transitioned to a `CANCELLED` state if they are in a
    non-terminal state and the time since their created time exceeds the
    `time-to-live` duration set.

### Modifying Retention Cronjobs

Modifications to a Cronjob will only update future Jobs created, but not any
currently running Jobs. These modifications can be applied to the
[`CronJobSpec`](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/cron-job-v1/#CronJobSpec)
for each cronjob you wish to modify.

-   The `schedule` field can be found in the CronJobSpec under the path
    `spec.schedule`, should look similar to `30 * * * *` and can be modified
    using the Cron syntax to update the frequency at which jobs are created to
    enforce the retention policy.

-   Other configurable options are passed as container args and can be found
    under the path `spec.jobTemplate.spec.template.spec.containers.0.args`.
    There you can update the `--dry-run=false` arg with a different boolean
    value to enable/disable the retention operations. You can also find the
    `--days-to-live` or `--time-to-live` arg which control the time allowed to
    pass before a retention procedure should apply. The `days-to-live` arg is
    specified as an int and the `time-to-live` arg is specified as a
    human-readable duration. See
    [Human-Readable Duration](#human-readable-duration) in the Appendix.

If you wish to modify a cronjob's `schedule`, `dry-run`, or `time-to-live`
settings, you can interactively modify the config with a command like:

```shell
kubectl edit cronjob exchanges-deletion-cronjob
```

You can then edit the fields described above and save desired changes using your
default text editor.

To modify cronjobs in a non-interactive manner, see
[`kubectl patch`](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/).

## Appendix

### Human-Readable Duration

Human-readable format consists of a sequence elements each consisting of a 
decimal number followed by a unit suffix. 
The valid suffixes are: 

* `d` - days 
* `h` - hours 
* `m` - minutes 
* `s` - seconds 
* `ms` - milliseconds 
* `ns` - nanoseconds 

For example, `3h50m` means 3 hours and 50 minutes.
