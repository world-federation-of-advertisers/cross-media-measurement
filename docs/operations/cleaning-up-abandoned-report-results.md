# Cleaning up abandoned report results

This guide describes the coordinated release of batched assembly and
owned-attempt recovery. New owned assembly attempts recover automatically.
`CleanupReportResults` is optional: use it to reclaim unowned, unattached result
trees left by older workers before upgrading to this release. No recurring
manual cleanup schedule is needed.

## Deployment order

Apply the Reporting Spanner migration, update all Reporting servers, then update
the assembly worker. Wait for older assembly writers and their in-flight RPCs
to finish before recording the environment's upgrade completion time.

## Optional cleanup of pre-upgrade rows

Dry run is the default and does not require pausing assembly. Run a preview if
pre-upgrade orphan rows are suspected; if none qualify, no deletion is needed.
Use the recorded upgrade completion time as a fixed `--created-before` cutoff
for both preview and deletion. The cutoff refers to the result tree's creation
time, independently of the dates covered by the report.

In this release, the CLI excludes owned attempts, even when their leases have
expired. Attached trees are preserved regardless of report state. Each deletion
rechecks the cutoff, absence of ownership, and association in a transaction and
deletes the parent and its children together.

**Pause and drain all assembly writers before deleting.** A creation cutoff
does not prove that an unowned tree is inactive. The CLI does not pause workers
or install a scheduled job.

## Maintenance procedure

### 1. Prepare the command and scope

Build the CLI from the repository root before pausing assembly:

```shell
bazel build //src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/gcloud/spanner/tools:CleanupReportResults
```

The following examples use Bash. Replace each placeholder with the intended
environment and consumer. Use Application Default Credentials authorized to
read and write the selected Reporting Spanner database.

```shell
report_cleanup_cli="$PWD/bazel-bin/src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/gcloud/spanner/tools/CleanupReportResults"
report_cleanup_context=CONTEXT
report_cleanup_namespace=NAMESPACE
report_cleanup_cronjob=CRONJOB_NAME
report_cleanup_args=(
  --spanner-project=PROJECT
  --spanner-instance=INSTANCE
  --spanner-database=DATABASE
  --cmms-measurement-consumer-id=CONSUMER_ID
  --created-before=2026-11-01T00:00:00Z
  --limit=1000
)

report_cleanup_kubectl() {
  kubectl --context="$report_cleanup_context" \
    --namespace="$report_cleanup_namespace" "$@"
}

report_cleanup_kubectl get cronjobs
```

Confirm that the Kubernetes deployment and Spanner database belong to the same
environment. Record their identifiers and the selected measurement consumer in
the maintenance log.

In the [Reporting manifest](../../src/main/k8s/reporting_v2.cue), assembly runs
in the `basic-reports-reports` init container of the
`report-result-post-processor` CronJob. Find its actual rendered Kubernetes
name; do not assume assembly has a separate CronJob. Include any additional
assembly deployments, manual jobs, or other writers sharing this database.

The example timestamp is illustrative. Replace it with the recorded upgrade
completion time for this environment, after older assembly writers have
finished. Keep that fixed RFC 3339 cutoff for every preview and deletion run.
The command also supports `--older-than`, but a moving cutoff is unnecessary
for this finite set of pre-upgrade rows. Specify exactly one cutoff.

### 2. Preview candidates

```shell
"$report_cleanup_cli" "${report_cleanup_args[@]}" --dry-run
```

Check the exit status, candidate external result IDs, creation times, and
summary. Capture stdout, stderr, and the exit status in the maintenance log.
An unknown measurement consumer causes a nonzero exit. If no candidates
qualify, stop here without pausing assembly.

Dry run is the default. It lists candidates without deleting rows. Preview
results can change before deletion; the command rechecks each candidate.

### 3. Pause new assembly and drain existing work

Read and record the original scheduling state before changing it:

```shell
report_cleanup_original_suspend="$(
  report_cleanup_kubectl get cronjob "$report_cleanup_cronjob" \
    -o jsonpath='{.spec.suspend}'
)"
```

Stop if the read fails. An absent `spec.suspend` means `false`. Save the
original value and resource identifiers in the maintenance log so another
operator can restore scheduling if this session is lost.

```shell
report_cleanup_original_suspend="${report_cleanup_original_suspend:-false}"

report_cleanup_kubectl patch cronjob "$report_cleanup_cronjob" \
  --type=merge --patch='{"spec":{"suspend":true}}'

report_cleanup_kubectl get cronjob "$report_cleanup_cronjob"
report_cleanup_kubectl get jobs,pods
```

Confirm the patch succeeded and scheduling is suspended. Prevent manual jobs
and other assembly writers from starting as well.

Wait for existing assembly jobs to finish and verify that their in-flight
assembly RPCs have ended, using worker and Reporting server logs or tracing.
Suspending a CronJob does not stop an existing Job. Terminating a worker does
not prove that its server RPCs have ended. Do not infer drainage from an empty
CronJob active list alone.

If drainage cannot be confirmed, skip deletion and restore scheduling using
step 5.

### 4. Delete within the maintenance window

Only after confirming the pause and drainage, run:

```shell
"$report_cleanup_cli" "${report_cleanup_args[@]}" \
  --dry-run=false --assembly-paused
```

`--assembly-paused` is the operator's confirmation that step 3 is complete.
It does not suspend Kubernetes jobs or check whether workers are still active.

Each invocation examines at most `--limit` candidates, in pages of 100,
oldest first. Record `examined`, `deleted`, `skipped`, `limit_reached`, and
the exit status. A skipped candidate was no longer eligible when rechecked.

If `limit_reached=true`, repeat a capped deletion run while assembly remains
paused and time remains in the maintenance window. Reaching the cap does not
prove that more candidates exist. A successful run with
`limit_reached=false` has exhausted eligible candidates observed by that run
for the selected consumer. Repeat for each explicitly selected consumer;
do not run cleanup concurrently.

A failure can leave earlier deletions committed. The command exits nonzero
and reports partial progress when candidate processing has started. Rerunning
safely handles remaining trees. Preserve the failure output and do not report
the failed run as successful.

### 5. Restore scheduling and check recovery

After cleanup has exited and any outstanding database operations have ended,
restore the original scheduling state, including after a cleanup failure:

```shell
report_cleanup_kubectl patch cronjob "$report_cleanup_cronjob" \
  --type=merge \
  --patch="{\"spec\":{\"suspend\":$report_cleanup_original_suspend}}"

report_cleanup_kubectl get cronjob "$report_cleanup_cronjob"
```

Restore every additional writer paused in step 3. Preserve an originally
suspended CronJob's state. If assembly was originally enabled, confirm that
the next scheduled job starts and reports continue progressing. Record the
cleanup totals and recovery checks. Escalate a failed restoration so assembly
is not left paused unnoticed.
