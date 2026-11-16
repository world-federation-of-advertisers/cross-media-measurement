# Cleaning up abandoned report results

Report assembly writes a result tree in batches and attaches it to a BasicReport
after the last batch. A worker crash or a failed cleanup call can leave an
unattached tree behind. Use `CleanupReportResults` to reclaim these rows during
periodic maintenance.

The command deletes only trees created before the cutoff that are not attached
to **any** BasicReport. Attached trees are preserved regardless of report state.
Each deletion rechecks the cutoff and association in a transaction and deletes
the parent and its children together. The cutoff refers to the result tree's
creation time, independently of the dates covered by the report.

## Suggested schedule

Start with a **weekly** maintenance window, `--older-than=7d`, and
`--limit=1000` per measurement consumer. Adjust the cadence and limit based on
the number of abandoned trees and the time available for maintenance.

The age cutoff is a retention choice. It does not establish that an unattached
tree is inactive. **Pause and drain all assembly writers before deleting.**
Dry runs do not require pausing assembly.

The CLI does not install a scheduled job or pause workers. Use the procedure
below manually, or implement the same sequence in a maintenance coordinator.

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
  --older-than=7d
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

A relative cutoff is calculated at the start of each CLI invocation. To use the
exact same cutoff for preview and deletion, replace `--older-than=7d` with a
fixed RFC 3339 timestamp such as
`--created-before=2026-11-01T00:00:00Z`. Specify exactly one cutoff.

### 2. Pause new assembly and drain existing work

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

### 3. Preview candidates

```shell
"$report_cleanup_cli" "${report_cleanup_args[@]}" --dry-run
```

Check the exit status, candidate external result IDs, creation times, and
summary. Capture stdout, stderr, and the exit status in the maintenance log.
An unknown measurement consumer causes a nonzero exit.

Dry run is the default. It lists candidates without deleting rows. Preview
results can change before deletion; the command rechecks each candidate.

### 4. Delete within the maintenance window

Only after confirming the pause and drainage, run:

```shell
"$report_cleanup_cli" "${report_cleanup_args[@]}" \
  --dry-run=false --assembly-paused
```

`--assembly-paused` is the operator's confirmation that step 2 is complete.
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

Restore every additional writer paused in step 2. Preserve an originally
suspended CronJob's state. If assembly was originally enabled, confirm that
the next scheduled job starts and reports continue progressing. Record the
cleanup totals and recovery checks. Escalate a failed restoration so assembly
is not left paused unnoticed.

## Automating periodic maintenance

Use one coordinator for all assembly writers sharing the Spanner database.
Its ordered workflow should be:

1.  Acquire exclusive maintenance ownership and record the original scheduling
    state of every writer.
2.  Suspend scheduling, prevent other starts, and drain active assembly work.
    Use a bounded drain wait; if it expires, skip deletion.
3.  Run the CLI for the selected consumers with an age cutoff, a candidate
    limit, and a total maintenance time budget.
4.  Wait for cleanup and outstanding database operations to end.
5.  Restore the saved scheduling state on success, failure, or cancellation,
    verify recovery, and release maintenance ownership.

A maintenance CronJob can use `concurrencyPolicy: Forbid` to prevent its own
overlapping runs. That policy does not coordinate it with the processing
CronJob or manual jobs; the pause and drain sequence is still required.

Cancellation handling must stop and wait for cleanup before resuming assembly.
Do not use an exit or cancellation handler that immediately resumes workers
while the cleanup process is still running. Persist enough state for another
operator to recover after a coordinator crash, and alert if restoration fails.

Keep per-run logs and totals to tune the schedule. Increasing abandoned-tree
counts can indicate assembly crashes or cleanup failures that need investigation.
