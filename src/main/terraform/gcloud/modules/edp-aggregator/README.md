
# EDP Aggregator System

Edp Aggregator components for Google Cloud.
## Durable EventGroupSync WorkItems

Event-group finalize routes enqueue a generation-specific `WorkItem` on
`event-group-sync-queue`. DataWatcher returns after the control plane durably accepts the task.
The Pub/Sub push consumer claims an attempt, renews its lease, and records success after the
mapping is published. Redelivery of a succeeded task does not repeat the sync. An active task
remains protected by its lease even if push delivery expires or reaches the DLQ listener.

Deploy the Secure Computation lease-aware API and updated queue registry first, then the EGS
consumer and queue IAM, and finally the DataWatcher routes. Drain legacy HTTP deliveries before
switching. Do not configure Scheduler and DataWatcher to run EGS for the same provider.

Each route uses `EventGroupSyncParams` and a stable `serialization_key` per provider. Keep keys
distinct from data availability tasks. The function retains one single-request instance.
Ordered publication and attempt leases protect redeliveries; they do not promise exactly-once
side effects after a crash between publishing a mapping and recording task completion.

The source must remain the current generation at the configured input directory. EGS downloads
that exact generation and checks it again before publishing the mapping. Superseded sources are
terminal failures; the newer finalize event supplies the current task. The existing HTTP JSON
formats remain supported for migration and manual invocations.

The consumer uses `SECURE_COMPUTATION_CONTROL_PLANE_TARGET`, optional
`SECURE_COMPUTATION_CERT_HOST`, and the `SECURE_COMPUTATION_CERT_FILE`,
`SECURE_COMPUTATION_PRIVATE_KEY_FILE`, and `SECURE_COMPUTATION_CERT_COLLECTION_FILE` paths for a
Secure Computation client identity. The provider's `cmms_connection` remains exclusive to Kingdom.
Terraform mounts both trust-domain identities and the control-plane CA. Monitor WorkItem states and
mapping freshness alongside push/DLQ metrics: delivery failures are distinct from synchronization
outcomes.

For queued GCS inputs, configure each provider's `event_groups_blob_uri` with its canonical
input object URI. The consumer restricts incoming sources to that bucket and directory and
reads the exact finalized generation. Update this URI when moving the input location.

Set `event_group_sync_memory` to the existing environment allocation before redeployment.
Leaving it unset preserves the existing deployment replacement keys; a new or redeployed
function still uses the legacy `512MB` allocation unless overridden.
