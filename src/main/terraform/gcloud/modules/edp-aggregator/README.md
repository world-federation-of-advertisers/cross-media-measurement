
# EDP Aggregator System

Edp Aggregator components for Google Cloud.
Optional [scheduled EventGroupSync caller jobs](../event-group-sync-job/README.md) are configured
with `event_group_sync_jobs` (empty by default).

Set `event_group_sync_memory` to the existing environment allocation before redeployment. Its default remains `512MB`; larger inputs need an explicitly sized allocation.
