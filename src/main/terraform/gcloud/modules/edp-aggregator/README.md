
# EDP Aggregator System

Edp Aggregator components for Google Cloud.
Optional [scheduled HTTP EventGroupSync](../event-group-sync-scheduler/README.md) is configured
with `event_group_sync_schedules` (empty by default).

Set `event_group_sync_memory` to the existing environment allocation before redeployment.
Leaving it unset preserves the existing deployment replacement keys; a new or redeployed
function still uses the legacy `512MB` allocation unless overridden.
