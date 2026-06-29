/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.eventgroupactivities

import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter

/**
 * OpenTelemetry metrics for EventGroup activity synchronization.
 *
 * Tracks created, deleted, and unchanged activity counts, the number of EventGroups processed, the
 * number of recoverable sync errors, the number of EventGroups whose deletions were skipped by the
 * data-loss guard, and the overall sync latency.
 *
 * Attributes:
 * - data_provider_name: The data provider performing the sync.
 * - error_type: For [syncErrors], the gRPC status code name or exception class that caused the
 *   failure.
 */
class EventGroupActivitySyncMetrics(meter: Meter) {
  /** Counter for EventGroupActivities created (present in input but missing in the Kingdom). */
  val activitiesCreated: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.activities_created")
      .setDescription("Number of EventGroupActivities created")
      .build()

  /** Counter for EventGroupActivities deleted (present in the Kingdom but missing from input). */
  val activitiesDeleted: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.activities_deleted")
      .setDescription("Number of EventGroupActivities deleted")
      .build()

  /** Counter for EventGroupActivities left unchanged (present in both input and the Kingdom). */
  val activitiesUnchanged: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.activities_unchanged")
      .setDescription("Number of EventGroupActivities left unchanged")
      .build()

  /** Counter for EventGroups successfully processed. */
  val eventGroupsProcessed: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.event_groups_processed")
      .setDescription("Number of EventGroups processed")
      .build()

  /** Counter for recoverable per-EventGroup sync errors, split by `error_type`. */
  val syncErrors: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.sync_errors")
      .setDescription("Number of EventGroups that failed to sync")
      .build()

  /**
   * Counter for EventGroups whose deletions were skipped because the fraction of existing
   * activities to delete exceeded the configured max delete fraction (data-loss guard).
   */
  val deletesSkippedGuard: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.deletes_skipped_guard")
      .setDescription("Number of EventGroups whose deletions were skipped by the max-delete guard")
      .build()

  /** Histogram for overall sync operation latency. */
  val syncLatency: DoubleHistogram =
    meter
      .histogramBuilder("edpa.event_group_activity.sync_latency")
      .setDescription("Time to complete the EventGroup activity sync operation")
      .setUnit("s")
      .build()
}
