/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.eventgroups

import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter

/**
 * OpenTelemetry metrics for Event Group synchronization.
 *
 * Tracks sync operations, success rates, and latency for syncing event groups between the EDP and
 * CMMS.
 *
 * Attributes:
 * - data_provider_name: The data provider performing the sync
 */
class EventGroupSyncMetrics(meter: Meter) {
  /**
   * Counter for total Event Group sync attempts.
   *
   * Incremented for each event group sync attempt, regardless of outcome.
   */
  val syncAttempts: LongCounter =
    meter
      .counterBuilder("edpa.event_group.sync_attempts")
      .setDescription("Number of Event Group sync attempts")
      .build()

  /**
   * Counter for successful Event Group syncs.
   *
   * Incremented only when an event group is successfully synced to the Kingdom.
   */
  val syncSuccess: LongCounter =
    meter
      .counterBuilder("edpa.event_group.sync_success")
      .setDescription("Number of successful Event Group syncs")
      .build()

  /**
   * Counter for failed Event Group syncs.
   *
   * Incremented when an event group sync fails due to API errors or other exceptions during sync
   * processing.
   */
  val syncFailure: LongCounter =
    meter
      .counterBuilder("edpa.event_group.sync_failure")
      .setDescription("Number of failed Event Group syncs")
      .build()

  /**
   * Counter for invalid Event Groups that fail validation.
   *
   * Incremented when an event group fails validation checks before sync processing begins.
   */
  val invalidEventGroupFailure: LongCounter =
    meter
      .counterBuilder("edpa.event_group.invalid_event_group_failure")
      .setDescription("Number of Event Groups that failed validation")
      .build()

  /**
   * Histogram for Event Group sync operation latency.
   *
   * Records the time taken to complete a single event group sync operation, from validation through
   * Kingdom API call completion.
   */
  val syncLatency: DoubleHistogram =
    meter
      .histogramBuilder("edpa.event_group.sync_latency")
      .setDescription("Time to complete Event Group sync operation")
      .setUnit("s")
      .build()

  /**
   * Counter for successfully deleted Event Groups.
   *
   * Incremented when an event group with DELETED state is successfully removed from CMMS.
   */
  val syncDeleted: LongCounter =
    meter
      .counterBuilder("edpa.event_group.sync_deleted")
      .setDescription("Number of Event Groups successfully deleted")
      .build()

  /**
   * Counter for unmapped Event Groups.
   *
   * Incremented when an event group cannot be resolved to any MeasurementConsumer. This can occur
   * when the direct measurementConsumer field is invalid and/or the clientAccountReferenceId lookup
   * returns no results.
   */
  val unmappedEventGroups: LongCounter =
    meter
      .counterBuilder("edpa.event_group.unmapped")
      .setDescription("Number of Event Groups that could not be mapped to any MeasurementConsumer")
      .build()

  /**
   * Counter for ClientAccount reference IDs not found in the account table.
   *
   * Incremented when a clientAccountReferenceId lookup returns no results, indicating the reference
   * ID is not mapped in the ClientAccounts table.
   */
  val unmappedClientAccounts: LongCounter =
    meter
      .counterBuilder("edpa.event_group.unmapped_client_accounts")
      .setDescription(
        "Number of ClientAccount reference IDs that are not mapped in the account table"
      )
      .build()
}
