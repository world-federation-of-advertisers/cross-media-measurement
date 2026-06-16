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

package org.wfanet.measurement.edpaggregator.vidlabeling

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongGauge
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry instruments used by [VidLabelingMonitor].
 *
 * Mirrors the `DataAvailabilityMonitorMetrics` pattern. Only the instruments used by the dispatch
 * sequencing + failure/staleness checks are defined here; phase-transition-recovery and
 * data-quality instruments are added with their respective checks in follow-up PRs.
 */
object VidLabelingMonitorMetrics {
  /**
   * Number of uploads in `CREATED` that were held back this run because another upload for the same
   * `DataProvider` is still `ACTIVE` (sequential processing). Keyed by [DATA_PROVIDER_ATTR].
   */
  val uploadsQueuedCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.vid_labeling_monitor.uploads_queued")
        .setDescription("Uploads waiting to be dispatched behind an in-progress upload")
        .setUnit("{upload}")
        .build()

  /**
   * Number of uploads found dispatched (activated) this run. Keyed by [DATA_PROVIDER_ATTR].
   */
  val uploadsDispatchedCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.vid_labeling_monitor.uploads_dispatched")
        .setDescription("Uploads dispatched (WorkItems/PoolAssignmentJobs created) this run")
        .setUnit("{upload}")
        .build()

  /**
   * Number of uploads in a non-terminal state past the staleness SLA. Keyed by [DATA_PROVIDER_ATTR].
   */
  val uploadsStuckCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.vid_labeling_monitor.uploads_stuck")
        .setDescription("Uploads in a non-terminal state beyond the configured SLA")
        .setUnit("{upload}")
        .build()

  /**
   * Number of uploads with a model line in `FAILED` beyond the failure threshold. Keyed by
   * [DATA_PROVIDER_ATTR].
   */
  val failedUploadsCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.vid_labeling_monitor.failed_uploads")
        .setDescription("Uploads with a model line in FAILED beyond the failure threshold")
        .setUnit("{upload}")
        .build()

  /**
   * Whether dispatch failed for a `DataProvider` on the most recent run: `1` when
   * [VidLabelingMonitor.run] caught an exception from the dispatch sequencer, `0` otherwise. Keyed
   * by [DATA_PROVIDER_ATTR]. A gauge (not a counter) so a stuck EDP reads as a steady `1` rather
   * than relying on per-tick `SEVERE` logs, and recovery reads as `0` on the next run.
   */
  val dispatchErrorsGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.dispatch_errors")
        .setDescription("Whether dispatch failed for a DataProvider on the most recent run")
        .ofLongs()
        .build()

  /** Attribute key for the `DataProvider` resource name. */
  val DATA_PROVIDER_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.vid_labeling_monitor.data_provider")
}
