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
   * Current number of uploads in `CREATED` held back because another upload for the same
   * `(DataProvider, ModelLine)` is still in flight. Keyed by [DATA_PROVIDER_ATTR].
   *
   * A gauge (not a counter): this is the backlog observed on a run, not an event, so it must
   * reflect the current value (set each run, including `0`) rather than accumulate across runs.
   */
  val uploadsQueuedGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.uploads_queued")
        .setDescription("Uploads waiting to be dispatched behind an in-progress upload")
        .setUnit("{upload}")
        .ofLongs()
        .build()

  /**
   * Number of uploads dispatched (activated) over time. Keyed by [DATA_PROVIDER_ATTR]. A counter
   * because each dispatch is a discrete event whose cumulative total is meaningful.
   */
  val uploadsDispatchedCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.vid_labeling_monitor.uploads_dispatched")
        .setDescription("Uploads dispatched (WorkItems/PoolAssignmentJobs created) this run")
        .setUnit("{upload}")
        .build()

  /**
   * Current number of uploads in a non-terminal state past the staleness SLA. Keyed by
   * [DATA_PROVIDER_ATTR].
   *
   * A gauge (not a counter): a steady-state observation of how many uploads are stuck right now,
   * set each run (including `0`) so recovery reads as `0` rather than a counter that grows forever.
   */
  val uploadsStuckGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.uploads_stuck")
        .setDescription("Uploads in a non-terminal state beyond the configured SLA")
        .setUnit("{upload}")
        .ofLongs()
        .build()

  /**
   * Current number of uploads with a model line in `FAILED` beyond the failure threshold. Keyed by
   * [DATA_PROVIDER_ATTR].
   *
   * A gauge (not a counter): a steady-state observation set each run (including `0`), so a
   * recovered DataProvider reads as `0` instead of a counter that never decreases.
   */
  val failedUploadsGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.failed_uploads")
        .setDescription("Uploads with a model line in FAILED beyond the failure threshold")
        .setUnit("{upload}")
        .ofLongs()
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

  /**
   * Current number of `VidLabelingJob`s under a COMPLETED model line that did not reach SUCCEEDED
   * (CREATED or FAILED) this scan. Keyed by [DATA_PROVIDER_ATTR].
   *
   * Keyed off job state rather than a storage-blob census: the labeler marks a job SUCCEEDED when
   * it finishes whether or not it wrote output, so a model line that legitimately drops every
   * impression (e.g. a backfill outside the active window) reads as `0`, not a false positive.
   *
   * A gauge (not a counter): a steady-state data-quality observation, set each run (including `0`)
   * so a recovered DataProvider reads back to `0` rather than a counter that grows forever.
   */
  val missingLabeledOutputsGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.missing_labeled_outputs")
        .setDescription("VidLabelingJobs under a COMPLETED model line that did not reach SUCCEEDED")
        .setUnit("{job}")
        .ofLongs()
        .build()

  /**
   * Current number of raw impression files whose Cloud Storage create time is after the date
   * folder's `done` blob create time this scan (the EDP wrote `done` before all files were in
   * place). Keyed by [DATA_PROVIDER_ATTR].
   *
   * A gauge (not a counter): a steady-state data-quality observation, set each run (including `0`).
   */
  val lateArrivingFilesGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.late_arriving_files")
        .setDescription("Files uploaded after the date folder's done blob was written")
        .setUnit("{file}")
        .ofLongs()
        .build()

  /**
   * Current number of date folders that exist but have no `done` blob this scan (a partial or
   * incomplete upload). Keyed by [DATA_PROVIDER_ATTR].
   *
   * A gauge (not a counter): a steady-state data-quality observation, set each run (including `0`).
   */
  val missingDoneBlobsGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.missing_done_blobs")
        .setDescription("Date folders that exist but have no done blob")
        .setUnit("{date}")
        .ofLongs()
        .build()

  /**
   * Current number of date folders whose `done` blob exists but that contain no data files this
   * scan. Keyed by [DATA_PROVIDER_ATTR].
   *
   * A gauge (not a counter): a steady-state data-quality observation, set each run (including `0`).
   */
  val zeroImpressionDatesGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.vid_labeling_monitor.zero_impression_dates")
        .setDescription("Date folders whose done blob exists but that contain no data files")
        .setUnit("{date}")
        .ofLongs()
        .build()

  /**
   * Number of stuck phase transitions re-triggered by the Monitor over time. Keyed by
   * [DATA_PROVIDER_ATTR]. A counter because each recovery is a discrete event whose cumulative
   * total is meaningful.
   */
  val phaseTransitionsRecoveredCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.vid_labeling_monitor.phase_transitions_recovered")
        .setDescription("Stuck phase transitions re-triggered by the Monitor")
        .setUnit("{transition}")
        .build()

  /** Attribute key for the `DataProvider` resource name. */
  val DATA_PROVIDER_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.vid_labeling_monitor.data_provider")
}
