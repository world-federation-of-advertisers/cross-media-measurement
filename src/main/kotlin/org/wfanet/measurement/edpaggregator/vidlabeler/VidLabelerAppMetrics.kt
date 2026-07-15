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

package org.wfanet.measurement.edpaggregator.vidlabeler

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry instruments used by [VidLabelerApp] for per-WorkItem orchestration.
 *
 * These cover the App-level driver layer (the per-WorkItem [VidLabelerApp.runWork]) rather than the
 * inner labeling math: WorkItem throughput, mark-RPC failures, done-blob count, and per-WorkItem
 * wall-clock. They integrate with `EdpaTelemetry`, so the labeler's per-WorkItem health surfaces on
 * operator dashboards (the App runs in the same TEE apps as the rest of the EDPA pipeline).
 * Counters and a duration histogram follow the EDPA convention established by
 * [RawImpressionSourceMetrics].
 *
 * @param meter the OpenTelemetry [Meter] used to create instruments.
 */
class VidLabelerAppMetrics(meter: Meter = Instrumentation.meter) {
  /** Counter for WorkItems processed to a successful completion. */
  val workItemsProcessedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler_app.work_items_processed")
      .setDescription("Number of VID-labeling WorkItems processed successfully")
      .build()

  /** Counter for failures of the `MarkVidLabelingJobSucceeded` RPC. */
  val markSucceededFailuresCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler_app.mark_succeeded_failures")
      .setDescription("Number of MarkVidLabelingJobSucceeded RPC failures")
      .build()

  /** Counter for non-benign failures of the `MarkRawImpressionUploadModelLineCompleted` RPC. */
  val markCompletedFailuresCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler_app.mark_completed_failures")
      .setDescription("Number of non-benign MarkRawImpressionUploadModelLineCompleted RPC failures")
      .build()

  /** Counter for `done` marker blobs written under the labeled-impressions prefix. */
  val doneBlobsWrittenCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler_app.done_blobs_written")
      .setDescription("Number of labeled-impressions done marker blobs written")
      .build()

  /** Wall time to process one VID-labeling WorkItem, in seconds. */
  val workItemDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler_app.work_item_duration")
      .setDescription("Wall time to process one VID-labeling WorkItem")
      .setUnit("s")
      .build()

  /** Attribute key for the `DataProvider` resource name. */
  val DATA_PROVIDER_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.vid_labeler_app.data_provider")

  /** Attribute key for the model line resource name. */
  val MODEL_LINE_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.vid_labeler_app.model_line")
}
