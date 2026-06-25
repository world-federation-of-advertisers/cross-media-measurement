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
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry instruments for [VidLabeler] and [VidLabelingSink].
 *
 * The [meter] is constructor-injected so tests can attach an in-memory reader and assert that the
 * right counters fire on each path. Every counter is keyed by [DATA_PROVIDER_KEY] and
 * [MODEL_LINE_KEY]; [impressionsDroppedCounter] additionally carries [DROP_REASON_KEY].
 *
 * @param meter the OpenTelemetry [Meter] to use for creating instruments.
 */
class VidLabelerMetrics(meter: Meter = Instrumentation.meter) {

  /** Labeled impressions emitted — one increment per assigned virtual person (co-viewing aware). */
  val impressionsLabeledCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler.impressions_labeled")
      .setDescription("Labeled impressions emitted, by data provider and model line")
      .setUnit("{impression}")
      .build()

  /** Source impressions dropped before producing any labeled impression, by [DROP_REASON_KEY]. */
  val impressionsDroppedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler.impressions_dropped")
      .setDescription(
        "Source impressions dropped before labeling, by data provider, model line, and reason"
      )
      .setUnit("{impression}")
      .build()

  /** Labeled-output blobs successfully written (data blob + metadata sidecar). */
  val blobsWrittenCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler.blobs_written")
      .setDescription("Labeled-output blobs written, by data provider and model line")
      .setUnit("{blob}")
      .build()

  /** Labeling failures — a `processBatch` or `writeGroup` threw. */
  val labelingErrorsCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeler.labeling_errors")
      .setDescription("Labeling failures, by data provider and model line")
      .setUnit("{error}")
      .build()

  companion object {
    val DATA_PROVIDER_KEY: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeler.data_provider")
    val MODEL_LINE_KEY: AttributeKey<String> = AttributeKey.stringKey("edpa.vid_labeler.model_line")
    val DROP_REASON_KEY: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeler.drop_reason")

    /** A row the [ImpressionConverter] returned `null` for (unparseable/filtered at the seam). */
    const val DROP_REASON_CONVERTER_SKIP = "converter_skip"
    /** A row whose event time fell outside the model line's active window. */
    const val DROP_REASON_OUTSIDE_WINDOW = "outside_window"
    /** A row the labeler assigned zero virtual people to. */
    const val DROP_REASON_NO_ASSIGNMENT = "no_assignment"
  }
}
