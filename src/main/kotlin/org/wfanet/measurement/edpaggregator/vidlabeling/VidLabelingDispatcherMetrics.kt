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

import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * Encapsulates the OpenTelemetry instruments used by [VidLabelingDispatcher].
 *
 * @param meter the OpenTelemetry [Meter] used to create instruments.
 */
class VidLabelingDispatcherMetrics(meter: Meter = Instrumentation.meter) {
  /** Histogram recording the duration of each dispatch cycle in seconds. */
  val dispatchDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeling_dispatcher.dispatch_duration")
      .setDescription("Duration of the VID labeling dispatch cycle")
      .setUnit("s")
      .build()

  /** Counter for the number of VID labeling batches created. */
  val batchesCreatedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeling_dispatcher.batches_created")
      .setDescription("Number of VID labeling batches created")
      .build()

  /** Counter for the number of raw impression files processed. */
  val filesProcessedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeling_dispatcher.files_processed")
      .setDescription("Number of raw impression files processed")
      .build()

  /** Counter for the number of files exceeding the batch max size. */
  val oversizedFileAlertsCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_labeling_dispatcher.oversized_file_alerts")
      .setDescription("Number of files exceeding the batch max size")
      .build()
}
