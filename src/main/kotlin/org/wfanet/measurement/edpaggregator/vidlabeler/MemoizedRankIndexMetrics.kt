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
import io.opentelemetry.api.metrics.LongGauge
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry instruments used by [MemoizedRankIndex] for the Phase-2 memoized-index cold start.
 *
 * These cover the one-time, per-WorkItem [MemoizedRankIndex.load] that pulls the per-subpool rank
 * maps into heap. They integrate with `EdpaTelemetry`, so subpool count, cold-start latency, and
 * per-subpool sizing surface on operator dashboards (the labeler runs in the same TEE apps as the
 * rest of the EDPA pipeline).
 *
 * Instrument choice follows the EDPA convention: a state observation (current subpool count,
 * per-subpool entry/ranked size) is a [LongGauge] set once per load; a latency is a
 * [DoubleHistogram]. Alerting is configured in Cloud Monitoring rather than here: the subpool count
 * is model-line-dependent (one entry per `RankedPopulationNode` leaf), so there is no fixed count
 * to assert in code. The actionable alerts are cold-start-duration regression and `entry_count /
 * ranked_size` saturation (surplus fingerprints fall back to the hash path).
 *
 * @param meter the OpenTelemetry [Meter] used to create instruments.
 */
class MemoizedRankIndexMetrics(meter: Meter = Instrumentation.meter) {
  /** Number of subpools loaded into heap by one [MemoizedRankIndex.load]. */
  val subpoolCountGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.vid_labeler.memoized_index.subpool_count")
      .setDescription("Number of subpools loaded into heap by this VM")
      .setUnit("{subpool}")
      .ofLongs()
      .build()

  /** Wall time to load (list + decrypt + decode) the full rank index, in seconds. */
  val coldStartDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.vid_labeler.memoized_index.cold_start_duration")
      .setDescription("Wall time to load the per-subpool rank index into heap")
      .setUnit("s")
      .build()

  /**
   * Number of ranked fingerprints loaded for one subpool. Keyed additionally by [POOL_OFFSET_ATTR].
   */
  val entryCountGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.vid_labeler.memoized_index.entry_count")
      .setDescription("Number of ranked fingerprints loaded for a subpool")
      .setUnit("{fingerprint}")
      .ofLongs()
      .build()

  /**
   * Configured ranked sub-range size for one subpool, from the model. Keyed additionally by
   * [POOL_OFFSET_ATTR]. Pairs with [entryCountGauge]: `entry_count / ranked_size` approaching 1
   * means the subpool is saturating and surplus fingerprints fall back to the hash path.
   */
  val rankedSizeGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.vid_labeler.memoized_index.ranked_size")
      .setDescription("Configured ranked sub-range size for a subpool")
      .setUnit("{rank}")
      .ofLongs()
      .build()

  /** Attribute key for the `DataProvider` resource name. */
  val DATA_PROVIDER_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.vid_labeler.memoized_index.data_provider")

  /** Attribute key for the model line resource name. */
  val MODEL_LINE_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.vid_labeler.memoized_index.model_line")

  /** Attribute key for a subpool's `pool_offset`. */
  val POOL_OFFSET_ATTR: AttributeKey<Long> =
    AttributeKey.longKey("edpa.vid_labeler.memoized_index.pool_offset")
}
