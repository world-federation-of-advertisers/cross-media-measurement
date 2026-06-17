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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry instruments for the Phase-1 ranker ([SubpoolRanker] / [VidRankBuilder]).
 *
 * Mirrors `SubpoolAssignerMetrics`: counters integrate with `EdpaTelemetry` so rank-allocation
 * rates surface on operator dashboards. The in-process [SubpoolRanker.Result] tallies are kept
 * separately for the completion log because OpenTelemetry counters are not readable in-process.
 *
 * @param meter the OpenTelemetry [Meter] used to create instruments.
 */
class VidRankBuilderMetrics(meter: Meter = Instrumentation.meter) {
  /** Ranks newly assigned to never-before-seen fingerprints. */
  val ranksAllocatedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_rank_builder.ranks_allocated")
      .setDescription("Ranks newly assigned to never-before-seen fingerprints")
      .build()

  /** Already-ranked fingerprints re-observed this dispatch (rank preserved). */
  val ranksRenewedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_rank_builder.ranks_renewed")
      .setDescription("Already-ranked fingerprints re-observed this dispatch")
      .build()

  /** Ranks freed by the retention pass. */
  val ranksFreedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_rank_builder.ranks_freed")
      .setDescription("Ranks freed by the retention pass")
      .build()

  /** Fingerprints left unranked because their subpool reached `ranked_size`. */
  val overflowFingerprintsCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_rank_builder.overflow_fingerprints")
      .setDescription("Fingerprints left unranked because the subpool was full")
      .build()

  /** Subpools ranked. */
  val subpoolsRankedCounter: LongCounter =
    meter
      .counterBuilder("edpa.vid_rank_builder.subpools_ranked")
      .setDescription("Number of subpools ranked")
      .build()
}
