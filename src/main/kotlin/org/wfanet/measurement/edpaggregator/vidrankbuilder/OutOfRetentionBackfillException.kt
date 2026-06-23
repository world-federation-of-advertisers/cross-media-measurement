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

/**
 * Thrown when [SubpoolRanker] is asked to rank a backfill whose event date is older than the
 * retention window (`latest_max_event_date - max_event_date > retention_days`).
 *
 * By then the rank state in force at the backfilled date has already aged out, so the fingerprint's
 * original rank cannot be reclaimed (the design's "Problem 3", out of scope). Rather than silently
 * forward-appending — which would re-rank fingerprints the data provider still tracks as the same
 * person and corrupt produced labels — the ranker fails the dispatch loudly. The orchestrator
 * ([VidRankBuilder]) marks the `RankerJob` `FAILED`, halting the pipeline on this subpool so an
 * operator can extend retention, mark the dispatch skip-ranked, or backfill manually.
 *
 * @param poolOffset the subpool being ranked.
 * @param modelLine the model line being ranked.
 * @param gapDays `latest_max_event_date - max_event_date`, in days.
 * @param retentionDays the configured retention window, in days.
 */
class OutOfRetentionBackfillException(
  poolOffset: Long,
  modelLine: String,
  gapDays: Int,
  retentionDays: Int,
) :
  Exception(
    "Subpool $poolOffset for $modelLine: out-of-retention backfill " +
      "(gap=$gapDays days, retention=$retentionDays). " +
      "Forward-append would silently re-rank still-tracked fingerprints. " +
      "Extend retention or mark the dispatch as skip-ranked before retrying."
  )
