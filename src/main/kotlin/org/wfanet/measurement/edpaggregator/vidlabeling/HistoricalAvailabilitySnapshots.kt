// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload

/** Validates snapshot chronology for an unaffected historical availability predecessor. */
object HistoricalAvailabilitySnapshots {
  /**
   * Whether all live [ownerSnapshots] predate the approved original [source]'s rank computation.
   *
   * [sourceSnapshots] includes deleted checkpoints. The same-pool boundary takes precedence; an
   * absent pool uses the first source checkpoint, then source creation if it never wrote a
   * checkpoint. Callers must separately validate current owner state, operation scope, and output
   * key ownership.
   */
  fun hasHistoricalBasis(
    source: RawImpressionUpload,
    sourceSnapshots: List<RankIndexBlob>,
    ownerSnapshots: List<RankIndexBlob>,
  ): Boolean {
    if (!source.registrationComplete || !source.hasCreateTime() || ownerSnapshots.isEmpty())
      return false
    if (sourceSnapshots.any { !it.hasCreateTime() }) return false
    val firstSourceCheckpoint =
      sourceSnapshots
        .minWithOrNull { a, b -> Timestamps.compare(a.createTime, b.createTime) }
        ?.createTime
    val boundaries =
      sourceSnapshots
        .groupBy { it.poolOffset }
        .mapValues { (_, rows) ->
          rows.minWith { a, b -> Timestamps.compare(a.createTime, b.createTime) }.createTime
        }
    return ownerSnapshots.all {
      it.hasCreateTime() &&
        Timestamps.compare(
          it.createTime,
          boundaries[it.poolOffset] ?: firstSourceCheckpoint ?: source.createTime,
        ) < 0
    }
  }
}
