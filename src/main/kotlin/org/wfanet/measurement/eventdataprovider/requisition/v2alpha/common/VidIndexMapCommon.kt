// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import java.nio.ByteOrder
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.common.toByteString

internal object VidIndexMapCommon {
  val HASH_SALT: ByteString = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)

  fun validatePopulationSpec(populationSpec: PopulationSpec) {
    PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()
  }

  fun hashVidToLongWithFarmHash(vid: Long, salt: ByteString = HASH_SALT): Long {
    val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
    return Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong()
  }

  @VisibleForTesting
  fun collectVids(populationSpec: PopulationSpec): IntArray {
    val totalVidCount =
      populationSpec.subpopulationsList.fold(0L) { acc, subPop ->
        acc +
          subPop.vidRangesList.fold(0L) { rangeAcc, range ->
            rangeAcc + (range.endVidInclusive - range.startVid + 1)
          }
      }

    require(totalVidCount <= Int.MAX_VALUE) { "Total VID count exceeds supported maximum." }
    val vids = IntArray(totalVidCount.toInt())
    var writeIndex = 0
    for (subPop in populationSpec.subpopulationsList) {
      for (range in subPop.vidRangesList) {
        for (vid in range.startVid..range.endVidInclusive) {
          require(vid < Integer.MAX_VALUE) {
            "VIDs must be less than ${Integer.MAX_VALUE}. Got ${vid}"
          }
          vids[writeIndex++] = vid.toInt()
        }
      }
    }
    return vids
  }
}
