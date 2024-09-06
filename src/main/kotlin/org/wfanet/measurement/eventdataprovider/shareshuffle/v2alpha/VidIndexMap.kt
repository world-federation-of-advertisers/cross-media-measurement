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

package org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha

import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntry

class VidNotFoundException(vid: Long) : Exception("Failed to find VID $vid.")

/** A mapping of VIDs to [FrequencyVector] indexes for a [PopulationSpec]. */
interface VidIndexMap {
  /** Gets the index in the [FrequencyVector] for the given VID */
  operator fun get(vid: Long): Int

  /** The number of VIDs managed by this VidIndexMap */
  val size: Long

  /** The PopulationSpec used to create this map */
  val populationSpec: PopulationSpec

  /**
   * Get an iterator for the VidIndexMapEntries of this VidIndexMap.
   *
   * The order of iteration is undefined.
   */
  operator fun iterator(): Iterator<VidIndexMapEntry>

  companion object {
    val EMPTY: VidIndexMap =
      object : VidIndexMap {
        override fun get(vid: Long): Int = throw VidNotFoundException(vid)

        override val size: Long = 0L

        override val populationSpec: PopulationSpec
          get() = PopulationSpec.getDefaultInstance()

        override fun iterator(): Iterator<VidIndexMapEntry> =
          emptyList<VidIndexMapEntry>().iterator()
      }
  }
}
