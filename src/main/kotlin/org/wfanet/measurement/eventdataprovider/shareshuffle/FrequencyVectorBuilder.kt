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

package org.wfanet.measurement.eventdataprovider.shareshuffle

import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpec

/**
 * A utility for building a [FrequencyVector].
 *
 * This class is a utility for building an appropriately sized [FrequencyVector] for a given
 * [VidSamplingInterval], and the [PopulationSpec] associated with a given [VidIndexMap].
 *
 * This class does not provide a constructor that accepts a [PopulationSpec] because this class
 * would have to use it to create a [VidIndexMap], which is a reasonably expensive operation.
 * Therefore, the caller is expected to create the [VidIndexMap] and pass it in.
 *
 * @constructor Creates a [FrequencyVectorBuilder] for the given [VidIndeMap] and [PopulationSpec]
 */
class FrequencyVectorBuilder(
  val vidIndexMap: VidIndexMap,
  val vidSamplingInterval: VidSamplingInterval,
) {
 /** Construct a FrequencyVectorBuilder with an existing [FrequencyVector].
   *
   * @throws IllegalArgumentException if the [frequencyVector] is not compatible with
   * the [vidSamplingInterval].
   */
  constructor(vidIndexMap: VidIndexMap,
              vidSamplingInterval: VidSamplingInterval,
              frequencyVector: FrequencyVector) : this(vidIndexMap, vidSamplingInterval) {
    // TODO(@kungfucraig): Implement this method
  }

  /** Build a FrequencyVector. */
  fun build(): FrequencyVector {
    // TODO(@kungfucraig): Implement this method
    return FrequencyVector.newBuilder().build()
  }

  /**
   * Add a single VID to the [FrequencyVector]. If the [vid] is not contained by the
   * VidSamplingInterval it is ignored.
   */
  fun addVid(vid: Long) {
    // TODO(@kungfucraig): Implement this method
  }

  /**
   * Add all vids in the input Collection to the [FrequencyVector]. If any vid in the Collection is
   * not contained by the VidSamplingInterval it is ignored.
   */
  fun addVids(vids: Collection<Long>) {
    // TODO(@kungfucraig): Implement this method
  }

  /**
   * Add all vids in the [other] Builder to the [FrequencyVector].
   *
   * @throws [IllegalArgumentException] if the [VidSamplingInterval] of [other] is different from
   *   [this].
   */
  fun addVids(other: FrequencyVectorBuilder) {
    // TODO(@kungfucraig): Implement this method
  }
}
