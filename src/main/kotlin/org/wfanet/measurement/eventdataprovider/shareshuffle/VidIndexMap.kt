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

import org.wfanet.measurement.api.v2alpha.PopulationSpec

class VidNotFoundException(message: String) : Exception(message)

/**
 * A mapping of VIDs to [FrequencyVector] indexes for a [PopulationSpec].
 *
 * @constructor Creates a [VidIndexMap] for the given [PopulationSpec]
 */
class VidIndexMap(populationSpec: PopulationSpec) {
  /** The number of VIDs managed by this VidIndexMap */
  // TODO(kungfucraig): Implement this property
  val vidCount: Long = 0

  /**
   * Returns the index in the [FrequencyVector] for the given [vid].
   *
   * @throws VidNotFoundException if the [vid] does not exist in the map
   */
  fun getVidIndex(vid: Long): Int {
    // TODO(kungfucraig): Implement this method
    return 0
  }
}
