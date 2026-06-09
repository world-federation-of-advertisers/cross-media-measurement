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

package org.wfanet.measurement.loadtest.dataprovider

import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec

object ReferenceVidDataGeneration {

  fun generate(spec: ReferenceVidEventGroupSpec): Sequence<ReferenceVidRecord> = sequence {
    for (dateSpec in spec.dateSpecsList) {
      for (demoDist in dateSpec.demographicDistributionsList) {
        for (referenceVid in demoDist.idRange.start until demoDist.idRange.endExclusive) {
          yield(
            ReferenceVidRecord(
              referenceVid = referenceVid,
              gender = demoDist.gender,
              minAge = demoDist.minAge,
              maxAge = demoDist.maxAge,
              frequency = demoDist.frequency,
              nonPopulationFieldValues = spec.nonPopulationFieldValuesMap,
            )
          )
        }
      }
    }
  }
}
