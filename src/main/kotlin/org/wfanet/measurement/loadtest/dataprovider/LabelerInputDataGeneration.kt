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

import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpec.DateSpec

/**
 * Pure-data expansion of a [LabelerInputEventGroupSpec] into a flat stream of
 * [LabelerInputRecord]s.
 *
 * No labeling, no I/O. Each yielded record represents one labeler input ID in one
 * [DemographicDistribution]: its source ID, demographic, frequency, and non-population field
 * values. The total record count equals the sum of all idRange sizes across all DateSpecs.
 */
object LabelerInputDataGeneration {

  /** Flat expansion over all DateSpecs. */
  fun generate(spec: LabelerInputEventGroupSpec): Sequence<LabelerInputRecord> = sequence {
    for (dateSpec in spec.dateSpecsList) {
      yieldAll(generateForDateSpec(dateSpec))
    }
  }

  /** Per-DateSpec expansion. Useful when consumers need to keep records grouped by date. */
  fun generateForDateSpec(dateSpec: DateSpec): Sequence<LabelerInputRecord> = sequence {
    for (demoDist in dateSpec.demographicDistributionsList) {
      for (labelerInputId in demoDist.idRange.start until demoDist.idRange.endExclusive) {
        yield(
          LabelerInputRecord(
            labelerInputId = labelerInputId,
            demoBucket = demoDist.demoBucket,
            frequency = demoDist.frequency,
            nonPopulationFieldValues = demoDist.nonPopulationFieldValuesMap,
          )
        )
      }
    }
  }
}
