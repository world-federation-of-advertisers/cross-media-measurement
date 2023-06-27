/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.validation

import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SimulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.common.toLocalDate

class SyntheticDataSpecValidationException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

fun SimulatorSyntheticDataSpec.validate() {
  val source = this
  if (!source.hasPopulation()) {
    throw SyntheticDataSpecValidationException(
      "Invalid simulatorSyntheticDataSpec",
      IllegalArgumentException("simulatorSyntheticDataSpec does not define a population.")
    )
  }
  if (eventGroupSpecCount == 0) {
    throw SyntheticDataSpecValidationException(
      "Invalid simulatorSyntheticDataSpec",
      IllegalArgumentException("simulatorSyntheticDataSpec does not define any eventgroup specs.")
    )
  }
}

fun SyntheticPopulationSpec.validate() {
  val source = this
  if (source.subPopulationsCount > 1) {
    val sortedSubPopulations = source.subPopulationsList.sortedBy { it.vidSubRange.start }
    var end = sortedSubPopulations[0].vidSubRange.endExclusive
    for (i in 1 until sortedSubPopulations.size) {
      if (sortedSubPopulations[i].vidSubRange.start < end) {
        throw SyntheticDataSpecValidationException(
          "Invalid syntheticPopulationSpec",
          IllegalArgumentException("subpopulation vid ranges cannot overlap.")
        )
      }
      end = sortedSubPopulations[i].vidSubRange.endExclusive
    }
    val populationFieldValues = mutableSetOf<String>()
    for (subPopulation in sortedSubPopulations) {
      if (populationFieldValues.contains(subPopulation.populationFieldsValuesMap.toString())) {
        throw SyntheticDataSpecValidationException(
          "Invalid syntheticPopulationSpec",
          IllegalArgumentException("subpopulations cannot have equal population field values.")
        )
      }
      populationFieldValues.add(subPopulation.populationFieldsValuesMap.toString())
    }
  }
}

fun SyntheticPopulationSpec.SubPopulation.validate(syntheticPopulation: SyntheticPopulationSpec) {
  val source = this
  if (source.populationFieldsValuesCount != syntheticPopulation.populationFieldsCount ||
    source.populationFieldsValuesMap.keys != syntheticPopulation.populationFieldsList.toSet()) {
    throw SyntheticDataSpecValidationException(
      "Invalid subpopulation",
      IllegalArgumentException("invalid population field values.")
    )
  }
  return
}
fun SyntheticEventGroupSpec.validate() {
  val source = this
  if (source.dateSpecsCount == 0) {
    throw SyntheticDataSpecValidationException(
      "Invalid syntheticEventGroupSpec",
      IllegalArgumentException("no dateSpecs defined.")
    )
  }
  val sortedDateSpecs = source.dateSpecsList.sortedWith(
    compareBy({it.dateRange.start.year}, {it.dateRange.start.month}, {it.dateRange.start.day}))
  var end = sortedDateSpecs[0].dateRange.endExclusive
  for (i in 1 until sortedDateSpecs.size) {
    if (sortedDateSpecs[i].dateRange.start.toLocalDate() < end.toLocalDate()) {
      throw SyntheticDataSpecValidationException(
        "Invalid syntheticEventGroupSpec",
        IllegalArgumentException("dateSpecs cannot overlap.")
      )
    }
    end = sortedDateSpecs[i].dateRange.endExclusive
  }

}

fun SyntheticEventGroupSpec.DateSpec.validate() {
  val source = this
  if (source.frequencySpecsCount == 0) {
    throw SyntheticDataSpecValidationException(
      "Invalid dateSpec",
      IllegalArgumentException("no frequencySpecs defined.")
    )
  }
  val frequencies = mutableSetOf<Int>()
  for (frequencySpec in source.frequencySpecsList) {
    if (frequencies.contains(frequencySpec.frequency.toInt())) {
      throw SyntheticDataSpecValidationException(
        "Invalid dateSpec",
        IllegalArgumentException("single frequency repeatedly defined for single dateSpec.")
      )
    }
    frequencies.add(frequencySpec.frequency.toInt())
  }
}


fun SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec.validate(syntheticPopulation: SyntheticPopulationSpec) {
  val source = this
  if (source.nonPopulationFieldValuesCount != syntheticPopulation.nonPopulationFieldsCount ||
    source.nonPopulationFieldValuesMap.keys != syntheticPopulation.nonPopulationFieldsList.toSet()) {
    throw SyntheticDataSpecValidationException(
      "Invalid vidRangeSpec",
      IllegalArgumentException("invalid non-population field values.")
    )
  }
  for (subPopulation in syntheticPopulation.subPopulationsList) {
    if (source.vidRange.start >= subPopulation.vidSubRange.start &&
      source.vidRange.endExclusive <= subPopulation.vidSubRange.endExclusive) {
      return
    }
  }
  throw SyntheticDataSpecValidationException(
    "Invalid vidRangeSpec",
    IllegalArgumentException("vidRange not within a subpopulation.")
  )
}

fun SyntheticEventGroupSpec.FrequencySpec.validate() {
  val source = this
  if (this.vidRangeSpecsList.size > 1) {
    val sortedVidRangeSpecs = source.vidRangeSpecsList.sortedBy { it.vidRange.start }
    var end = sortedVidRangeSpecs[0].vidRange.endExclusive
    for (i in 1 until sortedVidRangeSpecs.size) {
      if (sortedVidRangeSpecs[i].vidRange.start < end) {
        throw SyntheticDataSpecValidationException(
          "Invalid frequencySpec",
          IllegalArgumentException("vidRanges cannot overlap.")
        )
      }
      end = sortedVidRangeSpecs[i].vidRange.endExclusive
    }
  }
}

fun SyntheticEventGroupSpec.DateSpec.DateRange.validate() {
  val source = this
  if (source.start.toLocalDate() > source.endExclusive.toLocalDate()) {
    throw SyntheticDataSpecValidationException(
      "Invalid dateRange",
      IllegalArgumentException("dateRange start cannot be after end.")
    )
  }
}

fun VidRange.validate() {
  val source = this
  if (source.start < 1) {
    throw SyntheticDataSpecValidationException(
      "Invalid vidRange",
      IllegalArgumentException("vidRange start cannot be less than 1.")
    )
  }
  if (source.endExclusive < source.start) {
    throw SyntheticDataSpecValidationException(
      "Invalid vidRange",
      IllegalArgumentException("vidRange start cannot be greater than end.")
    )
  }
}
