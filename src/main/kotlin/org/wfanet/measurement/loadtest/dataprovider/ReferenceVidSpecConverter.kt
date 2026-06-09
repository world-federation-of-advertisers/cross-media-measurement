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

import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange as popVidRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.DateSpecKt.dateRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.dateSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.frequencySpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.virtualpeople.common.demoInfo
import org.wfanet.virtualpeople.common.eventId
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.profileInfo
import org.wfanet.virtualpeople.common.userInfo
import org.wfanet.virtualpeople.core.labeler.Labeler

/**
 * Converts a [ReferenceVidEventGroupSpec] to equivalent [SyntheticEventGroupSpec] and
 * [PopulationSpec] by running reference VIDs through a [Labeler].
 *
 * Only practical for small-pool VID models. Throws [IllegalArgumentException] if the output would
 * exceed [DEFAULT_MAX_VID_RANGE_SPECS] VidRangeSpec entries.
 */
object ReferenceVidSpecConverter {

  data class ConvertedSpecs(
    val syntheticEventGroupSpec: SyntheticEventGroupSpec,
    val populationSpec: PopulationSpec,
  )

  /**
   * Converts a [ReferenceVidEventGroupSpec] to a [SyntheticEventGroupSpec] and [PopulationSpec]
   * pair by running reference VIDs through the [labeler].
   *
   * @throws IllegalArgumentException if the resulting spec exceeds [maxVidRangeSpecs]
   */
  fun convert(
    labeler: Labeler,
    spec: ReferenceVidEventGroupSpec,
    sourcePopulationSpec: PopulationSpec,
    maxVidRangeSpecs: Int = DEFAULT_MAX_VID_RANGE_SPECS,
  ): ConvertedSpecs {
    val records: List<ReferenceVidRecord> = ReferenceVidDataGeneration.generate(spec).toList()

    val labeledVids: List<LabeledReferenceVid> =
      records.map { record ->
        val input = labelerInput {
          eventId = eventId { id = record.referenceVid.toString() }
          timestampUsec = 0L
          profileInfo = profileInfo {
            proprietaryIdSpace1UserInfo = userInfo {
              userId = record.referenceVid.toString()
              demo = demoInfo {
                demoBucket = demoBucket {
                  gender =
                    Gender.forNumber(record.gender) ?: error("Invalid gender: ${record.gender}")
                  age = ageRange {
                    minAge = record.minAge
                    maxAge = record.maxAge
                  }
                }
              }
            }
          }
        }

        val output = labeler.label(input)
        check(output.peopleCount > 0) {
          "Labeler returned no people for reference VID ${record.referenceVid}"
        }
        val person = output.getPeople(0)
        check(person.virtualPersonId > 0) {
          "Labeler returned VID 0 for reference VID ${record.referenceVid}"
        }

        val vid = person.virtualPersonId.toLong()
        val subPopIndex: Int =
          sourcePopulationSpec.subpopulationsList.indexOfFirst { sub ->
            sub.vidRangesList.any { range -> vid >= range.startVid && vid <= range.endVidInclusive }
          }
        require(subPopIndex >= 0) {
          "VID $vid (from reference VID ${record.referenceVid}) not in any PopulationSpec range"
        }

        LabeledReferenceVid(
          vid = vid,
          subPopulationIndex = subPopIndex,
          frequency = record.frequency,
        )
      }

    logger.info(
      "Labeled ${labeledVids.size} reference VIDs, " +
        "${labeledVids.map { it.vid }.distinct().size} unique VIDs"
    )

    return ConvertedSpecs(
      syntheticEventGroupSpec = convertSyntheticSpec(labeledVids, spec, maxVidRangeSpecs),
      populationSpec = convertPopulationSpec(labeledVids, sourcePopulationSpec),
    )
  }

  private data class LabeledReferenceVid(
    val vid: Long,
    val subPopulationIndex: Int,
    val frequency: Long,
  )

  private fun convertSyntheticSpec(
    labeledVids: List<LabeledReferenceVid>,
    spec: ReferenceVidEventGroupSpec,
    maxVidRangeSpecs: Int,
  ): SyntheticEventGroupSpec {
    val vidFrequencies: Map<Long, Long> =
      labeledVids.groupBy { it.vid }.mapValues { (_, vids) -> vids.sumOf { it.frequency } }

    val vidsByEffFreq: Map<Long, List<Long>> =
      vidFrequencies.entries.groupBy({ it.value }, { it.key }).mapValues { (_, vids) ->
        vids.sorted()
      }

    val totalVidRangeSpecs: Int = vidsByEffFreq.values.sumOf { mergeAdjacentVids(it).size }
    require(totalVidRangeSpecs <= maxVidRangeSpecs) {
      "Converted spec would have $totalVidRangeSpecs VidRangeSpecs, exceeding threshold of " +
        "$maxVidRangeSpecs. Use direct generation instead of converting."
    }

    val result = syntheticEventGroupSpec {
      for (refDateSpec in spec.dateSpecsList) {

        dateSpecs += dateSpec {
          this.dateRange = dateRange {
            start = refDateSpec.dateRange.start
            endExclusive = refDateSpec.dateRange.endExclusive
          }
          for ((freq, vids) in vidsByEffFreq.entries.sortedBy { it.key }) {
            frequencySpecs += frequencySpec {
              frequency = freq
              for (range in mergeAdjacentVids(vids)) {
                vidRangeSpecs += vidRangeSpec {
                  vidRange = vidRange {
                    start = range.first
                    endExclusive = range.last + 1
                  }
                  nonPopulationFieldValues.putAll(spec.nonPopulationFieldValuesMap)
                }
              }
            }
          }
        }
      }
    }

    logger.info("Converted to SyntheticEventGroupSpec with $totalVidRangeSpecs VidRangeSpecs")
    return result
  }

  private fun convertPopulationSpec(
    labeledVids: List<LabeledReferenceVid>,
    sourcePopulationSpec: PopulationSpec,
  ): PopulationSpec {
    val vidsBySubPop: Map<Int, List<Long>> =
      labeledVids
        .groupBy { it.subPopulationIndex }
        .mapValues { (_, vids) -> vids.map { it.vid }.distinct().sorted() }

    return populationSpec {
      for ((subPopIndex, vids) in vidsBySubPop.toSortedMap()) {
        val sourceSubPop = sourcePopulationSpec.getSubpopulations(subPopIndex)
        subpopulations += subPopulation {
          for (range in mergeAdjacentVids(vids)) {
            vidRanges += popVidRange {
              startVid = range.first
              endVidInclusive = range.last
            }
          }
          attributes += sourceSubPop.attributesList
        }
      }
    }
  }

  private fun mergeAdjacentVids(sortedVids: List<Long>): List<LongRange> {
    if (sortedVids.isEmpty()) return emptyList()

    val ranges = mutableListOf<LongRange>()
    var rangeStart = sortedVids[0]
    var rangeEnd = sortedVids[0]

    for (i in 1 until sortedVids.size) {
      if (sortedVids[i] == rangeEnd + 1) {
        rangeEnd = sortedVids[i]
      } else {
        ranges.add(rangeStart..rangeEnd)
        rangeStart = sortedVids[i]
        rangeEnd = sortedVids[i]
      }
    }
    ranges.add(rangeStart..rangeEnd)
    return ranges
  }

  const val DEFAULT_MAX_VID_RANGE_SPECS = 500

  private val logger: Logger = Logger.getLogger(this::class.java.name)
}
