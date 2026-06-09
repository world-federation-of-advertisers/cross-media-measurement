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

import com.google.protobuf.Message
import com.google.protobuf.TextFormat
import java.io.File
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
    val labeledReferenceVids: List<ReferenceVidDataGeneration.LabeledVid> =
      ReferenceVidDataGeneration.generateEvents(labeler, sourcePopulationSpec, spec)
        .flatMap { it.labeledVids.toList() }
        .toList()

    return ConvertedSpecs(
      syntheticEventGroupSpec = convertSyntheticSpec(labeledReferenceVids, spec, maxVidRangeSpecs),
      populationSpec = convertPopulationSpec(labeledReferenceVids, sourcePopulationSpec),
    )
  }

  private fun convertSyntheticSpec(
    labeledReferenceVids: List<ReferenceVidDataGeneration.LabeledVid>,
    spec: ReferenceVidEventGroupSpec,
    maxVidRangeSpecs: Int,
  ): SyntheticEventGroupSpec {
    val vidCounts: Map<Long, Int> = labeledReferenceVids.groupingBy { it.vid }.eachCount()

    val result = syntheticEventGroupSpec {
      for (refDateSpec in spec.dateSpecsList) {
        val specFrequency = refDateSpec.frequency

        val vidsByEffectiveFrequency: Map<Long, List<Long>> =
          vidCounts.entries.groupBy({ it.value * specFrequency }, { it.key }).mapValues { (_, vids)
            ->
            vids.sorted()
          }

        dateSpecs += dateSpec {
          this.dateRange = dateRange {
            start = refDateSpec.dateRange.start
            endExclusive = refDateSpec.dateRange.endExclusive
          }
          for ((freq, vids) in vidsByEffectiveFrequency.entries.sortedBy { it.key }) {
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

    val totalVidRangeSpecs: Int =
      result.dateSpecsList.sumOf { ds -> ds.frequencySpecsList.sumOf { it.vidRangeSpecsCount } }
    require(totalVidRangeSpecs <= maxVidRangeSpecs) {
      "Converted spec has $totalVidRangeSpecs VidRangeSpecs, exceeding threshold of " +
        "$maxVidRangeSpecs. Use direct generation instead of converting."
    }

    logger.info("Converted to SyntheticEventGroupSpec with $totalVidRangeSpecs VidRangeSpecs")
    return result
  }

  private fun convertPopulationSpec(
    labeledReferenceVids: List<ReferenceVidDataGeneration.LabeledVid>,
    sourcePopulationSpec: PopulationSpec,
  ): PopulationSpec {
    val vidsBySubPop: Map<Int, List<Long>> =
      labeledReferenceVids
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

  fun writeTextProto(file: File, message: Message) {
    file.writeText(TextFormat.printer().printToString(message))
    logger.info("Wrote ${file.path}")
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
