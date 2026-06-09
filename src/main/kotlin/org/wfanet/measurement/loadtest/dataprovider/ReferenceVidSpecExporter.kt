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

import com.google.protobuf.TextFormat
import java.io.File
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.DateSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.FrequencySpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.VidRange

object ReferenceVidSpecExporter {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  data class LabeledVidEvent(val vid: Long, val subPopulationIndex: Int)

  /**
   * Exports labeler results as a [SyntheticEventGroupSpec] that produces equivalent events when
   * used with [SyntheticDataGeneration].
   *
   * Groups VIDs by (subpopulation, frequency), merges adjacent VIDs into ranges, and emits
   * [VidRangeSpec] entries.
   *
   * @param labeledResults output from [ReferenceVidDataGeneration]
   * @param spec the original reference spec (for date ranges, frequency, non-population fields)
   * @param maxVidRangeSpecs warn if the output exceeds this many VidRangeSpec entries
   * @return the equivalent [SyntheticEventGroupSpec]
   */
  fun exportSyntheticSpec(
    labeledResults: List<ReferenceVidDataGeneration.LabeledVidResult>,
    spec: ReferenceVidEventGroupSpec,
    maxVidRangeSpecs: Int = 500,
  ): SyntheticEventGroupSpec {
    val vidCounts = labeledResults.groupingBy { it.vid }.eachCount()

    val dateSpecs =
      spec.dateSpecsList.map { refDateSpec ->
        val specFrequency = refDateSpec.frequency

        val vidsByEffectiveFrequency =
          vidCounts.entries.groupBy({ it.value * specFrequency }, { it.key }).mapValues { (_, vids)
            ->
            vids.sorted()
          }

        val frequencySpecs =
          vidsByEffectiveFrequency.entries
            .sortedBy { it.key }
            .map { (freq, vids) ->
              val vidRangeSpecs =
                mergeAdjacentVids(vids).map { range ->
                  VidRangeSpec.newBuilder()
                    .apply {
                      vidRange =
                        VidRange.newBuilder()
                          .setStart(range.first)
                          .setEndExclusive(range.last + 1)
                          .build()
                      putAllNonPopulationFieldValues(spec.nonPopulationFieldValuesMap)
                    }
                    .build()
                }
              FrequencySpec.newBuilder()
                .setFrequency(freq.toLong())
                .addAllVidRangeSpecs(vidRangeSpecs)
                .build()
            }

        val totalVidRangeSpecs = frequencySpecs.sumOf { it.vidRangeSpecsCount }
        if (totalVidRangeSpecs > maxVidRangeSpecs) {
          logger.warning(
            "Exported spec has $totalVidRangeSpecs VidRangeSpecs " +
              "(threshold: $maxVidRangeSpecs). " +
              "Consider using direct generation (Option A) instead."
          )
        }

        DateSpec.newBuilder()
          .apply {
            dateRange =
              DateSpec.DateRange.newBuilder()
                .setStart(refDateSpec.dateRange.start)
                .setEndExclusive(refDateSpec.dateRange.endExclusive)
                .build()
            addAllFrequencySpecs(frequencySpecs)
          }
          .build()
      }

    return SyntheticEventGroupSpec.newBuilder().addAllDateSpecs(dateSpecs).build()
  }

  fun exportPopulationSpec(
    labeledResults: List<ReferenceVidDataGeneration.LabeledVidResult>,
    sourcePopulationSpec: PopulationSpec,
  ): PopulationSpec {
    val vidsBySubPop =
      labeledResults
        .groupBy { it.subPopulationIndex }
        .mapValues { (_, results) -> results.map { it.vid }.distinct().sorted() }

    return PopulationSpec.newBuilder()
      .apply {
        for ((subPopIndex, vids) in vidsBySubPop.toSortedMap()) {
          val sourceSubPop = sourcePopulationSpec.getSubpopulations(subPopIndex)
          addSubpopulations(
            PopulationSpec.SubPopulation.newBuilder()
              .apply {
                for (range in mergeAdjacentVids(vids)) {
                  addVidRanges(
                    PopulationSpec.VidRange.newBuilder()
                      .setStartVid(range.first)
                      .setEndVidInclusive(range.last)
                      .build()
                  )
                }
                addAllAttributes(sourceSubPop.attributesList)
              }
              .build()
          )
        }
      }
      .build()
  }

  fun writeTextProto(file: File, message: com.google.protobuf.Message) {
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
}
