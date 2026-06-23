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

import com.google.type.Date
import java.time.LocalDate
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange as popVidRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.LabelerInputEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.DateSpecKt.dateRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.dateSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.frequencySpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.demoInfo
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.profileInfo
import org.wfanet.virtualpeople.common.userInfo
import org.wfanet.virtualpeople.core.labeler.Labeler

/**
 * Converts a [LabelerInputEventGroupSpec] to equivalent [SyntheticEventGroupSpec] and
 * [PopulationSpec] by running labeler input IDs through a [Labeler].
 *
 * Only practical for small-pool VID models. Throws [IllegalArgumentException] if the output would
 * exceed [DEFAULT_MAX_VID_RANGE_SPECS] VidRangeSpec entries.
 */
object LabelerInputSpecConverter {

  data class ConvertedSpecs(
    val syntheticEventGroupSpec: SyntheticEventGroupSpec,
    val populationSpec: PopulationSpec,
  )

  /**
   * Convenience overload that delegates labeling to [labeler].
   *
   * Equivalent to calling the primary [convert] with `labeler::label`.
   */
  fun convert(
    labeler: Labeler,
    spec: LabelerInputEventGroupSpec,
    sourcePopulationSpec: PopulationSpec,
    maxVidRangeSpecs: Int = DEFAULT_MAX_VID_RANGE_SPECS,
  ): ConvertedSpecs = convert(labeler::label, spec, sourcePopulationSpec, maxVidRangeSpecs)

  /**
   * Converts a [LabelerInputEventGroupSpec] to a [SyntheticEventGroupSpec] and [PopulationSpec]
   * pair by running each labeler input ID through [label].
   *
   * @throws IllegalArgumentException if the resulting spec exceeds [maxVidRangeSpecs]
   */
  fun convert(
    label: (LabelerInput) -> LabelerOutput,
    spec: LabelerInputEventGroupSpec,
    sourcePopulationSpec: PopulationSpec,
    maxVidRangeSpecs: Int = DEFAULT_MAX_VID_RANGE_SPECS,
  ): ConvertedSpecs {
    validateSpec(spec)

    val allLabeledVids = mutableListOf<LabelerOutputEntry>()
    val perDateSpecVids = mutableListOf<List<LabelerOutputEntry>>()

    for (dateSpec in spec.dateSpecsList) {
      val dateSpecVids = mutableListOf<LabelerOutputEntry>()
      for (record in LabelerInputDataGeneration.generateForDateSpec(dateSpec)) {
        val input = labelerInput {
          // Hardcoded to 0: this converter only works correctly for time-independent labeler
          // models (output VID is a deterministic function of profile + user_id alone). A
          // time-dependent model would map every labeler input ID to its t=0 routing, which then
          // mismatches every real impression's at-fulfillment-time routing — the resulting
          // SyntheticEventGroupSpec would be wrong by construction. Do not use this converter
          // with a time-dependent model.
          timestampUsec = 0L
          profileInfo = profileInfo {
            proprietaryIdSpace1UserInfo = userInfo {
              userId = record.labelerInputId.toString()
              demo = demoInfo { demoBucket = record.demoBucket }
            }
          }
        }

        val output = label(input)
        check(output.peopleCount > 0) {
          "Labeler returned no people for labeler input ID ${record.labelerInputId}"
        }

        // Each labeled person becomes its own output entry at the input's full frequency. People
        // without a virtual_person_id are impression-counting-only (per LabelerOutput's doc) and
        // are skipped — they don't contribute to reach. Non-population field values are
        // assumed to be a property of the input event and are duplicated across all output
        // people for this labeler input ID.
        var emittedForRecord = 0
        for (person in output.peopleList) {
          if (person.virtualPersonId == 0L) continue
          val vid = person.virtualPersonId.toLong()
          val subPopIndex: Int =
            sourcePopulationSpec.subpopulationsList.indexOfFirst { sub ->
              sub.vidRangesList.any { range ->
                vid >= range.startVid && vid <= range.endVidInclusive
              }
            }
          require(subPopIndex >= 0) {
            "VID $vid (from labeler input ID ${record.labelerInputId}) not in any PopulationSpec range"
          }
          val labeled =
            LabelerOutputEntry(
              vid = vid,
              subPopulationIndex = subPopIndex,
              frequency = record.frequency,
              nonPopulationFieldValues = record.nonPopulationFieldValues,
            )
          dateSpecVids.add(labeled)
          allLabeledVids.add(labeled)
          emittedForRecord++
        }
        check(emittedForRecord > 0) {
          "Labeler returned no people with virtual_person_id set for labeler input ID " +
            "${record.labelerInputId}"
        }
      }
      perDateSpecVids.add(dateSpecVids)
    }

    logger.info(
      "Labeled ${allLabeledVids.size} labeler input IDs, " +
        "${allLabeledVids.map { it.vid }.distinct().size} unique VIDs"
    )

    return ConvertedSpecs(
      syntheticEventGroupSpec = convertSyntheticSpec(perDateSpecVids, spec, maxVidRangeSpecs),
      populationSpec = convertPopulationSpec(allLabeledVids, sourcePopulationSpec),
    )
  }

  private data class LabelerOutputEntry(
    val vid: Long,
    val subPopulationIndex: Int,
    val frequency: Long,
    val nonPopulationFieldValues: Map<String, FieldValue>,
  )

  /** Identity used to bucket VIDs into a single VidRangeSpec (same frequency + same fields). */
  private data class VidGroupKey(val frequency: Long, val fields: Map<String, FieldValue>)

  private fun convertSyntheticSpec(
    perDateSpecVids: List<List<LabelerOutputEntry>>,
    spec: LabelerInputEventGroupSpec,
    maxVidRangeSpecs: Int,
  ): SyntheticEventGroupSpec {
    var totalVidRangeSpecs = 0

    val result = syntheticEventGroupSpec {
      for ((refDateSpec, dateSpecVids) in spec.dateSpecsList.zip(perDateSpecVids)) {
        // Collapse multiple entries per VID into a single (summed_frequency, fields) key.
        // The same VID arriving with different non_population_field_values can't be expressed
        // in one VidRangeSpec; reject that up front so callers see a clear error rather than
        // losing data in the adjacent-VID merge below.
        val vidToKey: Map<Long, VidGroupKey> =
          dateSpecVids
            .groupBy { it.vid }
            .mapValues { (vid, entries) ->
              val distinctFields = entries.map { it.nonPopulationFieldValues }.distinct()
              require(distinctFields.size == 1) {
                "Labeler VID $vid has ${distinctFields.size} distinct " +
                  "non_population_field_values maps from $entries; SyntheticEventGroupSpec " +
                  "does not allow more than one."
              }
              VidGroupKey(
                frequency = entries.sumOf { it.frequency },
                fields = distinctFields.first(),
              )
            }

        // Group VIDs by (frequency, fields) so adjacent-VID merging stays within a homogeneous
        // group — adjacent VIDs with different fields end up in different groups, each
        // producing their own VidRangeSpec with the correct field values.
        val vidsByKey: Map<VidGroupKey, List<Long>> =
          vidToKey.entries.groupBy({ it.value }, { it.key }).mapValues { (_, vids) ->
            vids.sorted()
          }

        val dateSpecRangeCount = vidsByKey.values.sumOf { mergeAdjacentVids(it).size }
        totalVidRangeSpecs += dateSpecRangeCount

        dateSpecs += dateSpec {
          this.dateRange = dateRange {
            start = refDateSpec.dateRange.start
            endExclusive = refDateSpec.dateRange.endExclusive
          }
          val keysByFrequency: Map<Long, List<VidGroupKey>> =
            vidsByKey.keys.groupBy { it.frequency }
          for ((freq, keysAtFreq) in keysByFrequency.toSortedMap()) {
            frequencySpecs += frequencySpec {
              frequency = freq
              // Deterministic ordering within a FrequencySpec: by lowest VID per group.
              for (key in keysAtFreq.sortedBy { vidsByKey.getValue(it).first() }) {
                for (range in mergeAdjacentVids(vidsByKey.getValue(key))) {
                  vidRangeSpecs += vidRangeSpec {
                    vidRange = vidRange {
                      start = range.first
                      endExclusive = range.last + 1
                    }
                    nonPopulationFieldValues.putAll(key.fields)
                  }
                }
              }
            }
          }
        }
      }
    }

    require(totalVidRangeSpecs <= maxVidRangeSpecs) {
      "Converted spec would have $totalVidRangeSpecs VidRangeSpecs, exceeding threshold of " +
        "$maxVidRangeSpecs. Use direct generation instead of converting."
    }

    logger.info("Converted to SyntheticEventGroupSpec with $totalVidRangeSpecs VidRangeSpecs")
    return result
  }

  private fun convertPopulationSpec(
    labeledVids: List<LabelerOutputEntry>,
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

  /**
   * Throws if [spec] is malformed: empty/inverted ranges, non-positive frequency, overlapping
   * idRanges within a DateSpec, or more labeler input IDs in total than
   * [MAX_INPUT_LABELER_INPUT_IDS].
   */
  private fun validateSpec(spec: LabelerInputEventGroupSpec) {
    var totalInputSize: Long = 0L
    for ((dateSpecIndex, dateSpec) in spec.dateSpecsList.withIndex()) {
      val dateRange = dateSpec.dateRange
      require(dateRange.endExclusive.toLocalDate() > dateRange.start.toLocalDate()) {
        "DateSpec[$dateSpecIndex] date_range.end_exclusive must be after start"
      }

      val ranges = mutableListOf<LongRange>()
      for ((distIndex, dist) in dateSpec.demographicDistributionsList.withIndex()) {
        val idRange = dist.idRange
        require(idRange.endExclusive > idRange.start) {
          "DateSpec[$dateSpecIndex].demographicDistributions[$distIndex] " +
            "id_range.end_exclusive (${idRange.endExclusive}) must be greater than start " +
            "(${idRange.start})"
        }
        require(dist.frequency > 0) {
          "DateSpec[$dateSpecIndex].demographicDistributions[$distIndex] " +
            "frequency must be positive (got ${dist.frequency})"
        }
        ranges.add(idRange.start until idRange.endExclusive)
        totalInputSize += idRange.endExclusive - idRange.start
      }

      // O(n^2) check; fine for the small specs this converter is intended to handle.
      for (i in ranges.indices) {
        for (j in i + 1 until ranges.size) {
          require(!rangesOverlap(ranges[i], ranges[j])) {
            "DateSpec[$dateSpecIndex] demographicDistributions[$i] id_range " +
              "(${ranges[i].first} until ${ranges[i].last + 1}) overlaps with " +
              "[$j] (${ranges[j].first} until ${ranges[j].last + 1})"
          }
        }
      }
    }
    require(totalInputSize <= MAX_INPUT_LABELER_INPUT_IDS) {
      "Input spec has $totalInputSize labeler input IDs, exceeding maximum of " +
        "$MAX_INPUT_LABELER_INPUT_IDS."
    }
  }

  private fun rangesOverlap(a: LongRange, b: LongRange): Boolean =
    a.first <= b.last && b.first <= a.last

  private fun Date.toLocalDate(): LocalDate = java.time.LocalDate.of(year, month, day)

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

  /**
   * Default fail-fast cap on the size of the converted [SyntheticEventGroupSpec], counted as the
   * total number of [VidRangeSpec] entries across all DateSpecs / FrequencySpecs. Protects callers
   * from accidentally producing specs too large to be useful or to ingest. Callers can override via
   * the `maxVidRangeSpecs` parameter to [convert].
   */
  const val DEFAULT_MAX_VID_RANGE_SPECS = 500

  /**
   * Hard cap on the total number of labeler input IDs across all DateSpecs /
   * DemographicDistributions in a single input spec. Caps the number of [Labeler.label] calls made
   * by one [convert] invocation. Not overridable — if you need more, run the converter on smaller
   * specs.
   */
  const val MAX_INPUT_LABELER_INPUT_IDS = 10_000L

  private val logger: Logger = Logger.getLogger(this::class.java.name)
}
