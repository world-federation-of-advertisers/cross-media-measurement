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
    val distinctVids = labeledResults.map { it.vid }.distinct().sorted()

    val dateSpecs =
      spec.dateSpecsList.map { refDateSpec ->
        val frequency = refDateSpec.frequency

        val vidRangeSpecs =
          mergeAdjacentVids(distinctVids).map { range ->
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

        val totalVidRangeSpecs = vidRangeSpecs.size
        if (totalVidRangeSpecs > maxVidRangeSpecs) {
          logger.warning(
            "Exported spec has $totalVidRangeSpecs VidRangeSpecs (threshold: $maxVidRangeSpecs). " +
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
            addFrequencySpecs(
              FrequencySpec.newBuilder()
                .setFrequency(frequency)
                .addAllVidRangeSpecs(vidRangeSpecs)
                .build()
            )
          }
          .build()
      }

    return SyntheticEventGroupSpec.newBuilder().addAllDateSpecs(dateSpecs).build()
  }

  /**
   * Exports a [PopulationSpec] from labeler results by grouping VIDs by subpopulation index and
   * merging adjacent VIDs into ranges.
   *
   * @param labeledResults output from [ReferenceVidDataGeneration]
   * @param sourcePopulationSpec the original PopulationSpec (for attributes)
   */
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

  /** Writes a spec as textproto to the given file. */
  fun writeTextProto(file: File, message: com.google.protobuf.Message) {
    file.writeText(TextFormat.printer().printToString(message))
    logger.info("Wrote ${file.path}")
  }

  /**
   * Merges a sorted list of VIDs into contiguous ranges. E.g. [1,2,3,5,6,8] -> [[1,3], [5,6],
   * [8,8]]
   */
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
