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

package org.wfanet.measurement.duchy.mill.trustee.v2

import it.unimi.dsi.fastutil.Arrays as FastUtilArrays
import it.unimi.dsi.fastutil.Swapper
import it.unimi.dsi.fastutil.ints.IntComparator
import org.wfanet.measurement.common.VidHashing
import org.wfanet.measurement.computation.SampledIndexRanges
import org.wfanet.measurement.internal.duchy.PopulationSpec

/**
 * A [SubpopulationIndex] over frequency vectors indexed by VID hash rank.
 *
 * A VID's global index is the rank of [VidHashing.hash] among every VID in the [PopulationSpec],
 * and position `p` of a vector over the sampling interval starting at global index `s` holds global
 * index `(s + p) mod populationSize`.
 *
 * @property vectorSize the length of every frequency vector over the sampling interval
 */
class HashRankSubpopulationIndex
private constructor(
  private val order: HashRankOrder,
  private val globalStartIndex: Int,
  val vectorSize: Int,
  private val attributes: SubpopulationAttributes,
) : SubpopulationIndex {
  override val subpopulationCount: Int
    get() = attributes.subpopulationCount

  /**
   * Returns the subpopulation of the VID at frequency vector [position].
   *
   * @throws IllegalArgumentException if [position] is not in `0 until vectorSize`
   */
  override fun subpopulationOf(position: Int): Int {
    require(position in 0 until vectorSize) { "Position $position is outside the vector" }
    val globalIndex =
      ((globalStartIndex.toLong() + position) % order.subpopulationByGlobalIndex.size).toInt()
    return order.subpopulationByGlobalIndex[globalIndex]
  }

  /**
   * Returns the enum symbol of the attribute at [path] for [subpopulation], or `null` if that
   * subpopulation does not set it.
   *
   * @param path `<event template name>.<field name>`, e.g. `person.age_group`
   */
  override fun attributeValue(subpopulation: Int, path: String): String? =
    attributes.attributeValue(subpopulation, path)

  override fun populationSize(subpopulation: Int): Long = order.subpopulationSizes[subpopulation]

  companion object {
    /**
     * Builds the index for [populationSpec] over the VID sampling interval of [samplingStart] and
     * [samplingWidth].
     *
     * @param attributes the attributes of the subpopulations of [populationSpec]
     * @throws IllegalArgumentException if [populationSpec] has overlapping, empty or out-of-range
     *   VID ranges, or does not match [attributes], or the sampling interval is invalid
     */
    fun build(
      populationSpec: PopulationSpec,
      attributes: SubpopulationAttributes,
      samplingStart: Float,
      samplingWidth: Float,
    ): HashRankSubpopulationIndex =
      build(HashRankOrder.build(populationSpec), attributes, samplingStart, samplingWidth)

    /**
     * Builds the index for the PopulationSpec of [order] over the VID sampling interval of
     * [samplingStart] and [samplingWidth].
     *
     * @param attributes the attributes of the subpopulations of the PopulationSpec of [order]
     * @throws IllegalArgumentException if [order] does not match [attributes], or the sampling
     *   interval is invalid
     */
    fun build(
      order: HashRankOrder,
      attributes: SubpopulationAttributes,
      samplingStart: Float,
      samplingWidth: Float,
    ): HashRankSubpopulationIndex {
      require(order.subpopulationSizes.size == attributes.subpopulationCount) {
        "PopulationSpec has ${order.subpopulationSizes.size} subpopulations, attributes " +
          "have ${attributes.subpopulationCount}"
      }
      // The same ranges the DataProviders build their vectors over.
      val ranges =
        SampledIndexRanges.of(order.subpopulationByGlobalIndex.size, samplingStart, samplingWidth)
      return HashRankSubpopulationIndex(order, ranges.primaryRange.first, ranges.size, attributes)
    }
  }
}

/**
 * The subpopulation of every VID of a PopulationSpec in VID hash rank order, which depends only on
 * the PopulationSpec, so that it can be built once and shared across computations.
 *
 * @property subpopulationByGlobalIndex the subpopulation of the VID at each global index
 * @property subpopulationSizes the number of VIDs in each subpopulation
 */
class HashRankOrder
private constructor(
  internal val subpopulationByGlobalIndex: IntArray,
  internal val subpopulationSizes: LongArray,
) {
  /** A VID range and the subpopulation it belongs to. */
  private data class SubpopulationRange(val first: Long, val last: Long, val subpopulation: Int)

  companion object {
    /**
     * Builds the order of [populationSpec].
     *
     * @throws IllegalArgumentException if [populationSpec] has overlapping, empty or out-of-range
     *   VID ranges
     */
    fun build(populationSpec: PopulationSpec): HashRankOrder {
      val ranges: List<SubpopulationRange> = sortedRanges(populationSpec)
      val populationSizeLong: Long = ranges.sumOf { it.last - it.first + 1 }
      require(populationSizeLong in 1 until Int.MAX_VALUE) {
        "Population size $populationSizeLong is not in 1 until ${Int.MAX_VALUE}"
      }
      val populationSize = populationSizeLong.toInt()

      val hashes = LongArray(populationSize)
      // Holds each VID, then once sorted, each VID's subpopulation.
      val vids = IntArray(populationSize)
      var next = 0
      for (range in ranges) {
        for (vid in range.first..range.last) {
          vids[next] = vid.toInt()
          hashes[next] = VidHashing.hash(vid)
          next++
        }
      }
      FastUtilArrays.parallelQuickSort(
        0,
        populationSize,
        IntComparator { a, b ->
          val byHash = hashes[a].compareTo(hashes[b])
          if (byHash != 0) byHash else vids[a].compareTo(vids[b])
        },
        Swapper { a, b ->
          hashes[a] = hashes[b].also { hashes[b] = hashes[a] }
          vids[a] = vids[b].also { vids[b] = vids[a] }
        },
      )
      val firsts = LongArray(ranges.size) { ranges[it].first }
      for (index in vids.indices) {
        // The range starting at or before the VID, which holds it since ranges cover every VID.
        val found = firsts.binarySearch(vids[index].toLong())
        val rangeIndex = if (found >= 0) found else -found - 2
        vids[index] = ranges[rangeIndex].subpopulation
      }

      val subpopulationSizes = LongArray(populationSpec.subpopulationsCount)
      for (range in ranges) {
        subpopulationSizes[range.subpopulation] += range.last - range.first + 1
      }
      return HashRankOrder(vids, subpopulationSizes)
    }

    /**
     * Returns the VID ranges of [populationSpec] sorted by first VID.
     *
     * @throws IllegalArgumentException if any range is empty, negative, beyond [Int.MAX_VALUE] or
     *   overlaps another
     */
    private fun sortedRanges(populationSpec: PopulationSpec): List<SubpopulationRange> {
      val ranges =
        populationSpec.subpopulationsList
          .flatMapIndexed { subpopulation, it ->
            it.vidRangesList.map { range ->
              SubpopulationRange(range.startVid, range.endVidInclusive, subpopulation)
            }
          }
          .sortedBy { it.first }
      for ((index, range) in ranges.withIndex()) {
        require(range.first in 0..range.last && range.last < Int.MAX_VALUE) {
          "Invalid VID range ${range.first}..${range.last}"
        }
        require(index == 0 || range.first > ranges[index - 1].last) {
          "VID range ${range.first}..${range.last} overlaps another"
        }
      }
      return ranges
    }
  }
}
