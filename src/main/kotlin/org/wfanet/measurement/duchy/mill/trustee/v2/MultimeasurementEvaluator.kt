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

import org.wfanet.measurement.api.v2alpha.Measurement.Result.MultimeasurementResult
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.MultimeasurementResultKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.multimeasurementResult
import org.wfanet.measurement.api.v2alpha.TrusTeeV2Methodology
import org.wfanet.measurement.internal.duchy.Query

/**
 * Evaluates every [Query] of a TrusTEE v2 multimeasurement over its base vectors and returns the
 * released [MultimeasurementResult].
 *
 * The first pass evaluates every cell. Each cell's `DataProvider` slices then take the slice test
 * of [SliceSuppression]; the cells that lost some slices are rebuilt in a corrective pass. Every
 * cell that is not suppressed is noised and thresholded by [CellReleaser].
 *
 * @param queries the queries of the multimeasurement
 * @param dataProviders the `DataProvider` that fulfilled each base vector, indexed by vector index
 * @param subpopulations the subpopulation of each vector position and its attribute values
 * @param vidSamplingIntervalWidth the width of the VID sampling interval the vectors cover
 * @param privacyParams the noise mechanism and thresholds of the Mill
 * @param measurementName resource name of the `Measurement`, which seeds the noise
 */
class MultimeasurementEvaluator(
  private val queries: List<Query>,
  private val dataProviders: List<String>,
  private val subpopulations: SubpopulationIndex,
  vidSamplingIntervalWidth: Double,
  private val privacyParams: CellPrivacyParams,
  measurementName: String,
) {
  private val noise = CellNoise(privacyParams, measurementName)
  private val sliceSuppression =
    SliceSuppression(privacyParams.resultMinimumThresholds, noise, vidSamplingIntervalWidth)
  private val releaser =
    CellReleaser(privacyParams.resultMinimumThresholds, noise, vidSamplingIntervalWidth)

  /**
   * A replayable source of the base vectors, read once per evaluation pass, so that the vectors
   * need not be held whole.
   */
  fun interface VectorSource {
    /**
     * Returns the vectors in aligned chunks in position order, each holding one array per base
     * vector, indexed by vector index.
     */
    fun chunks(): Sequence<QueryEvaluator.VectorChunk>
  }

  /**
   * Evaluates the queries over [vectors], the base vectors indexed by vector index, each holding
   * one unsigned count per position.
   *
   * @throws IllegalArgumentException if [vectors] does not hold one vector per `DataProvider` entry
   *   or the vectors differ in length
   */
  fun evaluate(vectors: List<ByteArray>): MultimeasurementResult {
    require(vectors.size == dataProviders.size) {
      "Expected ${dataProviders.size} vectors, got ${vectors.size}"
    }
    val vectorSize: Int = vectors.firstOrNull()?.size ?: 0
    require(vectors.all { it.size == vectorSize }) { "Base vectors must have the same length" }
    return evaluate { inMemoryChunks(vectors, vectorSize) }
  }

  /**
   * Evaluates the queries over the base vectors of [vectors].
   *
   * @throws IllegalArgumentException if a chunk does not hold one aligned array per base vector
   */
  fun evaluate(vectors: VectorSource): MultimeasurementResult {
    // TODO(world-federation-of-advertisers/cross-media-measurement#4532): Before the first pass,
    //  remove the slices of a DataProvider whose reported impression count is below min_users, as
    //  docs/trustee/v2-mill-evaluation.md describes. The slice test reaches the same verdicts, so
    //  this only saves work.
    val firstPass =
      QueryEvaluator(queries, dataProviders, subpopulations, privacyParams.impressionCap)
    addAll(firstPass, vectors)
    val firstPassCells: List<QueryEvaluator.Cell> = firstPass.cells()

    val verdicts: Map<QueryEvaluator.CellKey, SliceSuppression.Verdict> =
      firstPassCells.associate { it.key to sliceSuppression.verdict(it) }
    val corrections: Map<QueryEvaluator.CellKey, Set<String>> = buildMap {
      for ((key, verdict) in verdicts) {
        if (verdict is SliceSuppression.Verdict.Correct) put(key, verdict.failedDataProviders)
      }
    }
    val correctedCells: Map<QueryEvaluator.CellKey, QueryEvaluator.Cell> =
      if (corrections.isEmpty()) {
        emptyMap()
      } else {
        val correctivePass =
          QueryEvaluator.corrective(
            queries,
            dataProviders,
            subpopulations,
            corrections,
            privacyParams.impressionCap,
          )
        addAll(correctivePass, vectors)
        correctivePass.cells().associateBy { it.key }
      }

    val cellsByQuery: Map<String, List<QueryEvaluator.Cell>> = firstPassCells.groupBy { it.queryId }
    return multimeasurementResult {
      for (query in queries) {
        queryResults +=
          MultimeasurementResultKt.queryResult {
            queryId = query.id
            groupingPaths += query.groupingPathsList
            maxFrequency = query.maxFrequency
            for (cell in cellsByQuery[query.id].orEmpty()) {
              cells +=
                when (verdicts.getValue(cell.key)) {
                  SliceSuppression.Verdict.Release -> releaser.release(cell)
                  SliceSuppression.Verdict.Suppress -> releaser.suppressed(cell)
                  is SliceSuppression.Verdict.Correct ->
                    releaser.release(correctedCells.getValue(cell.key))
                }
            }
            reachNoiseMechanism = noise.publicNoiseMechanism
            frequencyNoiseMechanism = noise.publicNoiseMechanism
            impressionNoiseMechanism = noise.publicNoiseMechanism
            trusTeeV2 = TrusTeeV2Methodology.getDefaultInstance()
          }
      }
    }
  }

  private fun addAll(evaluator: QueryEvaluator, vectors: VectorSource) {
    for (chunk in vectors.chunks()) {
      evaluator.add(chunk)
    }
  }

  /** Returns [vectors] in aligned chunks of at most [CHUNK_SIZE] positions. */
  private fun inMemoryChunks(
    vectors: List<ByteArray>,
    vectorSize: Int,
  ): Sequence<QueryEvaluator.VectorChunk> =
    (0 until vectorSize step CHUNK_SIZE).asSequence().map { start ->
      val length = minOf(CHUNK_SIZE, vectorSize - start)
      QueryEvaluator.VectorChunk(
        start,
        vectors.map { vector -> IntArray(length) { vector[start + it].toInt() and 0xFF } },
      )
    }

  companion object {
    /** Number of positions converted and evaluated at a time. */
    private const val CHUNK_SIZE = 64 * 1024
  }
}
