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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.duchy.Query
import org.wfanet.measurement.internal.duchy.VidFilter
import org.wfanet.measurement.internal.duchy.VidFilterKt
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.populationAttributeValue
import org.wfanet.measurement.internal.duchy.query
import org.wfanet.measurement.internal.duchy.vectorSet
import org.wfanet.measurement.internal.duchy.vidFilter

@RunWith(JUnit4::class)
class QueryEvaluatorTest {
  @Test
  fun `cells sums summands and folds frequencies above max_frequency`() {
    val cells = evaluate(testQuery(summands = listOf(0, 1, 2), maxFrequency = 3))

    // Summed counts by position: 1, 1, 4, 2, 3, 0.
    assertThat(cells)
      .containsExactly(
        CellView(
          queryId = QUERY_ID,
          attributeValues = listOf(),
          frequencyHistogram = listOf(2L, 1L, 2L),
          impressions = 11L,
          sliceReach = mapOf(DATA_PROVIDER_A to 4L, DATA_PROVIDER_B to 2L),
        )
      )
  }

  @Test
  fun `cells counts a VID once per DataProvider in slice reach`() {
    // Position 2 is reached by vectors 0 and 1, both from DATA_PROVIDER_A.
    val cells = evaluate(testQuery(summands = listOf(0, 1), maxFrequency = 1))

    assertThat(cells.single().sliceReach).containsExactly(DATA_PROVIDER_A, 4L)
  }

  @Test
  fun `cells keeps only VIDs in the reach clause region`() {
    val filter = vidFilter {
      reachClauses +=
        VidFilterKt.reachClause {
          reachedSets += vectorSet { baseVectors += 0 }
          unreachedSet = vectorSet { baseVectors += 2 }
        }
    }

    val cells = evaluate(testQuery(summands = listOf(0, 1, 2), maxFrequency = 5, filter = filter))

    // Positions 0 and 4.
    assertThat(cells.single().frequencyHistogram).containsExactly(1L, 0L, 1L, 0L, 0L).inOrder()
    assertThat(cells.single().impressions).isEqualTo(4L)
  }

  @Test
  fun `cells keeps VIDs satisfying any reach clause`() {
    val filter = vidFilter {
      reachClauses +=
        VidFilterKt.reachClause {
          reachedSets += vectorSet { baseVectors += 0 }
          unreachedSet = vectorSet { baseVectors += 2 }
        }
      reachClauses += VidFilterKt.reachClause { reachedSets += vectorSet { baseVectors += 2 } }
    }

    val cells = evaluate(testQuery(summands = listOf(0, 1, 2), maxFrequency = 5, filter = filter))

    // Positions 0, 2, 3 and 4.
    assertThat(cells.single().frequencyHistogram).containsExactly(1L, 1L, 1L, 1L, 0L).inOrder()
    assertThat(cells.single().impressions).isEqualTo(10L)
  }

  @Test
  fun `cells requires a VID to be reached in every reached set of a clause`() {
    val filter = vidFilter {
      reachClauses +=
        VidFilterKt.reachClause {
          reachedSets += vectorSet { baseVectors += 0 }
          reachedSets += vectorSet { baseVectors += 1 }
        }
    }

    val cells = evaluate(testQuery(summands = listOf(0), maxFrequency = 3, filter = filter))

    // Position 2 only.
    assertThat(cells.single().frequencyHistogram).containsExactly(0L, 1L, 0L).inOrder()
  }

  @Test
  fun `cells keeps only VIDs in subpopulations matching the attribute filter`() {
    val filter = vidFilter {
      attributes.put(AGE_GROUP_PATH, populationAttributeValue { enumValue = YOUNG })
    }

    val cells = evaluate(testQuery(summands = listOf(0, 1, 2), maxFrequency = 2, filter = filter))

    // Even positions: 1, 4, 3.
    assertThat(cells.single().frequencyHistogram).containsExactly(1L, 2L).inOrder()
    assertThat(cells.single().impressions).isEqualTo(8L)
  }

  @Test
  fun `cells returns one cell per grouping combination`() {
    val cells =
      evaluate(
        testQuery(
          summands = listOf(0, 1, 2),
          maxFrequency = 2,
          groupingPaths = listOf(AGE_GROUP_PATH),
        )
      )

    assertThat(cells)
      .containsExactly(
        CellView(
          queryId = QUERY_ID,
          attributeValues = listOf(YOUNG),
          frequencyHistogram = listOf(1L, 2L),
          impressions = 8L,
          sliceReach = mapOf(DATA_PROVIDER_A to 3L, DATA_PROVIDER_B to 1L),
        ),
        CellView(
          queryId = QUERY_ID,
          attributeValues = listOf(OLD),
          frequencyHistogram = listOf(1L, 1L),
          impressions = 3L,
          sliceReach = mapOf(DATA_PROVIDER_A to 1L, DATA_PROVIDER_B to 1L),
        ),
      )
      .inOrder()
  }

  @Test
  fun `cells returns an empty cell for a grouping combination no VID reached`() {
    // Vector 0 only reaches even positions.
    val cells =
      evaluate(
        testQuery(summands = listOf(0), maxFrequency = 1, groupingPaths = listOf(AGE_GROUP_PATH))
      )

    assertThat(cells.map { it.attributeValues })
      .containsExactly(listOf(YOUNG), listOf(OLD))
      .inOrder()
    assertThat(cells.last().frequencyHistogram).containsExactly(0L)
    assertThat(cells.last().impressions).isEqualTo(0L)
    assertThat(cells.last().sliceReach).isEmpty()
  }

  @Test
  fun `cells reports the DataProviders of the summand vectors`() {
    val evaluator =
      QueryEvaluator(
        listOf(testQuery(summands = listOf(0, 2), maxFrequency = 1)),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
      )

    assertThat(evaluator.cells().single().summandDataProviders)
      .containsExactly(DATA_PROVIDER_A, DATA_PROVIDER_B)
  }

  @Test
  fun `fingerprint changes when a summand count changes`() {
    val query = testQuery(summands = listOf(0, 1, 2), maxFrequency = 3)
    val changed = VECTORS.map { it.copyOf() }
    changed[2][3] = 1

    assertThat(evaluateCells(VECTORS, query).single().fingerprint)
      .isNotEqualTo(evaluateCells(changed, query).single().fingerprint)
  }

  @Test
  fun `fingerprint ignores counts of vectors outside the summands`() {
    val query = testQuery(summands = listOf(0, 1), maxFrequency = 3)
    val changed = VECTORS.map { it.copyOf() }
    changed[2][3] = 1

    assertThat(evaluateCells(VECTORS, query).single().fingerprint)
      .isEqualTo(evaluateCells(changed, query).single().fingerprint)
  }

  @Test
  fun `corrective pass reads failed DataProviders' summand vectors as zero`() {
    val query = testQuery(summands = listOf(0, 1, 2), maxFrequency = 3)

    val cells = evaluateCorrective(query, mapOf(cellKey() to setOf(DATA_PROVIDER_B)))

    // Summed counts without vector 2 by position: 1, 1, 3, 0, 3, 0.
    assertThat(cells)
      .containsExactly(
        CellView(
          queryId = QUERY_ID,
          attributeValues = listOf(),
          frequencyHistogram = listOf(2L, 0L, 2L),
          impressions = 8L,
          sliceReach = mapOf(DATA_PROVIDER_A to 4L),
        )
      )
  }

  @Test
  fun `corrective pass reads failed DataProviders' reach clause vectors as zero`() {
    val filter = vidFilter {
      reachClauses += VidFilterKt.reachClause { reachedSets += vectorSet { baseVectors += 2 } }
    }
    val query = testQuery(summands = listOf(0, 1), maxFrequency = 3, filter = filter)

    val cells = evaluateCorrective(query, mapOf(cellKey() to setOf(DATA_PROVIDER_B)))

    assertThat(cells.single().frequencyHistogram).containsExactly(0L, 0L, 0L).inOrder()
  }

  @Test
  fun `corrective pass evaluates only the given cells`() {
    val query =
      testQuery(
        summands = listOf(0, 1, 2),
        maxFrequency = 3,
        groupingPaths = listOf(AGE_GROUP_PATH),
      )

    val cells = evaluateCorrective(query, mapOf(cellKey(YOUNG) to setOf(DATA_PROVIDER_B)))

    // Even positions without vector 2: 1, 3, 3.
    assertThat(cells)
      .containsExactly(
        CellView(
          queryId = QUERY_ID,
          attributeValues = listOf(YOUNG),
          frequencyHistogram = listOf(1L, 0L, 2L),
          impressions = 7L,
          sliceReach = mapOf(DATA_PROVIDER_A to 3L),
        )
      )
  }

  @Test
  fun `corrective fingerprint matches a first pass over the zeroed vectors`() {
    val query = testQuery(summands = listOf(0, 1, 2), maxFrequency = 3)
    val zeroed = listOf(VECTORS[0], VECTORS[1], IntArray(VECTORS[2].size))
    val evaluator =
      QueryEvaluator.corrective(
        listOf(query),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
        mapOf(cellKey() to setOf(DATA_PROVIDER_B)),
      )
    evaluator.add(QueryEvaluator.VectorChunk(0, VECTORS))

    assertThat(evaluator.cells().single().fingerprint)
      .isEqualTo(evaluateCells(zeroed, query).single().fingerprint)
  }

  @Test
  fun `corrective throws for a cell that is not a cell of any query`() {
    assertFailsWith<IllegalArgumentException> {
      QueryEvaluator.corrective(
        listOf(testQuery(summands = listOf(0), maxFrequency = 1)),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
        mapOf(cellKey(YOUNG) to setOf(DATA_PROVIDER_B)),
      )
    }
  }

  @Test
  fun `cells reports null for a grouping path no subpopulation sets`() {
    val cells =
      evaluate(
        testQuery(summands = listOf(0), maxFrequency = 1, groupingPaths = listOf(UNSET_PATH))
      )

    assertThat(cells.single().attributeValues).isEqualTo(listOf(null))
  }

  @Test
  fun `cells returns cells in query order`() {
    val first = testQuery(summands = listOf(2), maxFrequency = 1).copy { id = "first" }
    val second = testQuery(summands = listOf(0), maxFrequency = 1).copy { id = "second" }

    val cells = evaluate(second, first)

    assertThat(cells.map { it.queryId }).containsExactly("second", "first").inOrder()
  }

  @Test
  fun `cells is the same however the vectors are chunked`() {
    val query = testQuery(summands = listOf(0, 1, 2), maxFrequency = 3)
    val evaluator = QueryEvaluator(listOf(query), DATA_PROVIDERS, ParitySubpopulationIndex)

    for (start in VECTORS[0].indices) {
      evaluator.add(
        QueryEvaluator.VectorChunk(start, VECTORS.map { it.copyOfRange(start, start + 1) })
      )
    }

    assertThat(evaluator.cells().map { CellView(it) }).isEqualTo(evaluate(query))
    assertThat(evaluator.cells().single().fingerprint)
      .isEqualTo(evaluateCells(VECTORS, query).single().fingerprint)
  }

  @Test
  fun `constructor throws when a query names a vector that is not held`() {
    assertFailsWith<IllegalArgumentException> {
      QueryEvaluator(
        listOf(testQuery(summands = listOf(0, 3), maxFrequency = 1)),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
      )
    }
  }

  @Test
  fun `constructor throws when a vector set repeats a vector`() {
    assertFailsWith<IllegalArgumentException> {
      QueryEvaluator(
        listOf(testQuery(summands = listOf(0, 0), maxFrequency = 1)),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
      )
    }
  }

  @Test
  fun `constructor throws when max_frequency is not positive`() {
    assertFailsWith<IllegalArgumentException> {
      QueryEvaluator(
        listOf(testQuery(summands = listOf(0), maxFrequency = 0)),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
      )
    }
  }

  @Test
  fun `constructor throws when query ids repeat`() {
    val query = testQuery(summands = listOf(0), maxFrequency = 1)

    assertFailsWith<IllegalArgumentException> {
      QueryEvaluator(listOf(query, query), DATA_PROVIDERS, ParitySubpopulationIndex)
    }
  }

  @Test
  fun `add throws when the chunk does not hold every vector`() {
    val evaluator =
      QueryEvaluator(
        listOf(testQuery(summands = listOf(0), maxFrequency = 1)),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
      )

    assertFailsWith<IllegalArgumentException> {
      evaluator.add(QueryEvaluator.VectorChunk(0, VECTORS.take(2)))
    }
  }

  @Test
  fun `requireIndexesCoverRange accepts indexes covering the range in any order`() {
    QueryEvaluator.requireIndexesCoverRange(listOf(2, 0, 1))
  }

  @Test
  fun `requireIndexesCoverRange throws on a gap`() {
    assertFailsWith<IllegalArgumentException> {
      QueryEvaluator.requireIndexesCoverRange(listOf(0, 2))
    }
  }

  @Test
  fun `requireIndexesCoverRange throws on a repeat`() {
    assertFailsWith<IllegalArgumentException> {
      QueryEvaluator.requireIndexesCoverRange(listOf(0, 0, 1))
    }
  }

  /** A [QueryEvaluator.Cell] with value equality. */
  private data class CellView(
    val queryId: String,
    val attributeValues: List<String?>,
    val frequencyHistogram: List<Long>,
    val impressions: Long,
    val sliceReach: Map<String, Long>,
  ) {
    constructor(
      cell: QueryEvaluator.Cell
    ) : this(
      cell.queryId,
      cell.attributeValues,
      cell.frequencyHistogram.toList(),
      cell.impressions,
      cell.sliceReach,
    )
  }

  /** Even positions fall in subpopulation 0 ([YOUNG]), odd ones in subpopulation 1 ([OLD]). */
  private object ParitySubpopulationIndex : SubpopulationIndex {
    override val subpopulationCount: Int = 2

    override fun subpopulationOf(position: Int): Int = position % 2

    override fun attributeValue(subpopulation: Int, path: String): String? =
      if (path == AGE_GROUP_PATH) listOf(YOUNG, OLD)[subpopulation] else null
  }

  companion object {
    private const val QUERY_ID = "query"
    private const val AGE_GROUP_PATH = "person.age_group"
    private const val UNSET_PATH = "person.gender"
    private const val YOUNG = "YEARS_18_TO_34"
    private const val OLD = "YEARS_35_TO_54"
    private const val DATA_PROVIDER_A = "dataProviders/a"
    private const val DATA_PROVIDER_B = "dataProviders/b"

    /** Base vectors 0 and 1 come from [DATA_PROVIDER_A], vector 2 from [DATA_PROVIDER_B]. */
    private val DATA_PROVIDERS = listOf(DATA_PROVIDER_A, DATA_PROVIDER_A, DATA_PROVIDER_B)
    private val VECTORS: List<IntArray> =
      listOf(
        intArrayOf(1, 0, 2, 0, 3, 0),
        intArrayOf(0, 1, 1, 0, 0, 0),
        intArrayOf(0, 0, 1, 2, 0, 0),
      )

    private fun testQuery(
      summands: List<Int>,
      maxFrequency: Int,
      filter: VidFilter? = null,
      groupingPaths: List<String> = listOf(),
    ): Query {
      val summandSet = vectorSet { baseVectors += summands }
      return query {
        id = QUERY_ID
        this.summands = summandSet
        if (filter != null) vidFilter = filter
        this.groupingPaths += groupingPaths
        this.maxFrequency = maxFrequency
      }
    }

    private fun evaluate(vararg queries: Query): List<CellView> =
      evaluateCells(VECTORS, *queries).map { CellView(it) }

    private fun evaluateCells(
      vectors: List<IntArray>,
      vararg queries: Query,
    ): List<QueryEvaluator.Cell> {
      val evaluator = QueryEvaluator(queries.toList(), DATA_PROVIDERS, ParitySubpopulationIndex)
      evaluator.add(QueryEvaluator.VectorChunk(0, vectors))
      return evaluator.cells()
    }

    private fun evaluateCorrective(
      query: Query,
      failedDataProviders: Map<QueryEvaluator.CellKey, Set<String>>,
    ): List<CellView> {
      val evaluator =
        QueryEvaluator.corrective(
          listOf(query),
          DATA_PROVIDERS,
          ParitySubpopulationIndex,
          failedDataProviders,
        )
      evaluator.add(QueryEvaluator.VectorChunk(0, VECTORS))
      return evaluator.cells().map { CellView(it) }
    }

    private fun cellKey(vararg attributeValues: String?) =
      QueryEvaluator.CellKey(QUERY_ID, attributeValues.toList())
  }
}
