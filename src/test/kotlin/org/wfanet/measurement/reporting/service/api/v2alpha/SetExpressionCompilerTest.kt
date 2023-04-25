/*
 * Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

// Reporting set IDs and names
private val REPORTING_SETS = (0..2).map { ReportingSet(it) }

private val EXPECTED_REPORTING_SETS_LIST_ALL_UNION = REPORTING_SETS.sortedBy { it.id }

private val SET_EXPRESSION =
  SetOperationExpression(
    setOperator = Operator.UNION,
    lhs = REPORTING_SETS[0],
    rhs = REPORTING_SETS[1]
  )
private val SET_EXPRESSION_ALL_UNION =
  SetOperationExpression(
    setOperator = Operator.UNION,
    lhs = SET_EXPRESSION,
    rhs = REPORTING_SETS[2]
  )
// SetExpression = A + B + C - B

private val SET_EXPRESSION_ALL_UNION_BUT_ONE =
  SetOperationExpression(
    setOperator = Operator.DIFFERENCE,
    lhs = SET_EXPRESSION_ALL_UNION,
    rhs = EXPECTED_REPORTING_SETS_LIST_ALL_UNION[1]
  )

private val EXPECTED_RESULT_FOR_ALL_UNION_SET_EXPRESSION =
  listOf(WeightedSubsetUnion(EXPECTED_REPORTING_SETS_LIST_ALL_UNION.map { it.id }, coefficient = 1))

private val EXPECTED_RESULT_FOR_ALL_UNION_BUT_ONE_SET_EXPRESSION =
  listOf(
    WeightedSubsetUnion(EXPECTED_REPORTING_SETS_LIST_ALL_UNION.map { it.id }, coefficient = 1),
    WeightedSubsetUnion(listOf(EXPECTED_REPORTING_SETS_LIST_ALL_UNION[1].id), coefficient = -1)
  )

private val EXPECTED_CACHE_FOR_ALL_UNION_SET_EXPRESSION =
  mapOf(
    REPORTING_SETS.size to
      mapOf(
        1UL to mapOf(6UL to -1, 7UL to 1),
        2UL to mapOf(5UL to -1, 7UL to 1),
        3UL to mapOf(4UL to -1, 5UL to 1, 6UL to 1, 7UL to -1),
        4UL to mapOf(3UL to -1, 7UL to 1),
        5UL to mapOf(2UL to -1, 3UL to 1, 6UL to 1, 7UL to -1),
        6UL to mapOf(1UL to -1, 3UL to 1, 5UL to 1, 7UL to -1),
        7UL to mapOf(1UL to 1, 2UL to 1, 3UL to -1, 4UL to 1, 5UL to -1, 6UL to -1, 7UL to 1),
      )
  )

// {4: {3: -1, 7: 1}, 1: {6: -1, 7: 1}, 5: {2: -1, 3: 1, 6: 1, 7: -1}}
private val EXPECTED_CACHE_FOR_ALL_UNION_BUT_ONE_SET_EXPRESSION =
  mapOf(
    REPORTING_SETS.size to
      mapOf(
        1UL to mapOf(6UL to -1, 7UL to 1),
        4UL to mapOf(3UL to -1, 7UL to 1),
        5UL to mapOf(2UL to -1, 3UL to 1, 6UL to 1, 7UL to -1),
      )
  )

private val EXPECTED_CACHE_FOR_VENN_DIAGRAM_DECOMPOSITION =
  mapOf(
    REPORTING_SETS.size to
      listOf(
        // 1(=b’001’), 3(=b’011’), 5(=b’101’), 7(=b’111’)
        setOf(1UL, 3UL, 5UL, 7UL),
        // 2(=b’010’), 3(=b’011’), 6(=b’110’), 7(=b’111’)
        setOf(2UL, 3UL, 6UL, 7UL),
        // 4(=b’100’), 5(=b’101’), 6(=b’110’), 7(=b’111’)
        setOf(4UL, 5UL, 6UL, 7UL),
      )
  )

@RunWith(JUnit4::class)
class SetExpressionCompilerTest {
  private lateinit var reportResultCompiler: SetExpressionCompiler

  @Before
  fun initService() {
    reportResultCompiler = SetExpressionCompiler()
  }

  @Test
  fun `compileSetExpression returns a list of weightedSubsetUnions and store it in the cache`() {
    val resultAllUnionButOne = runBlocking {
      reportResultCompiler.compileSetExpression(
        SET_EXPRESSION_ALL_UNION_BUT_ONE,
        REPORTING_SETS.size
      )
    }

    Truth.assertThat(resultAllUnionButOne)
      .containsExactlyElementsIn(EXPECTED_RESULT_FOR_ALL_UNION_BUT_ONE_SET_EXPRESSION)
    Truth.assertThat(reportResultCompiler.getPrimitiveRegionCache())
      .isEqualTo(EXPECTED_CACHE_FOR_ALL_UNION_BUT_ONE_SET_EXPRESSION)
    Truth.assertThat(reportResultCompiler.getVennDiagramDecompositionCache())
      .isEqualTo(EXPECTED_CACHE_FOR_VENN_DIAGRAM_DECOMPOSITION)

    val resultAllUnion = runBlocking {
      reportResultCompiler.compileSetExpression(SET_EXPRESSION_ALL_UNION, REPORTING_SETS.size)
    }

    Truth.assertThat(resultAllUnion)
      .containsExactlyElementsIn(EXPECTED_RESULT_FOR_ALL_UNION_SET_EXPRESSION)
    Truth.assertThat(reportResultCompiler.getPrimitiveRegionCache())
      .isEqualTo(EXPECTED_CACHE_FOR_ALL_UNION_SET_EXPRESSION)
    Truth.assertThat(reportResultCompiler.getVennDiagramDecompositionCache())
      .isEqualTo(EXPECTED_CACHE_FOR_VENN_DIAGRAM_DECOMPOSITION)
  }

  @Test
  fun `compileSetExpression reuses the computation in the cache when there exists one`() {
    runBlocking {
      reportResultCompiler.compileSetExpression(SET_EXPRESSION_ALL_UNION, REPORTING_SETS.size)
    }
    val firstRoundPrimitiveRegionCache = reportResultCompiler.getPrimitiveRegionCache()
    val firstRoundVennDiagramDecompositionCache =
      reportResultCompiler.getVennDiagramDecompositionCache()

    runBlocking {
      reportResultCompiler.compileSetExpression(SET_EXPRESSION_ALL_UNION, REPORTING_SETS.size)
    }
    val secondRoundPrimitiveRegionCache = reportResultCompiler.getPrimitiveRegionCache()
    val secondRoundVennDiagramDecompositionCache =
      reportResultCompiler.getVennDiagramDecompositionCache()

    Truth.assertThat(firstRoundPrimitiveRegionCache).isEqualTo(secondRoundPrimitiveRegionCache)
    Truth.assertThat(firstRoundVennDiagramDecompositionCache)
      .isEqualTo(secondRoundVennDiagramDecompositionCache)
  }

  @Test
  fun `compileSetOperation throws IllegalArgumentException when a reporting set ID is invalid`() {
    val setOperationWithSetOperatorTypeNotSet =
      SET_EXPRESSION_ALL_UNION.copy(rhs = REPORTING_SETS[2].copy(id = REPORTING_SETS.size))

    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        runBlocking {
          reportResultCompiler.compileSetExpression(
            setOperationWithSetOperatorTypeNotSet,
            REPORTING_SETS.size
          )
        }
      }
    Truth.assertThat(exception.message)
      .isEqualTo("ReportingSet's ID must be less than the number of total reporting sets.")
  }
}
