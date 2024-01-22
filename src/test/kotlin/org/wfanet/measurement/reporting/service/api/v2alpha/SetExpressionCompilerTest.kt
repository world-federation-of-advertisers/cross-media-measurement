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

import com.google.common.truth.Truth.assertThat
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
    rhs = REPORTING_SETS[1],
  )
private val SET_EXPRESSION_ALL_UNION =
  SetOperationExpression(
    setOperator = Operator.UNION,
    lhs = SET_EXPRESSION,
    rhs = REPORTING_SETS[2],
  )
// SetExpression = A + B + C - B

private val SET_EXPRESSION_ALL_UNION_BUT_ONE =
  SetOperationExpression(
    setOperator = Operator.DIFFERENCE,
    lhs = SET_EXPRESSION_ALL_UNION,
    rhs = EXPECTED_REPORTING_SETS_LIST_ALL_UNION[1],
  )

private val EXPECTED_RESULT_FOR_ALL_UNION_SET_EXPRESSION =
  listOf(WeightedSubsetUnion(EXPECTED_REPORTING_SETS_LIST_ALL_UNION.map { it.id }, coefficient = 1))

private val EXPECTED_RESULT_FOR_ALL_UNION_BUT_ONE_SET_EXPRESSION =
  listOf(
    WeightedSubsetUnion(EXPECTED_REPORTING_SETS_LIST_ALL_UNION.map { it.id }, coefficient = 1),
    WeightedSubsetUnion(listOf(EXPECTED_REPORTING_SETS_LIST_ALL_UNION[1].id), coefficient = -1),
  )

@RunWith(JUnit4::class)
class SetExpressionCompilerTest {
  private lateinit var reportResultCompiler: SetExpressionCompiler

  @Before
  fun initService() {
    reportResultCompiler = SetExpressionCompiler()
  }

  @Test
  fun `compileSetExpression returns a list of weightedSubsetUnions for union all`() {
    val resultAllUnion = runBlocking {
      reportResultCompiler.compileSetExpression(SET_EXPRESSION_ALL_UNION, REPORTING_SETS.size)
    }
    assertThat(resultAllUnion)
      .containsExactlyElementsIn(EXPECTED_RESULT_FOR_ALL_UNION_SET_EXPRESSION)
  }

  @Test
  fun `compileSetExpression returns a list of weightedSubsetUnions for union all but one`() {
    val resultAllUnionButOne = runBlocking {
      reportResultCompiler.compileSetExpression(
        SET_EXPRESSION_ALL_UNION_BUT_ONE,
        REPORTING_SETS.size,
      )
    }
    assertThat(resultAllUnionButOne)
      .containsExactlyElementsIn(EXPECTED_RESULT_FOR_ALL_UNION_BUT_ONE_SET_EXPRESSION)
  }

  @Test
  fun `compileSetOperation throws IllegalArgumentException when a reporting set ID is invalid`() {
    val setOperationWithSetOperatorTypeNotSet =
      SET_EXPRESSION_ALL_UNION.copy(rhs = REPORTING_SETS[2].copy(id = REPORTING_SETS.size))

    assertThrows(IllegalArgumentException::class.java) {
      runBlocking {
        reportResultCompiler.compileSetExpression(
          setOperationWithSetOperatorTypeNotSet,
          REPORTING_SETS.size,
        )
      }
    }
  }
}
