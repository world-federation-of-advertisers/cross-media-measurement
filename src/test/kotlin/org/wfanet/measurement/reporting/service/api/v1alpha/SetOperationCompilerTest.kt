// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.copy

// Measurement consumer IDs and names
private const val MEASUREMENT_CONSUMER_EXTERNAL_ID = 111L
private const val MEASUREMENT_CONSUMER_EXTERNAL_ID_2 = 112L
private val MEASUREMENT_CONSUMER_REFERENCE_ID = externalIdToApiId(MEASUREMENT_CONSUMER_EXTERNAL_ID)
private val MEASUREMENT_CONSUMER_REFERENCE_ID_2 =
  externalIdToApiId(MEASUREMENT_CONSUMER_EXTERNAL_ID_2)
private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(MEASUREMENT_CONSUMER_REFERENCE_ID).toName()
private val MEASUREMENT_CONSUMER_NAME_2 =
  MeasurementConsumerKey(MEASUREMENT_CONSUMER_REFERENCE_ID_2).toName()

// Reporting set IDs and names
private val REPORTING_SET_EXTERNAL_ID = 331L
private val REPORTING_SET_EXTERNAL_ID_2 = 332L
private val REPORTING_SET_EXTERNAL_ID_3 = 333L

private val REPORTING_SET_NAME =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID))
    .toName()
private val REPORTING_SET_NAME_2 =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID_2))
    .toName()
private val REPORTING_SET_NAME_3 =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID_3))
    .toName()

private val SET_OPERATION_DISPLAY_NAME = "SET_OPERATION_DISPLAY_NAME"

private val SET_OPERATION = setOperation {
  type = Metric.SetOperation.Type.UNION
  lhs = operand { reportingSet = REPORTING_SET_NAME }
  rhs = operand { reportingSet = REPORTING_SET_NAME_2 }
}

private val SET_OPERATION_ALL_UNION = setOperation {
  type = Metric.SetOperation.Type.UNION
  lhs = operand { operation = SET_OPERATION }
  rhs = operand { reportingSet = REPORTING_SET_NAME_3 }
}

private val NAMED_SET_OPERATION_ALL_UNION = namedSetOperation {
  displayName = SET_OPERATION_DISPLAY_NAME
  setOperation = SET_OPERATION_ALL_UNION
}

private val EXPECTED_REPORTING_SET_NAMES_LIST =
  listOf(REPORTING_SET_NAME, REPORTING_SET_NAME_2, REPORTING_SET_NAME_3).sorted()

private val EXPECTED_RESULT_FOR_ALL_UNION_SET_OPERATION =
  listOf(WeightedMeasurement(EXPECTED_REPORTING_SET_NAMES_LIST, coefficient = 1))

private val EXPECTED_CACHE_FOR_ALL_UNION_SET_OPERATION =
  mapOf(
    EXPECTED_REPORTING_SET_NAMES_LIST.size to
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

@RunWith(JUnit4::class)
class SetOperationCompilerTest {
  private lateinit var reportResultCompiler: SetOperationCompiler

  @Before
  fun initService() {
    reportResultCompiler = SetOperationCompiler()
  }

  @Test
  fun `compileSetOperation returns a list of weightedMeasurements and store it in the cache`() {
    val result = runBlocking {
      reportResultCompiler.compileSetOperation(NAMED_SET_OPERATION_ALL_UNION)
    }
    val primitiveRegionCache = reportResultCompiler.getPrimitiveRegionCache()

    assertThat(result).containsExactlyElementsIn(EXPECTED_RESULT_FOR_ALL_UNION_SET_OPERATION)
    assertThat(primitiveRegionCache).isEqualTo(EXPECTED_CACHE_FOR_ALL_UNION_SET_OPERATION)
  }

  @Test
  fun `compileSetOperation reuses the computation in the cache when there exists one`() {
    runBlocking { reportResultCompiler.compileSetOperation(NAMED_SET_OPERATION_ALL_UNION) }
    val firstRoundPrimitiveRegionCache = reportResultCompiler.getPrimitiveRegionCache()

    runBlocking { reportResultCompiler.compileSetOperation(NAMED_SET_OPERATION_ALL_UNION) }
    val secondRoundPrimitiveRegionCache = reportResultCompiler.getPrimitiveRegionCache()

    assertThat(firstRoundPrimitiveRegionCache).isEqualTo(secondRoundPrimitiveRegionCache)
  }

  @Test
  fun `compileSetOperation throws INVALID_ARGUMENT when lhs in SetOperation is not set`() {
    val namedSetOperationWithLhsNotSet =
      NAMED_SET_OPERATION_ALL_UNION.copy {
        setOperation = SET_OPERATION_ALL_UNION.copy { clearLhs() }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { reportResultCompiler.compileSetOperation(namedSetOperationWithLhsNotSet) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("lhs in SetOperation must be set.")
  }

  @Test
  fun `compileSetOperation throws INVALID_ARGUMENT when the operand type of lhs is not set`() {
    val namedSetOperationWithLhsOperandTypeNotSet =
      NAMED_SET_OPERATION_ALL_UNION.copy {
        setOperation = SET_OPERATION_ALL_UNION.copy { lhs = operand {} }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          reportResultCompiler.compileSetOperation(namedSetOperationWithLhsOperandTypeNotSet)
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Operand type of lhs in SetOperation must be set.")
  }

  @Test
  fun `compileSetOperation throws INVALID_ARGUMENT when the set operator type is not set`() {
    val namedSetOperationWithSetOperatorTypeNotSet =
      NAMED_SET_OPERATION_ALL_UNION.copy {
        setOperation = SET_OPERATION_ALL_UNION.copy { clearType() }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          reportResultCompiler.compileSetOperation(namedSetOperationWithSetOperatorTypeNotSet)
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Set operator type is not specified.")
  }

  @Test
  fun `compileSetOperation throws INVALID_ARGUMENT when reportingSets are not unique`() {
    val namedSetOperationWithDuplicateReportingSets =
      NAMED_SET_OPERATION_ALL_UNION.copy {
        setOperation =
          SET_OPERATION_ALL_UNION.copy { rhs = operand { reportingSet = REPORTING_SET_NAME_2 } }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          reportResultCompiler.compileSetOperation(namedSetOperationWithDuplicateReportingSets)
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Reporting sets in SetOperation should be unique.")
  }
}
