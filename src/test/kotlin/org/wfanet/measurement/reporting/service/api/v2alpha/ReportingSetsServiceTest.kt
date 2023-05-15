/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.reportingSet

private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }

// Measurement consumers
private val MEASUREMENT_CONSUMER_KEYS: List<MeasurementConsumerKey> =
  (1L..2L).map { MeasurementConsumerKey(ExternalId(it + 110L).apiId.value) }

// Data providers
private val DATA_PROVIDER_KEYS: List<DataProviderKey> =
  (1L..3L).map { DataProviderKey(ExternalId(it + 220L).apiId.value) }

// Event group IDs and names
private val EVENT_GROUP_KEYS =
  DATA_PROVIDER_KEYS.mapIndexed { index, dataProviderKey ->
    val measurementConsumerKey = MEASUREMENT_CONSUMER_KEYS.first()
    EventGroupKey(
      measurementConsumerKey.measurementConsumerId,
      dataProviderKey.dataProviderId,
      ExternalId(index + 330L).apiId.value
    )
  }

// Internal reporting sets
private val INTERNAL_PRIMITIVE_REPORTING_SETS: List<InternalReportingSet> =
  (0L..2L).map {
    internalReportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
      externalReportingSetId = it + 440L
      filter = "AGE>18"
      displayName = "primitive_reporting_set_display_name$it"
      primitive =
        InternalReportingSetKt.primitive {
          eventGroupKeys += EVENT_GROUP_KEYS[it.toInt()].toInternal()
        }
      weightedSubsetUnions +=
        InternalReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            InternalReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = this@internalReportingSet.externalReportingSetId
              filters += this@internalReportingSet.filter
            }
          weight = 1
        }
    }
  }

private val INTERNAL_COMPOSITE_REPORTING_SET: InternalReportingSet = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
  externalReportingSetId = 450L
  filter = "GENDER==MALE"
  displayName = "composite_reporting_set_display_name"
  composite =
    InternalReportingSetKt.setExpression {
      operation = InternalReportingSet.SetExpression.Operation.UNION
      lhs =
        InternalReportingSetKt.SetExpressionKt.operand {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
        }
      rhs =
        InternalReportingSetKt.SetExpressionKt.operand {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
        }
    }
  weightedSubsetUnions +=
    InternalReportingSetKt.weightedSubsetUnion {
      primitiveReportingSetBases +=
        InternalReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
          filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
        }
      primitiveReportingSetBases +=
        InternalReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
          filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
        }
      weight = 1
    }
}

private val INTERNAL_COMPOSITE_REPORTING_SET2: InternalReportingSet =
  INTERNAL_COMPOSITE_REPORTING_SET.copy {
    externalReportingSetId += 1L
    displayName = "composite_reporting_set_display_name2"
  }

private val INTERNAL_ROOT_COMPOSITE_REPORTING_SET: InternalReportingSet = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
  externalReportingSetId = 451L
  displayName = "root_composite_reporting_set_display_name"
  composite =
    InternalReportingSetKt.setExpression {
      operation = InternalReportingSet.SetExpression.Operation.DIFFERENCE
      lhs =
        InternalReportingSetKt.SetExpressionKt.operand {
          expression =
            InternalReportingSetKt.setExpression {
              operation = InternalReportingSet.SetExpression.Operation.UNION
              lhs =
                InternalReportingSetKt.SetExpressionKt.operand {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[2].externalReportingSetId
                }
              rhs =
                InternalReportingSetKt.SetExpressionKt.operand {
                  externalReportingSetId = INTERNAL_COMPOSITE_REPORTING_SET2.externalReportingSetId
                }
            }
        }
      rhs =
        InternalReportingSetKt.SetExpressionKt.operand {
          externalReportingSetId = INTERNAL_COMPOSITE_REPORTING_SET.externalReportingSetId
        }
    }
  weightedSubsetUnions +=
    InternalReportingSetKt.weightedSubsetUnion {
      primitiveReportingSetBases +=
        InternalReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
          filters += INTERNAL_COMPOSITE_REPORTING_SET2.filter
          filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
        }
      primitiveReportingSetBases +=
        InternalReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
          filters += INTERNAL_COMPOSITE_REPORTING_SET2.filter
          filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
        }
      primitiveReportingSetBases +=
        InternalReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[2].externalReportingSetId
          filters += INTERNAL_PRIMITIVE_REPORTING_SETS[2].filter
        }
      weight = 1
    }
  weightedSubsetUnions +=
    InternalReportingSetKt.weightedSubsetUnion {
      primitiveReportingSetBases +=
        InternalReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
          filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
          filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
        }
      primitiveReportingSetBases +=
        InternalReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
          filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
          filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
        }
      weight = -1
    }
}

// Reporting sets
private val PRIMITIVE_REPORTING_SETS: List<ReportingSet> =
  INTERNAL_PRIMITIVE_REPORTING_SETS.map { internalReportingSet ->
    reportingSet {
      name = internalReportingSet.resourceName
      filter = internalReportingSet.filter
      displayName = internalReportingSet.displayName
      primitive =
        ReportingSetKt.primitive {
          eventGroups +=
            internalReportingSet.primitive.eventGroupKeysList.map { internalEventGroupKey ->
              internalEventGroupKey.resourceName
            }
        }
    }
  }

private val ROOT_COMPOSITE_REPORTING_SET: ReportingSet = reportingSet {
  name = INTERNAL_ROOT_COMPOSITE_REPORTING_SET.resourceName
  filter = INTERNAL_ROOT_COMPOSITE_REPORTING_SET.filter
  displayName = INTERNAL_ROOT_COMPOSITE_REPORTING_SET.displayName
  composite =
    ReportingSetKt.composite {
      expression =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.DIFFERENCE
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = INTERNAL_PRIMITIVE_REPORTING_SETS[2].resourceName
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = INTERNAL_COMPOSITE_REPORTING_SET2.resourceName
                    }
                }
            }
          rhs =
            ReportingSetKt.SetExpressionKt.operand {
              reportingSet = INTERNAL_COMPOSITE_REPORTING_SET.resourceName
            }
        }
    }
}

@RunWith(JUnit4::class)
class ReportingSetsServiceTest {

  private val internalReportingSetsMock: ReportingSetsCoroutineImplBase = mockService {
    onBlocking { createReportingSet(any()) }.thenReturn(INTERNAL_ROOT_COMPOSITE_REPORTING_SET)
    onBlocking { batchGetReportingSets(any()) }
      .thenAnswer {
        val request = it.arguments[0] as BatchGetReportingSetsRequest
        val internalReportingSetsMap =
          mapOf(
            INTERNAL_COMPOSITE_REPORTING_SET.externalReportingSetId to
              INTERNAL_COMPOSITE_REPORTING_SET,
            INTERNAL_COMPOSITE_REPORTING_SET2.externalReportingSetId to
              INTERNAL_COMPOSITE_REPORTING_SET2,
          ) +
            INTERNAL_PRIMITIVE_REPORTING_SETS.associateBy { internalReportingSet ->
              internalReportingSet.externalReportingSetId
            }
        batchGetReportingSetsResponse {
          reportingSets +=
            request.externalReportingSetIdsList.map { externalReportingSetId ->
              internalReportingSetsMap.getValue(externalReportingSetId)
            }
        }
      }
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalReportingSetsMock) }

  private lateinit var service: ReportingSetsService

  @Before
  fun initService() {
    service = ReportingSetsService(ReportingSetsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createReportingSet returns primitive reporting set`() = runBlocking {
    whenever(internalReportingSetsMock.createReportingSet(any()))
      .thenReturn(INTERNAL_PRIMITIVE_REPORTING_SETS.first())

    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = PRIMITIVE_REPORTING_SETS.first().copy { clearName() }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReportingSet(request) }
      }

    val expected = PRIMITIVE_REPORTING_SETS.first()

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::createReportingSet
      )
      .isEqualTo(
        INTERNAL_PRIMITIVE_REPORTING_SETS.first().copy {
          clearExternalReportingSetId()
          weightedSubsetUnions.clear()
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createReportingSet returns reporting set when primitiveReportingSetBases are not unique`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReportingSet(request) }
      }

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::createReportingSet
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(INTERNAL_ROOT_COMPOSITE_REPORTING_SET.copy { clearExternalReportingSetId() })

    assertThat(result).isEqualTo(ROOT_COMPOSITE_REPORTING_SET)
  }

  @Test
  fun `createReportingSet returns reporting set when primitiveReportingSetBases are unique`() =
    runBlocking {
      val internalCompositeReportingSet2 =
        INTERNAL_COMPOSITE_REPORTING_SET2.copy { filter = "GENDER==FEMALE" }

      val internalReportingSetsMap =
        mapOf(
          INTERNAL_COMPOSITE_REPORTING_SET.externalReportingSetId to
            INTERNAL_COMPOSITE_REPORTING_SET,
          internalCompositeReportingSet2.externalReportingSetId to internalCompositeReportingSet2,
        ) +
          INTERNAL_PRIMITIVE_REPORTING_SETS.associateBy { internalReportingSet ->
            internalReportingSet.externalReportingSetId
          }

      val internalRootCompositeReportingSet =
        INTERNAL_ROOT_COMPOSITE_REPORTING_SET.copy {
          clearExternalReportingSetId()
          weightedSubsetUnions.clear()
          weightedSubsetUnions +=
            InternalReportingSetKt.weightedSubsetUnion {
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
                  filters += internalCompositeReportingSet2.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
                  filters += internalCompositeReportingSet2.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[2].externalReportingSetId
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[2].filter
                }
              weight = 1
            }
          weightedSubsetUnions +=
            InternalReportingSetKt.weightedSubsetUnion {
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[0].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[0].filter
                }
              primitiveReportingSetBases +=
                InternalReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId =
                    INTERNAL_PRIMITIVE_REPORTING_SETS[1].externalReportingSetId
                  filters += INTERNAL_COMPOSITE_REPORTING_SET.filter
                  filters += INTERNAL_PRIMITIVE_REPORTING_SETS[1].filter
                }
              weight = -1
            }
        }

      whenever(internalReportingSetsMock.batchGetReportingSets(any())).thenAnswer {
        val request = it.arguments[0] as BatchGetReportingSetsRequest
        batchGetReportingSetsResponse {
          reportingSets +=
            request.externalReportingSetIdsList.map { externalReportingSetId ->
              internalReportingSetsMap.getValue(externalReportingSetId)
            }
        }
      }

      val request = createReportingSetRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }

      verifyProtoArgument(
          internalReportingSetsMock,
          ReportingSetsCoroutineImplBase::createReportingSet
        )
        .ignoringRepeatedFieldOrder()
        .isEqualTo(internalRootCompositeReportingSet)

      assertThat(result).isEqualTo(ROOT_COMPOSITE_REPORTING_SET)
    }

  @Test
  fun `createReportingSet throws UNAUTHENTICATED when no principal is found`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createReportingSet(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createReportingSet throws PERMISSION_DENIED when MC caller doesn't match`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.last().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createReportingSet throws UNAUTHENTICATED when caller is not MeasurementConsumer`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_KEYS.first().toName()) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("No ReportingPrincipal found")
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when parent is missing`() {
    val request = createReportingSetRequest {
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if ReportingSet is not specified`() {
    val request = createReportingSetRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if ReportingSet value is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          clearValue()
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if expression in composite is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite = ReportingSetKt.composite {}
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if operation in expression is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite = this.composite.copy { expression = this.expression.copy { clearOperation() } }
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if lhs in expression is not specified`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite = this.composite.copy { expression = this.expression.copy { clearLhs() } }
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT if name of child reporting set is not valid`() {
    val invalidReportingSetName = "invalid"
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite =
            this.composite.copy {
              expression =
                this.expression.copy {
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = invalidReportingSetName
                    }
                }
            }
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).contains(invalidReportingSetName)
  }

  @Test
  fun `createReportingSet throws PERMISSION_DENIED if caller can't access child reporting set`() {
    val inaccessibleReportingSetName =
      ReportingSetKey(
          MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId,
          ExternalId(400L).apiId.value
        )
        .toName()
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        ROOT_COMPOSITE_REPORTING_SET.copy {
          clearName()
          composite =
            this.composite.copy {
              expression =
                this.expression.copy {
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = inaccessibleReportingSetName
                    }
                }
            }
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).contains(inaccessibleReportingSetName)
  }

  @Test
  fun `createReportingSet throws NOT_FOUND when child reporting cannot be found`() = runBlocking {
    whenever(internalReportingSetsMock.batchGetReportingSets(any()))
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createReportingSet throws NOT_FOUND when child reporting cannot be found during creation`() =
    runBlocking {
      whenever(internalReportingSetsMock.createReportingSet(any()))
        .thenThrow(StatusRuntimeException(Status.NOT_FOUND))
      val request = createReportingSetRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        reportingSet = ROOT_COMPOSITE_REPORTING_SET.copy { clearName() }
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
            runBlocking { service.createReportingSet(request) }
          }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when EventGroups in primitive is empty`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        PRIMITIVE_REPORTING_SETS.first().copy {
          clearName()
          primitive = ReportingSetKt.primitive {}
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when there is any invalid EventGroup`() {
    val invalidEventGroupName = "invalid"
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        PRIMITIVE_REPORTING_SETS.first().copy {
          clearName()
          primitive = ReportingSetKt.primitive { eventGroups += invalidEventGroupName }
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).contains(invalidEventGroupName)
  }

  @Test
  fun `createReportingSet throws PERMISSION_DENIED when EventGroup doesn't belong to the caller`() {
    val notAccessibleEventGroupKey =
      EventGroupKey(
        MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId,
        DATA_PROVIDER_KEYS.first().dataProviderId,
        ExternalId(+300L).apiId.value
      )
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportingSet =
        PRIMITIVE_REPORTING_SETS.first().copy {
          clearName()
          primitive =
            ReportingSetKt.primitive { eventGroups += notAccessibleEventGroupKey.toName() }
        }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).contains(notAccessibleEventGroupKey.toName())
  }
}

private fun EventGroupKey.toInternal(): InternalReportingSet.Primitive.EventGroupKey {
  val source = this
  return InternalReportingSetKt.PrimitiveKt.eventGroupKey {
    cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
    cmmsDataProviderId = source.cmmsDataProviderId
    cmmsEventGroupId = source.cmmsEventGroupId
  }
}

private val InternalReportingSet.Primitive.EventGroupKey.resourceKey: EventGroupKey
  get() = EventGroupKey(cmmsMeasurementConsumerId, cmmsDataProviderId, cmmsEventGroupId)
private val InternalReportingSet.Primitive.EventGroupKey.resourceName: String
  get() = resourceKey.toName()

private val InternalReportingSet.resourceKey: ReportingSetKey
  get() = ReportingSetKey(cmmsMeasurementConsumerId, ExternalId(externalReportingSetId).apiId.value)
private val InternalReportingSet.resourceName: String
  get() = resourceKey.toName()
