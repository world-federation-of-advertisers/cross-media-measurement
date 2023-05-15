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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

@RunWith(JUnit4::class)
abstract class ReportingSetsServiceTest<T : ReportingSetsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportingSetsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
  }

  @Test
  fun `createReportingSet succeeds when ReportingSet is primitive`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)

    assertThat(createdReportingSet.weightedSubsetUnionsList)
      .containsExactly(
        ReportingSetKt.weightedSubsetUnion {
          weight = 1
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              this.externalReportingSetId = createdReportingSet.externalReportingSetId
              filters += reportingSet.filter
            }
        }
      )
    assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when event groups are repeated`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)

    assertThat(createdReportingSet.weightedSubsetUnionsList)
      .containsExactly(
        ReportingSetKt.weightedSubsetUnion {
          weight = 1
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              this.externalReportingSetId = createdReportingSet.externalReportingSetId
              filters += reportingSet.filter
            }
        }
      )
    assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when no new Event Groups are created`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val reportingSet2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    service.createReportingSet(reportingSet)
    val createdReportingSet2 = service.createReportingSet(reportingSet2)

    assertThat(createdReportingSet2.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when ReportingSet is composite`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.INTERSECTION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              externalReportingSetId =
                                createdPrimitiveReportingSet.externalReportingSetId
                            }
                        }
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          weight = 6
        }
    }

    val createdReportingSet = service.createReportingSet(compositeReportingSet)

    assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when ReportingSet contains another composite `() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val innerCompositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
        }
    }

    val createdInnerCompositeReportingSet = service.createReportingSet(innerCompositeReportingSet)

    val innerCompositeReportingSet2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
        }
    }

    val createdInnerCompositeReportingSet2 = service.createReportingSet(innerCompositeReportingSet2)

    val outerCompositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName3"
      filter = "filter3"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId =
                        createdInnerCompositeReportingSet.externalReportingSetId
                    }
                }
            }

          rhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId =
                        createdInnerCompositeReportingSet2.externalReportingSetId
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
        }
    }

    val createdOuterCompositeReportingSet = service.createReportingSet(outerCompositeReportingSet)

    assertThat(createdOuterCompositeReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when Reporting Set is primitive with no filter`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)

    assertThat(createdReportingSet.weightedSubsetUnionsList)
      .containsExactly(
        ReportingSetKt.weightedSubsetUnion {
          weight = 1
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              this.externalReportingSetId = createdReportingSet.externalReportingSetId
            }
        }
      )
    assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when ReportingSet is composite with no filters`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.INTERSECTION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              externalReportingSetId =
                                createdPrimitiveReportingSet.externalReportingSetId
                            }
                        }
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
            }
          weight = 5
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
            }
          weight = 6
        }
    }

    val createdReportingSet = service.createReportingSet(compositeReportingSet)

    assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `CreateReportingSet throws INVALID_ARGUMENT when ReportingSet missing value`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = 123
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createReportingSet(reportingSet) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `CreateReportingSet throws INVALID_ARGUMENT when set expression missing lhs`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          rhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createReportingSet(compositeReportingSet) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `CreateReportingSet throws NOT_FOUND when ReportingSet in basis not found`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = 123
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createReportingSet(compositeReportingSet) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `CreateReportingSet throws NOT_FOUND when ReportingSet in operand not found`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = 123 }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = 123
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createReportingSet(compositeReportingSet) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `CreateReportingSet throws FAILED_PRECONDITION when MC not found`() = runBlocking {
    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = "123"
      displayName = "displayName"
      filter = "filter"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = 123 }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = 123
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createReportingSet(compositeReportingSet) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("Measurement Consumer")
  }

  @Test
  fun `batchGetReportingSets succeeds when ReportingSet is primitive`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)

    val retrievedReportingSets =
      service
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportingSetIds += createdReportingSet.externalReportingSetId
          }
        )
        .reportingSetsList

    assertThat(retrievedReportingSets)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        reportingSet.copy {
          externalReportingSetId = createdReportingSet.externalReportingSetId
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = 1
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  this.externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += reportingSet.filter
                }
            }
        }
      )
  }

  @Test
  fun `batchGetReportingSets succeeds when ReportingSet is composite`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.INTERSECTION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              externalReportingSetId =
                                createdPrimitiveReportingSet.externalReportingSetId
                            }
                        }
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
              filters += "filter1"
              filters += "filter2"
            }
          weight = 6
        }
    }

    val createdReportingSet = service.createReportingSet(compositeReportingSet)

    val retrievedReportingSets =
      service
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportingSetIds += createdReportingSet.externalReportingSetId
          }
        )
        .reportingSetsList

    assertThat(retrievedReportingSets)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        compositeReportingSet.copy {
          externalReportingSetId = createdReportingSet.externalReportingSetId
        }
      )
  }

  @Test
  fun `batchGetReportingSets succeeds when one is composite and one is primitive`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val primitiveReportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        displayName = "displayName"
        filter = "filter"

        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

      val compositeReportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        displayName = "displayName2"
        filter = "filter2"

        composite =
          ReportingSetKt.setExpression {
            operation = ReportingSet.SetExpression.Operation.UNION
            lhs =
              ReportingSetKt.SetExpressionKt.operand {
                expression =
                  ReportingSetKt.setExpression {
                    operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                    lhs =
                      ReportingSetKt.SetExpressionKt.operand {
                        externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                      }
                    rhs =
                      ReportingSetKt.SetExpressionKt.operand {
                        expression =
                          ReportingSetKt.setExpression {
                            operation = ReportingSet.SetExpression.Operation.INTERSECTION
                            lhs =
                              ReportingSetKt.SetExpressionKt.operand {
                                externalReportingSetId =
                                  createdPrimitiveReportingSet.externalReportingSetId
                              }
                          }
                      }
                  }
              }
          }

        weightedSubsetUnions +=
          ReportingSetKt.weightedSubsetUnion {
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            weight = 5
          }

        weightedSubsetUnions +=
          ReportingSetKt.weightedSubsetUnion {
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            weight = 6
          }
      }

      val createdReportingSet = service.createReportingSet(compositeReportingSet)

      val retrievedReportingSets =
        service
          .batchGetReportingSets(
            batchGetReportingSetsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportingSetIds += createdReportingSet.externalReportingSetId
              externalReportingSetIds += createdPrimitiveReportingSet.externalReportingSetId
            }
          )
          .reportingSetsList

      assertThat(retrievedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          compositeReportingSet.copy {
            externalReportingSetId = createdReportingSet.externalReportingSetId
          },
          primitiveReportingSet.copy {
            externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
            weightedSubsetUnions +=
              ReportingSetKt.weightedSubsetUnion {
                weight = 1
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    this.externalReportingSetId =
                      createdPrimitiveReportingSet.externalReportingSetId
                    filters += primitiveReportingSet.filter
                  }
              }
          }
        )
    }

  @Test
  fun `batchGetReportingSets succeeds when no filter in basis`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.DIFFERENCE
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                    }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
            }
          weight = 6
        }
    }

    val createdReportingSet = service.createReportingSet(compositeReportingSet)

    val retrievedReportingSets =
      service
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportingSetIds += createdReportingSet.externalReportingSetId
          }
        )
        .reportingSetsList

    assertThat(retrievedReportingSets)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        compositeReportingSet.copy {
          externalReportingSetId = createdReportingSet.externalReportingSetId
        }
      )
  }

  @Test
  fun `batchGetReportingSets succeeds when no filter in primitive reporting set`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val primitiveReportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        displayName = "displayName"

        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = service.createReportingSet(primitiveReportingSet)

      val retrievedReportingSets =
        service
          .batchGetReportingSets(
            batchGetReportingSetsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportingSetIds += createdReportingSet.externalReportingSetId
            }
          )
          .reportingSetsList

      assertThat(retrievedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          createdReportingSet.copy {
            externalReportingSetId = createdReportingSet.externalReportingSetId
          }
        )
    }

  @Test
  fun `batchGetReportingSets throws NOT_FOUND when ReportingSet not found`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportingSetIds += createdReportingSet.externalReportingSetId
            externalReportingSetIds += 1L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `batchGetReportingSets throws INVALID_ARGUMENT when too many requested`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            for (i in 1L..10000L) {
              externalReportingSetIds += i
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `streamReportingSets filters when measurement consumer filter is set`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + 2 }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val reportingSet2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + 2
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1237"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)
    service.createReportingSet(reportingSet2)

    val retrievedReportingSets =
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .toList()

    assertThat(retrievedReportingSets)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        reportingSet.copy {
          externalReportingSetId = createdReportingSet.externalReportingSetId
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = 1
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  this.externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += reportingSet.filter
                }
            }
        }
      )
  }

  @Test
  fun `streamReportingSets filters when id after filter is set`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val reportingSet2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)
    val createdReportingSet2 = service.createReportingSet(reportingSet2)

    val afterId =
      minOf(createdReportingSet.externalReportingSetId, createdReportingSet2.externalReportingSetId)

    val retrievedReportingSets =
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportingSetIdAfter = afterId
              }
          }
        )
        .toList()

    if (createdReportingSet.externalReportingSetId == afterId) {
      assertThat(retrievedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          reportingSet.copy {
            externalReportingSetId = createdReportingSet2.externalReportingSetId
            weightedSubsetUnions +=
              ReportingSetKt.weightedSubsetUnion {
                weight = 1
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    this.externalReportingSetId = createdReportingSet2.externalReportingSetId
                    filters += reportingSet.filter
                  }
              }
          }
        )
    } else {
      assertThat(retrievedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          reportingSet2.copy {
            externalReportingSetId = createdReportingSet.externalReportingSetId
            weightedSubsetUnions +=
              ReportingSetKt.weightedSubsetUnion {
                weight = 1
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    this.externalReportingSetId = createdReportingSet.externalReportingSetId
                    filters += reportingSet.filter
                  }
              }
          }
        )
    }
  }

  @Test
  fun `streamReportingSets limits the number of results when limit is set`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val reportingSet2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)
    val createdReportingSet2 = service.createReportingSet(reportingSet2)

    val retrievedReportingSets =
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            limit = 1
          }
        )
        .toList()

    if (createdReportingSet.externalReportingSetId < createdReportingSet2.externalReportingSetId) {
      assertThat(retrievedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          reportingSet.copy {
            externalReportingSetId = createdReportingSet.externalReportingSetId
            weightedSubsetUnions +=
              ReportingSetKt.weightedSubsetUnion {
                weight = 1
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    this.externalReportingSetId = createdReportingSet.externalReportingSetId
                    filters += reportingSet.filter
                  }
              }
          }
        )
    } else {
      assertThat(retrievedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          reportingSet2.copy {
            externalReportingSetId = createdReportingSet2.externalReportingSetId
            weightedSubsetUnions +=
              ReportingSetKt.weightedSubsetUnion {
                weight = 1
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    this.externalReportingSetId = createdReportingSet2.externalReportingSetId
                    filters += reportingSet.filter
                  }
              }
          }
        )
    }
  }

  @Test
  fun `streamReportingSets returns empty flow when no reporting sets are found`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val retrievedReportingSets =
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .toList()

    assertThat(retrievedReportingSets).hasSize(0)
  }

  @Test
  fun `streamReportingSets throws INVALID_ARGUMENT when MC filter missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.streamReportingSets(streamReportingSetsRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
