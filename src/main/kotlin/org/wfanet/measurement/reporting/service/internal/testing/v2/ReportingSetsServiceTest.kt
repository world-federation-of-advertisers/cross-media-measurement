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
import com.google.rpc.errorInfo
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
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.ErrorCode
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

@RunWith(JUnit4::class)
abstract class ReportingSetsServiceTest<T : ReportingSetsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportingSetsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }

      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet
          externalReportingSetId = "external-reporting-set-id"
        }
      )

    assertThat(createdReportingSet.weightedSubsetUnionsList)
      .containsExactly(
        ReportingSetKt.weightedSubsetUnion {
          weight = 1
          binaryRepresentation = 1
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet
          externalReportingSetId = "reporting-set-id"
        }
      )

    assertThat(createdReportingSet.weightedSubsetUnionsList)
      .containsExactly(
        ReportingSetKt.weightedSubsetUnion {
          weight = 1
          binaryRepresentation = 1
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val reportingSet2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    service.createReportingSet(
      createReportingSetRequest {
        this.reportingSet = reportingSet
        externalReportingSetId = "reporting-set-id"
      }
    )
    val createdReportingSet2 =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet2
          externalReportingSetId = "reportingSetId2"
        }
      )

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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = primitiveReportingSet
          externalReportingSetId = "primitive-reporting-set-id"
        }
      )

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
          binaryRepresentation = 1
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
          binaryRepresentation = 1
          weight = 6
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = compositeReportingSet
          externalReportingSetId = "composite-reporting-set-id"
        }
      )

    assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when ReportingSet contains another composite`() = runBlocking {
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = primitiveReportingSet
          externalReportingSetId = "primitive-reporting-set-id"
        }
      )

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

    val createdInnerCompositeReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = innerCompositeReportingSet
          externalReportingSetId = "innerComposite-reporting-set-id"
        }
      )

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

    val createdInnerCompositeReportingSet2 =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = innerCompositeReportingSet2
          externalReportingSetId = "innerCompositeReportingSet2"
        }
      )

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
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdOuterCompositeReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = outerCompositeReportingSet
          externalReportingSetId = "outerComposite-reporting-set-id"
        }
      )

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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet
          externalReportingSetId = "reporting-set-id"
        }
      )

    assertThat(createdReportingSet.weightedSubsetUnionsList)
      .containsExactly(
        ReportingSetKt.weightedSubsetUnion {
          weight = 1
          binaryRepresentation = 1
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = primitiveReportingSet
          externalReportingSetId = "primitive-reporting-set-id"
        }
      )

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
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = compositeReportingSet
          externalReportingSetId = "composite-reporting-set-id"
        }
      )

    assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when request missing external ID`() =
    runBlocking {
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
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }

            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsDataProviderId = "2235"
                cmmsEventGroupId = "2236"
              }
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createReportingSet(createReportingSetRequest { this.reportingSet = reportingSet })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createReportingSet throws ALREADY_EXISTS when the external ID exists`() = runBlocking {
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    service.createReportingSet(
      createReportingSetRequest {
        this.reportingSet = reportingSet
        externalReportingSetId = "reporting-set-id"
      }
    )
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = reportingSet
            externalReportingSetId = "reporting-set-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when ReportingSet missing value`() = runBlocking {
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
              externalReportingSetId = "123"
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = reportingSet
            externalReportingSetId = "reporting-set-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when set expression missing lhs`() = runBlocking {
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = primitiveReportingSet
          externalReportingSetId = "primitive-reporting-set-id"
        }
      )

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
      assertFailsWith<StatusRuntimeException> {
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = compositeReportingSet
            externalReportingSetId = "composite-reporting-set-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportingSet throws FAILED_PRECONDITION when ReportingSet in basis not found`() =
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
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdPrimitiveReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = primitiveReportingSet
            externalReportingSetId = "primitive-reporting-set-id"
          }
        )

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
                externalReportingSetId = "123"
                filters += "filter1"
                filters += "filter2"
              }
            weight = 5
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createReportingSet(
            createReportingSetRequest {
              this.reportingSet = compositeReportingSet
              externalReportingSetId = "composite-reporting-set-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.message).contains("ReportingSet")
    }

  @Test
  fun `createReportingSet throws FAILED_PRECONDITION when ReportingSet in operand not found`() =
    runBlocking {
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
                    lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "123" }
                  }
              }
          }

        weightedSubsetUnions +=
          ReportingSetKt.weightedSubsetUnion {
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = "123"
                filters += "filter1"
                filters += "filter2"
              }
            weight = 5
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createReportingSet(
            createReportingSetRequest {
              this.reportingSet = compositeReportingSet
              externalReportingSetId = "composite-reporting-set-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.message).contains("ReportingSet")
    }

  @Test
  fun `createReportingSet throws NOT_FOUND when MC not found`() = runBlocking {
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
                  lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "123" }
                }
            }
        }

      weightedSubsetUnions +=
        ReportingSetKt.weightedSubsetUnion {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis {
              externalReportingSetId = "123"
              filters += "filter1"
              filters += "filter2"
            }
          weight = 5
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = compositeReportingSet
            externalReportingSetId = "composite-reporting-set-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Measurement Consumer")
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when creating composite Campaign Group`() =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )
      val primitiveReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = reportingSet {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              displayName = "displayName"
              filter = "filter"

              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = "1235"
                      cmmsEventGroupId = "1236"
                    }
                }
            }
            externalReportingSetId = "primitive-reporting-set-id"
          }
        )
      val request = createReportingSetRequest {
        externalReportingSetId = "composite-reporting-set-id"
        this.reportingSet = reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalCampaignGroupId = this@createReportingSetRequest.externalReportingSetId
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
                          externalReportingSetId = primitiveReportingSet.externalReportingSetId
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          expression =
                            ReportingSetKt.setExpression {
                              operation = ReportingSet.SetExpression.Operation.INTERSECTION
                              lhs =
                                ReportingSetKt.SetExpressionKt.operand {
                                  externalReportingSetId =
                                    primitiveReportingSet.externalReportingSetId
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
                  externalReportingSetId = primitiveReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = primitiveReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
              binaryRepresentation = 1
              weight = 5
            }

          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = primitiveReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = primitiveReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
              binaryRepresentation = 1
              weight = 6
            }
          details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          // Attempt to create a composite ReportingSet that is a Campaign Group.
          service.createReportingSet(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = ReportingInternalException.DOMAIN
            reason = ErrorCode.CAMPAIGN_GROUP_INVALID.name
            metadata["cmmsMeasurementConsumerId"] = CMMS_MEASUREMENT_CONSUMER_ID
            metadata["externalReportingSetId"] = request.externalReportingSetId
          }
        )
    }

  @Test
  fun `createReportingSet throws FAILED_PRECONDITION when referenced Campaign Group is invalid`() =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )
      val primitiveReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = reportingSet {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              displayName = "displayName"
              filter = "filter"

              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = "1235"
                      cmmsEventGroupId = "1236"
                    }
                }
            }
            externalReportingSetId = "primitive-reporting-set-id"
          }
        )
      val compositeReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            externalReportingSetId = "composite-reporting-set-id"
            reportingSet = reportingSet {
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
                              externalReportingSetId = primitiveReportingSet.externalReportingSetId
                            }
                          rhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              expression =
                                ReportingSetKt.setExpression {
                                  operation = ReportingSet.SetExpression.Operation.INTERSECTION
                                  lhs =
                                    ReportingSetKt.SetExpressionKt.operand {
                                      externalReportingSetId =
                                        primitiveReportingSet.externalReportingSetId
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
                      externalReportingSetId = primitiveReportingSet.externalReportingSetId
                      filters += "filter1"
                      filters += "filter2"
                    }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = primitiveReportingSet.externalReportingSetId
                      filters += "filter1"
                      filters += "filter2"
                    }
                  binaryRepresentation = 1
                  weight = 5
                }

              weightedSubsetUnions +=
                ReportingSetKt.weightedSubsetUnion {
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = primitiveReportingSet.externalReportingSetId
                      filters += "filter1"
                      filters += "filter2"
                    }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = primitiveReportingSet.externalReportingSetId
                      filters += "filter1"
                      filters += "filter2"
                    }
                  binaryRepresentation = 1
                  weight = 6
                }
              details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
            }
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          // Attempt to create a ReportingSet using a composite ReportingSet as its Campaign Group.
          service.createReportingSet(
            createReportingSetRequest {
              externalReportingSetId = "my-reporting-set"
              this.reportingSet = reportingSet {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalCampaignGroupId = compositeReportingSet.externalReportingSetId
                displayName = "displayName"
                filter = "filter"

                primitive =
                  ReportingSetKt.primitive {
                    eventGroupKeys +=
                      ReportingSetKt.PrimitiveKt.eventGroupKey {
                        cmmsDataProviderId = "1235"
                        cmmsEventGroupId = "1236"
                      }
                  }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = ReportingInternalException.DOMAIN
            reason = ErrorCode.CAMPAIGN_GROUP_INVALID.name
            metadata["cmmsMeasurementConsumerId"] = CMMS_MEASUREMENT_CONSUMER_ID
            metadata["externalReportingSetId"] = compositeReportingSet.externalReportingSetId
          }
        )
    }

  @Test
  fun `createReportingSet throws FAILED_PRECONDITION when referenced Campaign Group is not found`() =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createReportingSet(
            createReportingSetRequest {
              externalReportingSetId = "my-reporting-set"
              this.reportingSet = reportingSet {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalCampaignGroupId = "bogus"
                displayName = "displayName"
                filter = "filter"

                primitive =
                  ReportingSetKt.primitive {
                    eventGroupKeys +=
                      ReportingSetKt.PrimitiveKt.eventGroupKey {
                        cmmsDataProviderId = "1235"
                        cmmsEventGroupId = "1236"
                      }
                  }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = ReportingInternalException.DOMAIN
            reason = ErrorCode.REPORTING_SET_NOT_FOUND.name
            metadata["cmmsMeasurementConsumerId"] = CMMS_MEASUREMENT_CONSUMER_ID
            metadata["externalReportingSetId"] = "bogus"
          }
        )
    }

  @Test
  fun `createReportingSet returns created ReportingSet when it is a Campaign Group`() =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val response: ReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            externalReportingSetId = "my-reporting-set"
            this.reportingSet = reportingSet {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalCampaignGroupId = this@createReportingSetRequest.externalReportingSetId
              displayName = "displayName"
              filter = "filter"

              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = "1235"
                      cmmsEventGroupId = "1236"
                    }
                }
            }
          }
        )

      assertThat(response.externalCampaignGroupId).isEqualTo(response.externalReportingSetId)
      assertThat(response)
        .isEqualTo(
          service
            .batchGetReportingSets(
              batchGetReportingSetsRequest {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportingSetIds += response.externalReportingSetId
              }
            )
            .reportingSetsList
            .first()
        )
    }

  @Test
  fun `createReportingSet returns created ReportingSet when referenced Campaign Group is valid`() =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )
      val campaignGroup: ReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            externalReportingSetId = "my-campaign-group"
            this.reportingSet = reportingSet {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalCampaignGroupId = this@createReportingSetRequest.externalReportingSetId
              displayName = "displayName"
              filter = "filter"

              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = "1235"
                      cmmsEventGroupId = "1236"
                    }
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = "1236"
                      cmmsEventGroupId = "1237"
                    }
                }
            }
          }
        )

      val response: ReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            externalReportingSetId = "my-reporting-set"
            reportingSet =
              campaignGroup.copy {
                externalCampaignGroupId = campaignGroup.externalReportingSetId
                primitive =
                  ReportingSetKt.primitive {
                    eventGroupKeys += campaignGroup.primitive.eventGroupKeysList.first()
                  }
                weightedSubsetUnions.clear()
              }
          }
        )

      assertThat(response.externalCampaignGroupId).isEqualTo(campaignGroup.externalReportingSetId)
      assertThat(response)
        .isEqualTo(
          service
            .batchGetReportingSets(
              batchGetReportingSetsRequest {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportingSetIds += response.externalReportingSetId
              }
            )
            .reportingSetsList
            .first()
        )
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "2235"
              cmmsEventGroupId = "2236"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet
          externalReportingSetId = "reporting-set-id"
        }
      )

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
              binaryRepresentation = 1
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdPrimitiveReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = primitiveReportingSet
          externalReportingSetId = "primitive-reporting-set-id"
        }
      )

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
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = compositeReportingSet
          externalReportingSetId = "composite-reporting-set-id"
        }
      )

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
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdPrimitiveReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = primitiveReportingSet
            externalReportingSetId = "primitive-reporting-set-id"
          }
        )

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
        details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
      }

      val createdReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = compositeReportingSet
            externalReportingSetId = "composite-reporting-set-id"
          }
        )

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
                binaryRepresentation = 1
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    this.externalReportingSetId =
                      createdPrimitiveReportingSet.externalReportingSetId
                    filters += primitiveReportingSet.filter
                  }
              }
          },
        )
        .inOrder()
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val createdPrimitiveReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = primitiveReportingSet
          externalReportingSetId = "primitive-reporting-set-id"
        }
      )

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

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = compositeReportingSet
          externalReportingSetId = "composite-reporting-set-id"
        }
      )

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
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
        details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
      }

      val createdReportingSet =
        service.createReportingSet(
          createReportingSetRequest {
            this.reportingSet = primitiveReportingSet
            externalReportingSetId = "primitive-reporting-set-id"
          }
        )

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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet
          externalReportingSetId = "reporting-set-id"
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportingSetIds += createdReportingSet.externalReportingSetId
            externalReportingSetIds += "1L"
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
              externalReportingSetIds += i.toString()
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
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    val reportingSet2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + 2
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1237"
            }
        }
    }

    val createdReportingSet =
      service.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet
          externalReportingSetId = "reporting-set-id"
        }
      )
    service.createReportingSet(
      createReportingSetRequest {
        this.reportingSet = reportingSet2
        externalReportingSetId = "reportingSet2"
      }
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

    assertThat(retrievedReportingSets)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        reportingSet.copy {
          externalReportingSetId = createdReportingSet.externalReportingSetId
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = 1
              binaryRepresentation = 1
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
    val reportingSet1 =
      service.createReportingSet(
        createReportingSetRequest {
          externalReportingSetId = "reporting-set-id-1"
          reportingSet = reportingSet {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            displayName = "displayName"
            filter = "filter"

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "1235"
                    cmmsEventGroupId = "1236"
                  }

                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "2235"
                    cmmsEventGroupId = "2236"
                  }
              }
            details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
          }
        }
      )
    val reportingSet2 =
      service.createReportingSet(
        createReportingSetRequest {
          externalReportingSetId = "reporting-set-id-2"
          reportingSet = reportingSet {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            displayName = "displayName"
            filter = "filter"

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "1235"
                    cmmsEventGroupId = "1236"
                  }

                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "2235"
                    cmmsEventGroupId = "2236"
                  }
              }
            details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
          }
        }
      )

    val retrievedReportingSets =
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportingSetIdAfter = reportingSet1.externalReportingSetId
              }
          }
        )
        .toList()

    assertThat(retrievedReportingSets).containsExactly(reportingSet2)
  }

  @Test
  fun `streamReportingSets filters when campaign group id filter is set`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val campaignGroup =
      service.createReportingSet(
        createReportingSetRequest {
          externalReportingSetId = "my-reporting-set"
          this.reportingSet = reportingSet {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalCampaignGroupId = this@createReportingSetRequest.externalReportingSetId
            displayName = "displayName"
            filter = "filter"

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "1235"
                    cmmsEventGroupId = "1236"
                  }
              }
          }
        }
      )

    service.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = "reporting-set-id-2"
        reportingSet = reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          displayName = "displayName"
          filter = "filter"

          primitive =
            ReportingSetKt.primitive {
              eventGroupKeys +=
                ReportingSetKt.PrimitiveKt.eventGroupKey {
                  cmmsDataProviderId = "1235"
                  cmmsEventGroupId = "1236"
                }
            }
          details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
        }
      }
    )

    val retrievedReportingSets =
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalCampaignGroupId = campaignGroup.externalReportingSetId
              }
          }
        )
        .toList()

    assertThat(retrievedReportingSets).containsExactly(campaignGroup)
  }

  @Test
  fun `streamReportingSets limits the number of results when limit is set`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )
    val reportingSet1 =
      service.createReportingSet(
        createReportingSetRequest {
          externalReportingSetId = "reporting-set-id-1"
          reportingSet = reportingSet {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            displayName = "displayName"
            filter = "filter"

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "1235"
                    cmmsEventGroupId = "1236"
                  }

                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "2235"
                    cmmsEventGroupId = "2236"
                  }
              }
            details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
          }
        }
      )
    service.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = "reporting-set-id-2"
        reportingSet = reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          displayName = "displayName"
          filter = "filter"

          primitive =
            ReportingSetKt.primitive {
              eventGroupKeys +=
                ReportingSetKt.PrimitiveKt.eventGroupKey {
                  cmmsDataProviderId = "1235"
                  cmmsEventGroupId = "1236"
                }

              eventGroupKeys +=
                ReportingSetKt.PrimitiveKt.eventGroupKey {
                  cmmsDataProviderId = "2235"
                  cmmsEventGroupId = "2236"
                }
            }
          details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
        }
      }
    )

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

    assertThat(retrievedReportingSets).containsExactly(reportingSet1)
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

  companion object {
    private val REPORTING_SET_TAGS = mapOf("tag1" to "tag_value1", "tag2" to "tag_value2")
  }
}
