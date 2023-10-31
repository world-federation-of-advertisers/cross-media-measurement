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
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.getMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec

@RunWith(JUnit4::class)
abstract class MetricCalculationSpecsServiceTest<T : MetricCalculationSpecsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val metricCalculationSpecsService: T,
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
    service = services.metricCalculationSpecsService
    measurementConsumersService = services.measurementConsumersService
  }

  @Test
  fun `createMetricCalculationSpec returns a metric calculation spec`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(request)

    assertThat(createdMetricCalculationSpec.externalMetricCalculationSpecId)
      .isEqualTo(request.externalMetricCalculationSpecId)
  }

  @Test
  fun `createMetricCalculationSpec throws ALREADY_EXISTS when same external ID used 2x`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val metricCalculationSpec = createMetricCalculationSpecForRequest()

      val request = createMetricCalculationSpecRequest {
        this.metricCalculationSpec = metricCalculationSpec
        externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
      }
      val createdMetricCalculationSpec = service.createMetricCalculationSpec(request)

      assertThat(createdMetricCalculationSpec.externalMetricCalculationSpecId)
        .isEqualTo(request.externalMetricCalculationSpecId)

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createMetricCalculationSpec(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when missing external ID`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createMetricCalculationSpec(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.message).contains("external_metric_calculation_spec_id")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when no reporting metric entries`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val metricCalculationSpec =
        createMetricCalculationSpecForRequest().copy {
          details = MetricCalculationSpecKt.details { metricSpecs.clear() }
        }

      val request = createMetricCalculationSpecRequest {
        this.metricCalculationSpec = metricCalculationSpec
        externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createMetricCalculationSpec(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("metric_specs")
    }

  @Test
  fun `createMetricCalculationSpec throws FAILED_PRECONDITION when MC not found`() = runBlocking {
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createMetricCalculationSpec(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("Measurement Consumer")
  }

  @Test
  fun `getMetricCalculationSpec returns metric calculation spec`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val createRequest = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(createRequest)

    val retrievedMetricCalculationSpec =
      service.getMetricCalculationSpec(
        getMetricCalculationSpecRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecId =
            createdMetricCalculationSpec.externalMetricCalculationSpecId
        }
      )

    assertThat(retrievedMetricCalculationSpec.externalMetricCalculationSpecId)
      .isEqualTo(createRequest.externalMetricCalculationSpecId)
    assertThat(retrievedMetricCalculationSpec)
      .ignoringFields(MetricCalculationSpec.EXTERNAL_METRIC_CALCULATION_SPEC_ID_FIELD_NUMBER)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(metricCalculationSpec)
  }

  @Test
  fun `getMetricCalculationSpec throws NOT_FOUND when spec not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getMetricCalculationSpec(
          getMetricCalculationSpecRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  @Test
  fun `getMetricCalculationSpec throws INVALID_ARGUMENT when cmms mc id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getMetricCalculationSpec(
          getMetricCalculationSpecRequest {
            externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_i")
  }

  @Test
  fun `getMetricCalculationSpec throws INVALID_ARGUMENT when spec id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getMetricCalculationSpec(
          getMetricCalculationSpecRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("external_metric_calculation_spec_id")
  }

  @Test
  fun `listMetricCalculationSpecs lists 2 specs in asc order by external id`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(request)
    service.createMetricCalculationSpec(
      request.copy { externalMetricCalculationSpecId = "external-metric-calculation-spec-id-2" }
    )

    val retrievedMetricCalculationSpecs =
      service
        .listMetricCalculationSpecs(
          listMetricCalculationSpecsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          }
        )
        .metricCalculationSpecsList

    assertThat(retrievedMetricCalculationSpecs).hasSize(1)
    assertThat(retrievedMetricCalculationSpecs[0].externalMetricCalculationSpecId)
      .isEqualTo(createdMetricCalculationSpec.externalMetricCalculationSpecId)
    assertThat(retrievedMetricCalculationSpecs[0].externalMetricCalculationSpecId)
      .isLessThan(retrievedMetricCalculationSpecs[1].externalMetricCalculationSpecId)
  }

  @Test
  fun `listMetricCalculationSpecs lists 1 spec when limit is specified`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(request)
    service.createMetricCalculationSpec(
      request.copy { externalMetricCalculationSpecId = "external-metric-calculation-spec-id-2" }
    )

    val listResponse =
      service
        .listMetricCalculationSpecs(
          listMetricCalculationSpecsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            limit = 1
          }
        )
    val retrievedMetricCalculationSpecs = listResponse.metricCalculationSpecsList

    assertThat(listResponse.hasMoreThanLimit).isTrue()
    assertThat(retrievedMetricCalculationSpecs).hasSize(1)
    assertThat(retrievedMetricCalculationSpecs[0].externalMetricCalculationSpecId)
      .isEqualTo(createdMetricCalculationSpec.externalMetricCalculationSpecId)
  }

  @Test
  fun `listReportSchedules lists 1 schedule when after id is specified`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(request)
    val createdMetricCalculationSpec2 =
      service.createMetricCalculationSpec(
        request.copy { externalMetricCalculationSpecId = "external-metric-calculation-spec-id-2" }
      )

    val retrievedMetricCalculationSpecs =
      service
        .listMetricCalculationSpecs(
          listMetricCalculationSpecsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricCalculationSpecIdAfter =
              createdMetricCalculationSpec.externalMetricCalculationSpecId
          }
        )
        .metricCalculationSpecsList

    assertThat(retrievedMetricCalculationSpecs).hasSize(1)
    assertThat(retrievedMetricCalculationSpecs[0].externalMetricCalculationSpecId)
      .isEqualTo(createdMetricCalculationSpec2.externalMetricCalculationSpecId)
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when cmms mc id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listMetricCalculationSpecs(listMetricCalculationSpecsRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_id")
  }

  @Test
  fun `batchGetMetricCalculationSpecs lists 1 spec when 1 requested`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(request)

    val retrievedMetricCalculationSpecs =
      service
        .batchGetMetricCalculationSpecs(
          batchGetMetricCalculationSpecsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricCalculationSpecIds +=
              createdMetricCalculationSpec.externalMetricCalculationSpecId
          }
        )
        .metricCalculationSpecsList

    assertThat(retrievedMetricCalculationSpecs).hasSize(1)
    assertThat(retrievedMetricCalculationSpecs[0].externalMetricCalculationSpecId)
      .isEqualTo(createdMetricCalculationSpec.externalMetricCalculationSpecId)
  }

  @Test
  fun `batchGetMetricCalculationSpecs lists 3 specs in request order`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val request = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(request)
    val createdMetricCalculationSpec2 =
      service.createMetricCalculationSpec(
        request.copy { externalMetricCalculationSpecId = "external-metric-calculation-spec-id-2" }
      )

    val retrievedMetricCalculationSpecs =
      service
        .batchGetMetricCalculationSpecs(
          batchGetMetricCalculationSpecsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricCalculationSpecIds +=
              createdMetricCalculationSpec.externalMetricCalculationSpecId
            externalMetricCalculationSpecIds +=
              createdMetricCalculationSpec2.externalMetricCalculationSpecId
            externalMetricCalculationSpecIds +=
              createdMetricCalculationSpec.externalMetricCalculationSpecId
          }
        )
        .metricCalculationSpecsList

    assertThat(retrievedMetricCalculationSpecs).hasSize(3)
    assertThat(retrievedMetricCalculationSpecs[0].externalMetricCalculationSpecId)
      .isEqualTo(createdMetricCalculationSpec.externalMetricCalculationSpecId)
    assertThat(retrievedMetricCalculationSpecs[1].externalMetricCalculationSpecId)
      .isEqualTo(createdMetricCalculationSpec2.externalMetricCalculationSpecId)
    assertThat(retrievedMetricCalculationSpecs[2].externalMetricCalculationSpecId)
      .isEqualTo(createdMetricCalculationSpec.externalMetricCalculationSpecId)
  }

  @Test
  fun `batchGetMetricCalculationSpecs throws INVALID_ARGUMENT when cmms mc id missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchGetMetricCalculationSpecs(batchGetMetricCalculationSpecsRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("cmms_measurement_consumer_id")
    }

  @Test
  fun `batchGetMetricCalculationSpecs throws NOT_FOUND when spec not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val metricCalculationSpec = createMetricCalculationSpecForRequest()

    val createRequest = createMetricCalculationSpecRequest {
      this.metricCalculationSpec = metricCalculationSpec
      externalMetricCalculationSpecId = "external-metric-calculation-spec-id"
    }
    val createdMetricCalculationSpec = service.createMetricCalculationSpec(createRequest)

    val batchGetRequest = batchGetMetricCalculationSpecsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalMetricCalculationSpecIds +=
        createdMetricCalculationSpec.externalMetricCalculationSpecId
      externalMetricCalculationSpecIds += "1234"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetMetricCalculationSpecs(batchGetRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

    private fun createMetricCalculationSpecForRequest(): MetricCalculationSpec {
      return metricCalculationSpec {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        details =
          MetricCalculationSpecKt.details {
            displayName = "display"
            metricSpecs += metricSpec {
              reach =
                MetricSpecKt.reachParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = 0.1f
                  width = 0.5f
                }
            }
            groupings += MetricCalculationSpecKt.grouping { predicates += "age > 10" }
            cumulative = false
          }
      }
    }
  }
}
