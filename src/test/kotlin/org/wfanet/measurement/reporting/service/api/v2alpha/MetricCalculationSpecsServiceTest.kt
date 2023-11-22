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
import com.google.protobuf.util.Durations
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
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt as InternalMetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricCalculationSpecRequest as internalCreateMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.getMetricCalculationSpecRequest as internalGetMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest as internalListMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsResponse as internalListMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec as internalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.reporting.v2alpha.ListMetricCalculationSpecsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsPageToken
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsResponse
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec

@RunWith(JUnit4::class)
class MetricCalculationSpecsServiceTest {
  private val internalMetricCalculationSpecsMock: MetricCalculationSpecsCoroutineImplBase =
    mockService {
      onBlocking { createMetricCalculationSpec(any()) }.thenReturn(INTERNAL_METRIC_CALCULATION_SPEC)
      onBlocking {
          getMetricCalculationSpec(
            eq(
              internalGetMetricCalculationSpecRequest {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
            )
          )
        }
        .thenReturn(INTERNAL_METRIC_CALCULATION_SPEC)

      onBlocking { listMetricCalculationSpecs(any()) }
        .thenReturn(
          internalListMetricCalculationSpecsResponse {
            metricCalculationSpecs += INTERNAL_METRIC_CALCULATION_SPEC
            metricCalculationSpecs += INTERNAL_METRIC_CALCULATION_SPEC_WITH_GREATER_ID
          }
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(internalMetricCalculationSpecsMock) }

  private lateinit var service: MetricCalculationSpecsService

  @Before
  fun initService() {
    service =
      MetricCalculationSpecsService(
        MetricCalculationSpecsCoroutineStub(grpcTestServerRule.channel),
        METRIC_SPEC_CONFIG,
      )
  }

  @Test
  fun `createMetricCalculationSpec returns metric calculation spec`() = runBlocking {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val createdMetricCalculationSpec =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.createMetricCalculationSpec(request) }
      }

    val expected = METRIC_CALCULATION_SPEC

    assertThat(createdMetricCalculationSpec).isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::createMetricCalculationSpec
      )
      .isEqualTo(
        internalCreateMetricCalculationSpecRequest {
          metricCalculationSpec =
            INTERNAL_METRIC_CALCULATION_SPEC.copy { clearExternalMetricCalculationSpecId() }
          externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when parent name is invalid`() {
    val request = createMetricCalculationSpecRequest {
      parent = "name"
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent")
  }

  @Test
  fun `createMetricCalculationSpec throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME + 1, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains("another MeasurementConsumer")
  }

  @Test
  fun `createMetricCalculationSpec throws UNAUTHENTICATED when the caller is not a MC`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when display name missing`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { clearDisplayName() }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("display_name")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when no metric specs`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC.copy { metricSpecs.clear() }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("metric_spec")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when no predicate in grouping`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          groupings.clear()
          groupings += MetricCalculationSpec.Grouping.getDefaultInstance()
        }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("predicates in Grouping")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when predicates duplicated`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          groupings.clear()
          groupings += MetricCalculationSpecKt.grouping { predicates += "age = 1" }
          groupings += MetricCalculationSpecKt.grouping { predicates += "age = 1" }
        }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("duplicate predicates")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when metric_calculation_spec_id bad`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec = METRIC_CALCULATION_SPEC
      metricCalculationSpecId = "!!!!!!!"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("metric_calculation_spec_id")
  }

  @Test
  fun `createMetricCalculationSpec throws INVALID_ARGUMENT when metric_spec bad`() {
    val request = createMetricCalculationSpecRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      metricCalculationSpec =
        METRIC_CALCULATION_SPEC.copy {
          metricSpecs.clear()
          metricSpecs +=
            REACH_METRIC_SPEC.copy {
              vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 2.0f }
            }
        }
      metricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.createMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("metric_spec")
  }

  @Test
  fun `getMetricCalculationSpec returns metric calculation spec`() = runBlocking {
    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }

    val metricCalculationSpec =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.getMetricCalculationSpec(request) }
      }

    val expected = METRIC_CALCULATION_SPEC

    assertThat(metricCalculationSpec).isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::getMetricCalculationSpec
      )
      .isEqualTo(
        internalGetMetricCalculationSpecRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `getMetricCalculationSpec throws NOT_FOUND when spec not found`() = runBlocking {
    whenever(
        internalMetricCalculationSpecsMock.getMetricCalculationSpec(
          eq(
            internalGetMetricCalculationSpecRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
            }
          )
        )
      )
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.getMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Unable to get")
  }

  @Test
  fun `getMetricCalculationSpec throws INVALID_ARGUMENT when name is invalid`() {
    val invalidMetricCalculationSpecName = "$METRIC_CALCULATION_SPEC_NAME/test/1234"
    val request = getMetricCalculationSpecRequest { name = invalidMetricCalculationSpecName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.getMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("name")
  }

  @Test
  fun `getMetricCalculationSpec throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME + 1, CONFIG) {
          runBlocking { service.getMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains("other MeasurementConsumers")
  }

  @Test
  fun `getMetricCalculationSpec throws UNAUTHENTICATED when the caller is not a MC`() {
    val request = getMetricCalculationSpecRequest { name = METRIC_CALCULATION_SPEC_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.getMetricCalculationSpec(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listMetricCalculationSpecs returns with next page token when there is another page`() {
    runBlocking {
      whenever(internalMetricCalculationSpecsMock.listMetricCalculationSpecs(any()))
        .thenReturn(
          internalListMetricCalculationSpecsResponse {
            metricCalculationSpecs += INTERNAL_METRIC_CALCULATION_SPEC
            limited = true
          }
        )
    }

    val pageSize = 1
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      nextPageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs returns with no next page token when no other page`() {
    val pageSize = 2
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs succeeds when there is a previous page token`() {
    val pageSize = 2
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecIdAfter = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with page size too large replaced by max page size`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = MAX_PAGE_SIZE
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with page size too large replaced by one in prev page token`() {
    val pageSize = MAX_PAGE_SIZE + 1
    val oldPageSize = 5
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = oldPageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecIdAfter = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with no page size replaced by default size`() {
    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = DEFAULT_PAGE_SIZE
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with page size replaces size in page token`() {
    val pageSize = 6
    val oldPageSize = 3
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      pageToken =
        listMetricCalculationSpecsPageToken {
            this.pageSize = oldPageSize
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastMetricCalculationSpec =
              ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricCalculationSpecIdAfter = METRIC_CALCULATION_SPEC_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs with a filter returns filtered results`() {
    val pageSize = 2
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      filter = "name != '$METRIC_CALCULATION_SPEC_NAME'"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    val expected = listMetricCalculationSpecsResponse {
      metricCalculationSpecs += METRIC_CALCULATION_SPEC_WITH_GREATER_ID
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(
        internalMetricCalculationSpecsMock,
        MetricCalculationSpecsCoroutineImplBase::listMetricCalculationSpecs
      )
      .isEqualTo(
        internalListMetricCalculationSpecsRequest {
          limit = pageSize
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        }
      )
  }

  @Test
  fun `listMetricCalculationSpecs throws CANCELLED when cancelled`() = runBlocking {
    whenever(internalMetricCalculationSpecsMock.listMetricCalculationSpecs(any()))
      .thenThrow(StatusRuntimeException(Status.CANCELLED))

    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.CANCELLED)
    assertThat(exception.message).contains("Unable to list")
  }

  @Test
  fun `listMetricCalculationSpecs throws UNAUTHENTICATED when no principal is found`() {
    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listMetricCalculationSpecs(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listMetricCalculationSpecs throws PERMISSION_DENIED when MC caller doesn't match`() {
    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME + 1 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains("other MeasurementConsumers")
  }

  @Test
  fun `listMetricCalculationSpecs throws UNAUTHENTICATED when the caller is not MC`() {
    val request = listMetricCalculationSpecsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listMetricCalculationSpecsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = -1
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("less than 0")
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = listMetricCalculationSpecsRequest { pageSize = 1 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("unspecified")
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when mc id doesn't match page token`() {
    val pageToken =
      listMetricCalculationSpecsPageToken {
          pageSize = 2
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          lastMetricCalculationSpec =
            ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
            }
        }
        .toByteString()
        .base64UrlEncode()

    val request = listMetricCalculationSpecsRequest {
      pageSize = 2
      parent = "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID + 1}"
      this.pageToken = pageToken
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("kept the same")
  }

  @Test
  fun `listMetricCalculationSpecs throws INVALID_ARGUMENT when filter is not valid CEL`() {
    val request = listMetricCalculationSpecsRequest {
      pageSize = 2
      parent = MEASUREMENT_CONSUMER_NAME
      filter = "name >>> 5"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking { service.listMetricCalculationSpecs(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL")
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID"

    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
    private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }

    private const val METRIC_CALCULATION_SPEC_ID = "b123"
    private const val METRIC_CALCULATION_SPEC_NAME =
      "$MEASUREMENT_CONSUMER_NAME/metricCalculationSpecs/$METRIC_CALCULATION_SPEC_ID"

    private const val NUMBER_VID_BUCKETS = 300
    private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
    private const val REACH_ONLY_VID_SAMPLING_START = 0.0f
    private const val REACH_ONLY_REACH_EPSILON = 0.0041

    private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
    private const val REACH_FREQUENCY_VID_SAMPLING_START = 48.0f / NUMBER_VID_BUCKETS
    private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
    private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115
    private const val REACH_FREQUENCY_MAXIMUM_FREQUENCY = 10

    private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
    private const val IMPRESSION_VID_SAMPLING_START = 143.0f / NUMBER_VID_BUCKETS
    private const val IMPRESSION_EPSILON = 0.0011
    private const val IMPRESSION_MAXIMUM_FREQUENCY_PER_USER = 60

    private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
    private const val WATCH_DURATION_VID_SAMPLING_START = 205.0f / NUMBER_VID_BUCKETS
    private const val WATCH_DURATION_EPSILON = 0.001
    private val MAXIMUM_WATCH_DURATION_PER_USER = Durations.fromSeconds(4000)

    private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

    private val REACH_METRIC_SPEC: MetricSpec = metricSpec {
      reach =
        MetricSpecKt.reachParams {
          privacyParams =
            MetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }
    }

    private val METRIC_CALCULATION_SPEC = metricCalculationSpec {
      name = METRIC_CALCULATION_SPEC_NAME
      displayName = "displayName"
      metricSpecs += REACH_METRIC_SPEC
      groupings +=
        listOf(
          MetricCalculationSpecKt.grouping {
            predicates += listOf("gender == MALE", "gender == FEMALE")
          },
          MetricCalculationSpecKt.grouping {
            predicates += listOf("age == 18_34", "age == 55_PLUS")
          }
        )
      cumulative = false
    }

    private val METRIC_CALCULATION_SPEC_WITH_GREATER_ID =
      METRIC_CALCULATION_SPEC.copy { name += "2" }

    private val INTERNAL_REACH_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      reach =
        InternalMetricSpecKt.reachParams {
          privacyParams =
            InternalMetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
      vidSamplingInterval =
        InternalMetricSpecKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }
    }

    private val INTERNAL_METRIC_CALCULATION_SPEC: InternalMetricCalculationSpec =
      internalMetricCalculationSpec {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalMetricCalculationSpecId = METRIC_CALCULATION_SPEC_ID
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = METRIC_CALCULATION_SPEC.displayName
            metricSpecs += INTERNAL_REACH_METRIC_SPEC
            groupings +=
              METRIC_CALCULATION_SPEC.groupingsList.map {
                InternalMetricCalculationSpecKt.grouping { predicates += it.predicatesList }
              }
            cumulative = METRIC_CALCULATION_SPEC.cumulative
          }
      }

    private val INTERNAL_METRIC_CALCULATION_SPEC_WITH_GREATER_ID =
      INTERNAL_METRIC_CALCULATION_SPEC.copy { externalMetricCalculationSpecId += "2" }

    private val METRIC_SPEC_CONFIG = metricSpecConfig {
      reachParams =
        MetricSpecConfigKt.reachParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
      reachVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }

      reachAndFrequencyParams =
        MetricSpecConfigKt.reachAndFrequencyParams {
          reachPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          frequencyPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
        }
      reachAndFrequencyVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = REACH_FREQUENCY_VID_SAMPLING_START
          width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
        }

      impressionCountParams =
        MetricSpecConfigKt.impressionCountParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = IMPRESSION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
        }
      impressionCountVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = IMPRESSION_VID_SAMPLING_START
          width = IMPRESSION_VID_SAMPLING_WIDTH
        }

      watchDurationParams =
        MetricSpecConfigKt.watchDurationParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = WATCH_DURATION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
        }
      watchDurationVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = WATCH_DURATION_VID_SAMPLING_START
          width = WATCH_DURATION_VID_SAMPLING_WIDTH
        }
    }
  }
}
