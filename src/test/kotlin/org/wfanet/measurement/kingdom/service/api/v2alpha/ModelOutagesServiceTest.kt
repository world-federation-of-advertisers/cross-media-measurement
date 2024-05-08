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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.DeleteModelOutageRequest
import org.wfanet.measurement.api.v2alpha.ListModelOutagesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelOutagesRequest
import org.wfanet.measurement.api.v2alpha.ListModelOutagesRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelOutage
import org.wfanet.measurement.api.v2alpha.ModelOutage.State as ModelOutageState
import org.wfanet.measurement.api.v2alpha.ModelOutageKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelOutageRequest
import org.wfanet.measurement.api.v2alpha.deleteModelOutageRequest
import org.wfanet.measurement.api.v2alpha.listModelOutagesPageToken
import org.wfanet.measurement.api.v2alpha.listModelOutagesRequest
import org.wfanet.measurement.api.v2alpha.listModelOutagesResponse
import org.wfanet.measurement.api.v2alpha.modelOutage
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelOutage as InternalModelOutage
import org.wfanet.measurement.internal.kingdom.ModelOutage.State as InternalModelOutageState
import org.wfanet.measurement.internal.kingdom.ModelOutagesGrpcKt.ModelOutagesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelOutagesGrpcKt.ModelOutagesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.filter as internalFilter
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.outageInterval
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteModelOutageRequest as internalDeleteModelOutageRequest
import org.wfanet.measurement.internal.kingdom.modelOutage as internalModelOutage
import org.wfanet.measurement.internal.kingdom.streamModelOutagesRequest as internalStreamModelOutagesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageStateIllegalException

private const val DEFAULT_LIMIT = 50

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME_2 = "modelProviders/AAAAAAAAAJs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME_2/modelSuites/AAAAAAAAAJs"
private const val MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAHs"
private const val MODEL_LINE_NAME_2 = "$MODEL_SUITE_NAME_2/modelLines/AAAAAAAAAJs"
private const val MODEL_OUTAGE_NAME = "$MODEL_LINE_NAME/modelOutages/AAAAAAAAAHs"
private const val MODEL_OUTAGE_NAME_2 = "$MODEL_LINE_NAME/modelOutages/AAAAAAAAAJs"
private const val MODEL_OUTAGE_NAME_3 = "$MODEL_LINE_NAME/modelOutages/AAAAAAAAAKs"
private const val MODEL_OUTAGE_NAME_4 = "$MODEL_LINE_NAME_2/modelOutages/AAAAAAAAAhs"
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_LINE_ID =
  apiIdToExternalId(ModelLineKey.fromName(MODEL_LINE_NAME)!!.modelLineId)
private val EXTERNAL_MODEL_OUTAGE_ID =
  apiIdToExternalId(ModelOutageKey.fromName(MODEL_OUTAGE_NAME)!!.modelOutageId)
private val EXTERNAL_MODEL_OUTAGE_ID_2 =
  apiIdToExternalId(ModelOutageKey.fromName(MODEL_OUTAGE_NAME_2)!!.modelOutageId)
private val EXTERNAL_MODEL_OUTAGE_ID_3 =
  apiIdToExternalId(ModelOutageKey.fromName(MODEL_OUTAGE_NAME_3)!!.modelOutageId)

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val DELETE_TIME: Timestamp = Instant.ofEpochSecond(912).toProtoTime()
private val OUTAGE_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
private val OUTAGE_END_TIME: Timestamp = Instant.ofEpochSecond(789).toProtoTime()

private val INTERNAL_MODEL_OUTAGE: InternalModelOutage = internalModelOutage {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelLineId = EXTERNAL_MODEL_LINE_ID
  externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
  modelOutageStartTime = OUTAGE_START_TIME
  modelOutageEndTime = OUTAGE_END_TIME
  state = InternalModelOutageState.ACTIVE
  createTime = CREATE_TIME
  deleteTime = DELETE_TIME
}

private val MODEL_OUTAGE: ModelOutage = modelOutage {
  name = MODEL_OUTAGE_NAME
  outageInterval = interval {
    startTime = OUTAGE_START_TIME
    endTime = OUTAGE_END_TIME
  }
  state = ModelOutageState.ACTIVE
  createTime = CREATE_TIME
  deleteTime = DELETE_TIME
}

@RunWith(JUnit4::class)
class ModelOutagesServiceTest {

  private val internalModelOutagesMock: ModelOutagesCoroutineImplBase =
    mockService() {
      onBlocking { createModelOutage(any()) }
        .thenAnswer {
          val request = it.getArgument<InternalModelOutage>(0)
          if (request.externalModelLineId != EXTERNAL_MODEL_LINE_ID) {
            failGrpc(Status.NOT_FOUND) { "ModelLine not found" }
          } else {
            INTERNAL_MODEL_OUTAGE
          }
        }
      onBlocking { deleteModelOutage(any()) }
        .thenReturn(INTERNAL_MODEL_OUTAGE.copy { state = InternalModelOutageState.DELETED })
      onBlocking { streamModelOutages(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MODEL_OUTAGE,
            INTERNAL_MODEL_OUTAGE.copy { externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID_2 },
            INTERNAL_MODEL_OUTAGE.copy { externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID_3 },
          )
        )
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelOutagesMock) }

  private lateinit var service: ModelOutagesService

  @Before
  fun initService() {
    service = ModelOutagesService(ModelOutagesCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createModelOutage returns model outage`() {
    val request = createModelOutageRequest {
      parent = MODEL_LINE_NAME
      modelOutage = MODEL_OUTAGE
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelOutage(request) }
      }

    val expected = MODEL_OUTAGE

    verifyProtoArgument(internalModelOutagesMock, ModelOutagesCoroutineImplBase::createModelOutage)
      .isEqualTo(
        INTERNAL_MODEL_OUTAGE.copy {
          clearCreateTime()
          clearDeleteTime()
          clearExternalModelOutageId()
          clearState()
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createModelOutage throws UNAUTHENTICATED when no principal is found`() {
    val request = createModelOutageRequest {
      parent = MODEL_LINE_NAME
      modelOutage = MODEL_OUTAGE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createModelOutage(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createModelOutage throws PERMISSION_DENIED when principal is data provider`() {
    val request = createModelOutageRequest {
      parent = MODEL_LINE_NAME
      modelOutage = MODEL_OUTAGE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createModelOutage(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelOutage throws PERMISSION_DENIED when principal is duchy`() {
    val request = createModelOutageRequest {
      parent = MODEL_LINE_NAME
      modelOutage = MODEL_OUTAGE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.createModelOutage(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelOutage throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = createModelOutageRequest {
      parent = MODEL_LINE_NAME
      modelOutage = MODEL_OUTAGE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createModelOutage(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelOutage throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelOutage(createModelOutageRequest { modelOutage = MODEL_OUTAGE })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createModelOutage throws PERMISSION_DENIED when principal doesn't match`() {
    val request = createModelOutageRequest {
      parent = MODEL_LINE_NAME
      modelOutage = MODEL_OUTAGE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelOutage(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelOutage throws NOT_FOUND if model line is not found`() {
    val request = createModelOutageRequest {
      parent = MODEL_LINE_NAME_2
      modelOutage = MODEL_OUTAGE.copy { name = MODEL_OUTAGE_NAME_4 }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelOutage(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `listModelOutages with filters succeeds for model provider caller`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      showDeleted = true
      filter = filter {
        outageIntervalOverlapping = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
      }
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listModelOutages(request) }
      }

    val expected = listModelOutagesResponse {
      modelOutages += MODEL_OUTAGE
      modelOutages += MODEL_OUTAGE.copy { name = MODEL_OUTAGE_NAME_2 }
      modelOutages += MODEL_OUTAGE.copy { name = MODEL_OUTAGE_NAME_3 }
    }

    val streamModelOutagesRequest =
      captureFirst<StreamModelOutagesRequest> {
        verify(internalModelOutagesMock).streamModelOutages(capture())
      }

    assertThat(streamModelOutagesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelOutagesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            outageInterval = outageInterval {
              modelOutageStartTime = OUTAGE_START_TIME
              modelOutageEndTime = OUTAGE_END_TIME
            }
            showDeleted = true
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelOutages with filters succeeds for data provider caller`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      showDeleted = true
      filter = filter {
        outageIntervalOverlapping = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelOutages(request) }
      }

    val expected = listModelOutagesResponse {
      modelOutages += MODEL_OUTAGE
      modelOutages += MODEL_OUTAGE.copy { name = MODEL_OUTAGE_NAME_2 }
      modelOutages += MODEL_OUTAGE.copy { name = MODEL_OUTAGE_NAME_3 }
    }

    val streamModelOutagesRequest =
      captureFirst<StreamModelOutagesRequest> {
        verify(internalModelOutagesMock).streamModelOutages(capture())
      }

    assertThat(streamModelOutagesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelOutagesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            outageInterval = outageInterval {
              modelOutageStartTime = OUTAGE_START_TIME
              modelOutageEndTime = OUTAGE_END_TIME
            }
            showDeleted = true
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelOutages throws UNAUTHENTICATED when no principal is found`() {
    val request = listModelOutagesRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listModelOutages(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listModelOutages throws PERMISSION_DENIED when principal doesn't match`() {
    val request = listModelOutagesRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.listModelOutages(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelOutages throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelOutages(ListModelOutagesRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelOutages throws PERMISSION_DENIED when principal is duchy`() {
    val request = listModelOutagesRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.listModelOutages(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelOutages throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = listModelOutagesRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listModelOutages(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelOutages throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      pageSize = -1
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelOutages(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelOutages throws invalid argument when parent doesn't match parent in page token`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      val listModelOutagesPageToken = listModelOutagesPageToken {
        pageSize = 2
        externalModelProviderId = 987L
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        outageInterval = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
        showDeleted = false
      }
      pageToken = listModelOutagesPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelOutages(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelOutages throws invalid argument when showDeleted doesn't match showDeleted in page token`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      val listModelOutagesPageToken = listModelOutagesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        outageInterval = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
        showDeleted = true
      }
      pageToken = listModelOutagesPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelOutages(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelOutages throws invalid argument when outageInterval doesn't match outageInterval in page token`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      val listModelOutagesPageToken = listModelOutagesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        outageInterval = interval {
          startTime = Instant.ofEpochSecond(123).toProtoTime()
          endTime = OUTAGE_END_TIME
        }
        showDeleted = true
      }
      pageToken = listModelOutagesPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelOutages(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelOutages with page token gets the next page`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      pageSize = 2
      showDeleted = true
      filter = filter {
        outageIntervalOverlapping = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
      }
      val listModelOutagesPageToken = listModelOutagesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        outageInterval = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
        showDeleted = true
        lastModelOutage = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
        }
      }
      pageToken = listModelOutagesPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelOutages(request) }
      }

    val expected = listModelOutagesResponse {
      modelOutages += MODEL_OUTAGE
      modelOutages += MODEL_OUTAGE.copy { name = MODEL_OUTAGE_NAME_2 }
      val listModelRolloutsPageToken = listModelOutagesPageToken {
        pageSize = request.pageSize
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        outageInterval = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
        showDeleted = true
        lastModelOutage = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID_2
        }
      }
      nextPageToken = listModelRolloutsPageToken.toByteArray().base64UrlEncode()
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelOutagesRequest> {
        verify(internalModelOutagesMock).streamModelOutages(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelOutagesRequest {
          limit = request.pageSize + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            outageInterval = outageInterval {
              modelOutageStartTime = OUTAGE_START_TIME
              modelOutageEndTime = OUTAGE_END_TIME
            }
            showDeleted = true
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelOutages with new page size replaces page size in page token`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      pageSize = 4
      showDeleted = false
      filter = filter {
        outageIntervalOverlapping = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
      }
      val listModelOutagesPageToken = listModelOutagesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        outageInterval = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
        showDeleted = false
        lastModelOutage = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
        }
      }
      pageToken = listModelOutagesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelOutages(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelOutagesRequest> {
        verify(internalModelOutagesMock).streamModelOutages(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelOutagesRequest {
          limit = request.pageSize + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            outageInterval = outageInterval {
              modelOutageStartTime = OUTAGE_START_TIME
              modelOutageEndTime = OUTAGE_END_TIME
            }
            showDeleted = false
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelOutages with no page size uses page size in page token`() {
    val request = listModelOutagesRequest {
      parent = MODEL_LINE_NAME
      showDeleted = false
      filter = filter {
        outageIntervalOverlapping = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
      }
      val listModelOutagesPageToken = listModelOutagesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        outageInterval = interval {
          startTime = OUTAGE_START_TIME
          endTime = OUTAGE_END_TIME
        }
        showDeleted = false
        lastModelOutage = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
        }
      }
      pageToken = listModelOutagesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelOutages(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelOutagesRequest> {
        verify(internalModelOutagesMock).streamModelOutages(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelOutagesRequest {
          limit = 3
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            outageInterval = outageInterval {
              modelOutageStartTime = OUTAGE_START_TIME
              modelOutageEndTime = OUTAGE_END_TIME
            }
            showDeleted = false
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
            }
          }
        }
      )
  }

  @Test
  fun `deleteModelOutage returns model outage with state updated`() {
    val request = deleteModelOutageRequest { name = MODEL_OUTAGE_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.deleteModelOutage(request) }
      }

    val expected = MODEL_OUTAGE.copy { state = ModelOutageState.DELETED }

    verifyProtoArgument(internalModelOutagesMock, ModelOutagesCoroutineImplBase::deleteModelOutage)
      .isEqualTo(
        internalDeleteModelOutageRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelOutageId = EXTERNAL_MODEL_OUTAGE_ID
        }
      )
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `deleteModelOutage throws UNAUTHENTICATED when no principal is found`() {
    val request = deleteModelOutageRequest { name = MODEL_OUTAGE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.deleteModelOutage(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteModelOutage throws PERMISSION_DENIED when principal doesn't match`() {
    val request = deleteModelOutageRequest { name = MODEL_OUTAGE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.deleteModelOutage(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelOutage throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.deleteModelOutage(DeleteModelOutageRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteModelOutage throws PERMISSION_DENIED when principal is duchy`() {
    val request = deleteModelOutageRequest { name = MODEL_OUTAGE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.deleteModelOutage(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelOutage throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = deleteModelOutageRequest { name = MODEL_OUTAGE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.deleteModelOutage(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelOutage throws PERMISSION_DENIED when principal is data provider`() {
    val request = deleteModelOutageRequest { name = MODEL_OUTAGE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteModelOutage(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelOutage throws NOT_FOUND with model line name when model line not found`() {
    internalModelOutagesMock.stub {
      onBlocking { createModelOutage(any()) }
        .thenThrow(
          ModelLineNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelLine not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelOutage(
              createModelOutageRequest {
                parent = MODEL_LINE_NAME
                modelOutage = MODEL_OUTAGE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLine", MODEL_LINE_NAME)
  }

  @Test
  fun `createModelOutage throwns INVALID_ARGUMENT with model outage name when argument invalid`() {
    internalModelOutagesMock.stub {
      onBlocking { createModelOutage(any()) }
        .thenThrow(
          ModelOutageInvalidArgsException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_OUTAGE_ID),
            )
            .asStatusRuntimeException(
              Status.Code.INVALID_ARGUMENT,
              "ModelOutage invalid arguments.",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelOutage(
              createModelOutageRequest {
                parent = MODEL_LINE_NAME
                modelOutage = MODEL_OUTAGE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelOutage", MODEL_OUTAGE_NAME)
  }

  @Test
  fun `deleteModelOutage throws NOT_FOUND with model outage name when model outage not found`() {
    internalModelOutagesMock.stub {
      onBlocking { deleteModelOutage(any()) }
        .thenThrow(
          ModelOutageNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_OUTAGE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelOutage not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.deleteModelOutage(deleteModelOutageRequest { name = MODEL_OUTAGE_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelOutage", MODEL_OUTAGE_NAME)
  }

  @Test
  fun `deleteModelOutage throws NOT_FOUND with model outage name when model outage was deleted`() {
    internalModelOutagesMock.stub {
      onBlocking { deleteModelOutage(any()) }
        .thenThrow(
          ModelOutageStateIllegalException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_OUTAGE_ID),
              InternalModelOutageState.DELETED,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelOutage not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.deleteModelOutage(deleteModelOutageRequest { name = MODEL_OUTAGE_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelOutage", MODEL_OUTAGE_NAME)
  }
}
