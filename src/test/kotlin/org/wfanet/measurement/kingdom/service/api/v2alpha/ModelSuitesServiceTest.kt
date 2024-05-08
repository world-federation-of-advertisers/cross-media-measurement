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
import org.wfanet.measurement.api.v2alpha.ListModelSuitesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.getModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesPageToken
import org.wfanet.measurement.api.v2alpha.listModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.modelSuite
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
import org.wfanet.measurement.internal.kingdom.ModelSuite as InternalModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getModelSuiteRequest as internalGetModelSuiteRequest
import org.wfanet.measurement.internal.kingdom.modelSuite as internalModelSuite
import org.wfanet.measurement.internal.kingdom.streamModelSuitesRequest as internalStreamModelSuitesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException

private const val DEFAULT_LIMIT = 50

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME_2 = "modelProviders/BBBBBBBBBHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAJs"
private const val MODEL_SUITE_NAME_3 = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAKs"
private const val MODEL_SUITE_NAME_4 = "$MODEL_PROVIDER_NAME_2/modelSuites/AAAAAAAAAHs"
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_SUITE_ID_2 =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME_2)!!.modelSuiteId)
private val EXTERNAL_MODEL_SUITE_ID_3 =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME_3)!!.modelSuiteId)
private const val DISPLAY_NAME = "Display name"
private const val DESCRIPTION = "Description"
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val INTERNAL_MODEL_SUITE: InternalModelSuite = internalModelSuite {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  createTime = CREATE_TIME
}

private val MODEL_SUITE: ModelSuite = modelSuite {
  name = MODEL_SUITE_NAME
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  createTime = CREATE_TIME
}

@RunWith(JUnit4::class)
class ModelSuitesServiceTest {

  private val internalModelSuitesMock: ModelSuitesCoroutineImplBase =
    mockService() {
      onBlocking { createModelSuite(any()) }
        .thenAnswer {
          val request = it.getArgument<InternalModelSuite>(0)
          if (request.externalModelProviderId != 123L) {
            failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
          } else {
            INTERNAL_MODEL_SUITE
          }
        }
      onBlocking { getModelSuite(any()) }.thenReturn(INTERNAL_MODEL_SUITE)
      onBlocking { streamModelSuites(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MODEL_SUITE,
            INTERNAL_MODEL_SUITE.copy { externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_2 },
            INTERNAL_MODEL_SUITE.copy { externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_3 },
          )
        )
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelSuitesMock) }

  private lateinit var service: ModelSuitesService

  @Before
  fun initService() {
    service =
      ModelSuitesService(ModelSuitesGrpcKt.ModelSuitesCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createModelSuite returns model suite`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME
      modelSuite = MODEL_SUITE
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelSuite(request) }
      }

    val expected = MODEL_SUITE

    verifyProtoArgument(internalModelSuitesMock, ModelSuitesCoroutineImplBase::createModelSuite)
      .isEqualTo(
        INTERNAL_MODEL_SUITE.copy {
          clearCreateTime()
          clearExternalModelSuiteId()
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createModelSuite throws UNAUTHENTICATED when no principal is found`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME
      modelSuite = MODEL_SUITE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createModelSuite(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createModelSuite throws PERMISSION_DENIED when principal is data provider`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME
      modelSuite = MODEL_SUITE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createModelSuite(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelSuite throws PERMISSION_DENIED when principal is duchy`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME
      modelSuite = MODEL_SUITE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.createModelSuite(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelSuite throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME
      modelSuite = MODEL_SUITE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createModelSuite(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelSuite throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME
      modelSuite = MODEL_SUITE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelSuite(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelSuite throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelSuite(createModelSuiteRequest { modelSuite = MODEL_SUITE })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createModelSuite throws NOT_FOUND if model provider is not found`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME_2
      modelSuite = modelSuite {
        name = MODEL_SUITE_NAME_4
        displayName = DISPLAY_NAME
        description = DESCRIPTION
        createTime = CREATE_TIME
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelSuite(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getModelSuite returns model suite when model provider caller is found`() {
    val request = getModelSuiteRequest { name = MODEL_SUITE_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.getModelSuite(request) }
      }

    val expected = MODEL_SUITE

    verifyProtoArgument(internalModelSuitesMock, ModelSuitesCoroutineImplBase::getModelSuite)
      .isEqualTo(
        internalGetModelSuiteRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getModelSuite returns model suite when data provider caller is found`() {
    val request = getModelSuiteRequest { name = MODEL_SUITE_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.getModelSuite(request) }
      }

    val expected = MODEL_SUITE

    verifyProtoArgument(internalModelSuitesMock, ModelSuitesCoroutineImplBase::getModelSuite)
      .isEqualTo(
        internalGetModelSuiteRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getModelSuite throws PERMISSION_DENIED when principal is duchy`() {
    val request = getModelSuiteRequest { name = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.getModelSuite(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getModelSuite throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = getModelSuiteRequest { name = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getModelSuite(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getModelSuite throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = getModelSuiteRequest { name = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.getModelSuite(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getModelSuite throws UNAUTHENTICATED when no principal is found`() {
    val request = getModelSuiteRequest { name = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getModelSuite(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getModelSuite throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.getModelSuite(getModelSuiteRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelSuites with parent uses filter with parent succeeds for model provider caller`() {
    val request = listModelSuitesRequest { parent = MODEL_PROVIDER_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listModelSuites(request) }
      }

    val expected = listModelSuitesResponse {
      modelSuites += MODEL_SUITE
      modelSuites += MODEL_SUITE.copy { name = MODEL_SUITE_NAME_2 }
      modelSuites += MODEL_SUITE.copy { name = MODEL_SUITE_NAME_3 }
    }

    val streamModelSuitesRequest =
      captureFirst<StreamModelSuitesRequest> {
        verify(internalModelSuitesMock).streamModelSuites(capture())
      }

    assertThat(streamModelSuitesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelSuitesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = filter { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelSuites with parent uses filter with parent succeeds for data provider caller`() {
    val request = listModelSuitesRequest { parent = MODEL_PROVIDER_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelSuites(request) }
      }

    val expected = listModelSuitesResponse {
      modelSuites += MODEL_SUITE
      modelSuites += MODEL_SUITE.copy { name = MODEL_SUITE_NAME_2 }
      modelSuites += MODEL_SUITE.copy { name = MODEL_SUITE_NAME_3 }
    }

    val streamModelSuitesRequest =
      captureFirst<StreamModelSuitesRequest> {
        verify(internalModelSuitesMock).streamModelSuites(capture())
      }

    assertThat(streamModelSuitesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelSuitesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = filter { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelSuites with page token gets the next page`() {
    val request = listModelSuitesRequest {
      parent = MODEL_PROVIDER_NAME
      pageSize = 2
      val listModelSuitesPageToken = listModelSuitesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        lastModelSuite = previousPageEnd {
          createTime = CREATE_TIME
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        }
      }
      pageToken = listModelSuitesPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelSuites(request) }
      }

    val expected = listModelSuitesResponse {
      modelSuites += MODEL_SUITE
      modelSuites += MODEL_SUITE.copy { name = MODEL_SUITE_NAME_2 }
      val listModelSuitesPageToken = listModelSuitesPageToken {
        pageSize = request.pageSize
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        lastModelSuite = previousPageEnd {
          createTime = CREATE_TIME
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_2
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        }
      }
      nextPageToken = listModelSuitesPageToken.toByteArray().base64UrlEncode()
    }

    val streamModelSuitesRequest =
      captureFirst<StreamModelSuitesRequest> {
        verify(internalModelSuitesMock).streamModelSuites(capture())
      }

    assertThat(streamModelSuitesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelSuitesRequest {
          limit = request.pageSize + 1
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelSuites with new page size replaces page size in page token`() {
    val request = listModelSuitesRequest {
      parent = MODEL_PROVIDER_NAME
      pageSize = 4
      val listModelSuitesPageToken = listModelSuitesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        lastModelSuite = previousPageEnd {
          createTime = CREATE_TIME
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        }
      }
      pageToken = listModelSuitesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelSuites(request) }
    }

    val streamModelSuitesRequest =
      captureFirst<StreamModelSuitesRequest> {
        verify(internalModelSuitesMock).streamModelSuites(capture())
      }

    assertThat(streamModelSuitesRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        internalStreamModelSuitesRequest {
          limit = request.pageSize + 1
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelSuites with no page size uses page size in page token`() {
    val request = listModelSuitesRequest {
      parent = MODEL_PROVIDER_NAME
      val listModelSuitesPageToken = listModelSuitesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        lastModelSuite = previousPageEnd {
          createTime = CREATE_TIME
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        }
      }
      pageToken = listModelSuitesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelSuites(request) }
    }

    val streamModelSuitesRequest =
      captureFirst<StreamModelSuitesRequest> {
        verify(internalModelSuitesMock).streamModelSuites(capture())
      }

    assertThat(streamModelSuitesRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        internalStreamModelSuitesRequest {
          limit = 3
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelSuites throws UNAUTHENTICATED when no principal is found`() {
    val request = listModelSuitesRequest { parent = MODEL_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listModelSuites(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listModelSuites throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = listModelSuitesRequest { parent = MODEL_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.listModelSuites(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelSuites throws PERMISSION_DENIED when principal is duchy`() {
    val request = listModelSuitesRequest { parent = MODEL_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.listModelSuites(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelSuites throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = listModelSuitesRequest { parent = MODEL_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listModelSuites(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelSuites throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelSuites(ListModelSuitesRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelSuites throws INVALID_ARGUMENT when page size is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.listModelSuites(
              listModelSuitesRequest {
                parent = MODEL_PROVIDER_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelSuites throws invalid argument when parent doesn't match parent in page token`() {
    val request = listModelSuitesRequest {
      parent = MODEL_PROVIDER_NAME
      val listModelSuitesPageToken = listModelSuitesPageToken {
        pageSize = 2
        externalModelProviderId = 987
        lastModelSuite = previousPageEnd {
          createTime = CREATE_TIME
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        }
      }
      pageToken = listModelSuitesPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelSuites(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createModelSuite throws NOT_FOUND with model provider name when model provider not found`() {
    internalModelSuitesMock.stub {
      onBlocking { createModelSuite(any()) }
        .thenThrow(
          ModelProviderNotFoundException(ExternalId(EXTERNAL_MODEL_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelSuite(
              createModelSuiteRequest {
                parent = MODEL_PROVIDER_NAME
                modelSuite = MODEL_SUITE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelProvider", MODEL_PROVIDER_NAME)
  }

  @Test
  fun `getModelSuite throws NOT_FOUND with model suite name when model suite not found`() {
    internalModelSuitesMock.stub {
      onBlocking { getModelSuite(any()) }
        .thenThrow(
          ModelSuiteNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelSuite not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.getModelSuite(getModelSuiteRequest { name = MODEL_SUITE_NAME }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelSuite", MODEL_SUITE_NAME)
  }
}
