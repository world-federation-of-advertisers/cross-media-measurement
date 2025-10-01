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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ListModelReleasesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.ListModelReleasesRequestKt
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.getModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.listModelReleasesPageToken
import org.wfanet.measurement.api.v2alpha.listModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.listModelReleasesResponse
import org.wfanet.measurement.api.v2alpha.modelRelease
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
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelRelease as InternalModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getModelReleaseRequest as internalGetModelReleaseRequest
import org.wfanet.measurement.internal.kingdom.modelRelease as internalModelRelease
import org.wfanet.measurement.internal.kingdom.populationKey
import org.wfanet.measurement.internal.kingdom.streamModelReleasesRequest as internalStreamModelReleasesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException

private const val DEFAULT_LIMIT = 50

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME_2 = "modelProviders/BBBBBBBBBHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME_2/modelSuites/AAAAAAAAAJs"
private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
private const val MODEL_RELEASE_NAME_2 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAJs"
private const val MODEL_RELEASE_NAME_3 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAKs"
private const val MODEL_RELEASE_NAME_4 = "$MODEL_SUITE_NAME_2/modelReleases/AAAAAAAAAHs"
private val POPULATION_NAME = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAHs"

private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_RELEASE_ID =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME)!!.modelReleaseId)
private val EXTERNAL_MODEL_RELEASE_ID_2 =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME_2)!!.modelReleaseId)
private val EXTERNAL_MODEL_RELEASE_ID_3 =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME_3)!!.modelReleaseId)
private val EXTERNAL_DATA_PROVIDER_ID =
  apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
private val EXTERNAL_POPULATION_ID =
  apiIdToExternalId(PopulationKey.fromName(POPULATION_NAME)!!.populationId)

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val INTERNAL_MODEL_RELEASE: InternalModelRelease = internalModelRelease {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
  createTime = CREATE_TIME
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalPopulationId = EXTERNAL_POPULATION_ID
}

private val MODEL_RELEASE: ModelRelease = modelRelease {
  name = MODEL_RELEASE_NAME
  createTime = CREATE_TIME
  population = POPULATION_NAME
}

@RunWith(JUnit4::class)
class ModelReleasesServiceTest {

  private val internalModelLinesMock: ModelReleasesCoroutineImplBase = mockService {
    onBlocking { createModelRelease(any()) }
      .thenAnswer {
        val request = it.getArgument<InternalModelRelease>(0)
        if (request.externalModelSuiteId != EXTERNAL_MODEL_SUITE_ID) {
          failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
        } else {
          INTERNAL_MODEL_RELEASE
        }
      }
    onBlocking { getModelRelease(any()) }.thenReturn(INTERNAL_MODEL_RELEASE)
    onBlocking { streamModelReleases(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_MODEL_RELEASE,
          INTERNAL_MODEL_RELEASE.copy { externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID_2 },
          INTERNAL_MODEL_RELEASE.copy { externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID_3 },
        )
      )
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelLinesMock) }

  private lateinit var service: ModelReleasesService

  @Before
  fun initService() {
    service = ModelReleasesService(ModelReleasesCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createModelRelease returns model release`() {
    val request = createModelReleaseRequest {
      parent = MODEL_SUITE_NAME
      modelRelease = MODEL_RELEASE
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelRelease(request) }
      }

    val expected = MODEL_RELEASE

    verifyProtoArgument(internalModelLinesMock, ModelReleasesCoroutineImplBase::createModelRelease)
      .isEqualTo(
        INTERNAL_MODEL_RELEASE.copy {
          clearCreateTime()
          clearExternalModelReleaseId()
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createModelRelease throws UNAUTHENTICATED when no principal is found`() {
    val request = createModelReleaseRequest {
      parent = MODEL_SUITE_NAME
      modelRelease = MODEL_RELEASE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createModelRelease(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createModelRelease throws PERMISSION_DENIED when principal is data provider`() {
    val request = createModelReleaseRequest {
      parent = MODEL_SUITE_NAME
      modelRelease = MODEL_RELEASE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createModelRelease(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRelease throws PERMISSION_DENIED when principal is duchy`() {
    val request = createModelReleaseRequest {
      parent = MODEL_SUITE_NAME
      modelRelease = MODEL_RELEASE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.createModelRelease(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRelease throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = createModelReleaseRequest {
      parent = MODEL_SUITE_NAME
      modelRelease = MODEL_RELEASE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createModelRelease(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRelease throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = createModelReleaseRequest {
      parent = MODEL_SUITE_NAME
      modelRelease = MODEL_RELEASE
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelRelease(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRelease throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelRelease(createModelReleaseRequest { modelRelease = MODEL_RELEASE })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createModelRelease throws NOT_FOUND if model suite is not found`() {
    val request = createModelReleaseRequest {
      parent = MODEL_SUITE_NAME_2
      modelRelease = MODEL_RELEASE.copy { name = MODEL_RELEASE_NAME_4 }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelRelease(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getModelRelease returns model release when model provider caller is found`() {
    val request = getModelReleaseRequest { name = MODEL_RELEASE_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.getModelRelease(request) }
      }

    val expected = MODEL_RELEASE

    verifyProtoArgument(internalModelLinesMock, ModelReleasesCoroutineImplBase::getModelRelease)
      .isEqualTo(
        internalGetModelReleaseRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getModelRelease returns model release when data provider caller is found`() {
    val request = getModelReleaseRequest { name = MODEL_RELEASE_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.getModelRelease(request) }
      }

    val expected = MODEL_RELEASE

    verifyProtoArgument(internalModelLinesMock, ModelReleasesCoroutineImplBase::getModelRelease)
      .isEqualTo(
        internalGetModelReleaseRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getModelRelease throws PERMISSION_DENIED when principal is duchy`() {
    val request = getModelReleaseRequest { name = MODEL_RELEASE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.getModelRelease(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getModelRelease throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = getModelReleaseRequest { name = MODEL_RELEASE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getModelRelease(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getModelRelease throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = getModelReleaseRequest { name = MODEL_RELEASE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.getModelRelease(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getModelRelease throws UNAUTHENTICATED when no principal is found`() {
    val request = getModelReleaseRequest { name = MODEL_RELEASE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getModelRelease(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getModelRelease throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.getModelRelease(getModelReleaseRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelReleases succeeds for model provider caller`() {
    val request = listModelReleasesRequest { parent = MODEL_SUITE_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listModelReleases(request) }
      }

    val expected = listModelReleasesResponse {
      modelReleases += MODEL_RELEASE
      modelReleases += MODEL_RELEASE.copy { name = MODEL_RELEASE_NAME_2 }
      modelReleases += MODEL_RELEASE.copy { name = MODEL_RELEASE_NAME_3 }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelReleasesRequest> {
        verify(internalModelLinesMock).streamModelReleases(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelReleasesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelReleases succeeds for data provider caller`() {
    val request = listModelReleasesRequest { parent = MODEL_SUITE_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelReleases(request) }
      }

    val expected = listModelReleasesResponse {
      modelReleases += MODEL_RELEASE
      modelReleases += MODEL_RELEASE.copy { name = MODEL_RELEASE_NAME_2 }
      modelReleases += MODEL_RELEASE.copy { name = MODEL_RELEASE_NAME_3 }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelReleasesRequest> {
        verify(internalModelLinesMock).streamModelReleases(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelReleasesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelReleases succeeds when filtering by Population across ModelSuites`() {
    val request = listModelReleasesRequest {
      parent = "$MODEL_PROVIDER_NAME/modelSuites/-"
      filter = ListModelReleasesRequestKt.filter { populationIn += POPULATION_NAME }
    }

    runBlocking {
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) { service.listModelReleases(request) }
    }

    val internalRequest: StreamModelReleasesRequest =
      verifyAndCapture(internalModelLinesMock, ModelReleasesCoroutineImplBase::streamModelReleases)
    assertThat(internalRequest)
      .isEqualTo(
        internalStreamModelReleasesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            populationKeyIn += populationKey {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              externalPopulationId = EXTERNAL_POPULATION_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelReleases throws UNAUTHENTICATED when no principal is found`() {
    val request = listModelReleasesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listModelReleases(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listModelReleases throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = listModelReleasesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.listModelReleases(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelReleases throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelReleases(ListModelReleasesRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelReleases throws INVALID_ARGUMENT when page size is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.listModelReleases(
              listModelReleasesRequest {
                parent = MODEL_SUITE_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelReleases throws invalid argument when parent doesn't match parent in page token`() {
    val request = listModelReleasesRequest {
      parent = MODEL_SUITE_NAME
      val listModelReleasesPageToken = listModelReleasesPageToken {
        pageSize = 2
        externalModelProviderId = 987L
        externalModelSuiteId = 456L
        lastModelRelease = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
        }
      }
      pageToken = listModelReleasesPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelReleases(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelReleases throws PERMISSION_DENIED when principal is duchy`() {
    val request = listModelReleasesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.listModelReleases(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelReleases throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = listModelReleasesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listModelReleases(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelReleases with page token gets the next page`() {
    val request = listModelReleasesRequest {
      parent = MODEL_SUITE_NAME
      pageSize = 2
      val listModelReleasesPageToken = listModelReleasesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        lastModelRelease = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
        }
      }
      pageToken = listModelReleasesPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelReleases(request) }
      }

    val expected = listModelReleasesResponse {
      modelReleases += MODEL_RELEASE
      modelReleases += MODEL_RELEASE.copy { name = MODEL_RELEASE_NAME_2 }
      val listModelLinesPageToken = listModelReleasesPageToken {
        pageSize = request.pageSize
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        lastModelRelease = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID_2
        }
      }
      nextPageToken = listModelLinesPageToken.toByteArray().base64UrlEncode()
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelReleasesRequest> {
        verify(internalModelLinesMock).streamModelReleases(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelReleasesRequest {
          limit = request.pageSize + 1
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelReleases with new page size replaces page size in page token`() {
    val request = listModelReleasesRequest {
      parent = MODEL_SUITE_NAME
      pageSize = 2
      val listModelReleasesPageToken = listModelReleasesPageToken {
        pageSize = 4
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        lastModelRelease = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
        }
      }
      pageToken = listModelReleasesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelReleases(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelReleasesRequest> {
        verify(internalModelLinesMock).streamModelReleases(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelReleasesRequest {
          limit = request.pageSize + 1
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelReleases with no page size uses page size in page token`() {
    val request = listModelReleasesRequest {
      parent = MODEL_SUITE_NAME
      val listModelReleasesPageToken = listModelReleasesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        lastModelRelease = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
        }
      }
      pageToken = listModelReleasesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelReleases(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelReleasesRequest> {
        verify(internalModelLinesMock).streamModelReleases(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelReleasesRequest {
          limit = 3
          filter = filter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
            }
          }
        }
      )
  }

  @Test
  fun `createModelRelease throws NOT_FOUND with model suite name when model suite not found`() {
    internalModelLinesMock.stub {
      onBlocking { createModelRelease(any()) }
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
          runBlocking {
            service.createModelRelease(
              createModelReleaseRequest {
                parent = MODEL_SUITE_NAME
                modelRelease = MODEL_RELEASE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelSuite", MODEL_SUITE_NAME)
  }

  @Test
  fun `getModelRelease throws NOT_FOUND with model release name when model release not found`() {
    internalModelLinesMock.stub {
      onBlocking { getModelRelease(any()) }
        .thenThrow(
          ModelReleaseNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_RELEASE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelRelease not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.getModelRelease(getModelReleaseRequest { name = MODEL_RELEASE_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRelease", MODEL_RELEASE_NAME)
  }
}
