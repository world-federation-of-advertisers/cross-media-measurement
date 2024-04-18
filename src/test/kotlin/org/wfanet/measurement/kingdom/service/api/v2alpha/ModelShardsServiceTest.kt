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
import com.google.protobuf.Empty
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
import org.wfanet.measurement.api.v2alpha.ListModelShardsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelShardsRequest
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelShard
import org.wfanet.measurement.api.v2alpha.ModelShardKey
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelShardRequest
import org.wfanet.measurement.api.v2alpha.deleteModelShardRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsPageToken
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelShard
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
import org.wfanet.measurement.internal.kingdom.ModelShard as InternalModelShard
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequest as InternalStreamModelShardsRequest
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequest
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteModelShardRequest as internalDeleteModelShardRequest
import org.wfanet.measurement.internal.kingdom.modelShard as internalModelShard
import org.wfanet.measurement.internal.kingdom.streamModelShardsRequest as internalStreamModelShardsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelShardInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelShardNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException

private const val DEFAULT_LIMIT = 50

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME_2 = "dataProviders/AAAAAAAAAJs"
private const val MODEL_SHARD_NAME = "$DATA_PROVIDER_NAME/modelShards/AAAAAAAAAHs"
private const val MODEL_SHARD_NAME_2 = "$DATA_PROVIDER_NAME/modelShards/AAAAAAAAAJs"
private const val MODEL_SHARD_NAME_3 = "$DATA_PROVIDER_NAME/modelShards/AAAAAAAAAKs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME_2 = "modelProviders/AAAAAAAAAJs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME_2/modelSuites/AAAAAAAAAJs"
private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
private const val MODEL_RELEASE_NAME_2 = "$MODEL_SUITE_NAME_2/modelReleases/AAAAAAAAAJs"
private val EXTERNAL_DATA_PROVIDER_ID =
  apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
private val EXTERNAL_MODEL_SHARD_ID =
  apiIdToExternalId(ModelShardKey.fromName(MODEL_SHARD_NAME)!!.modelShardId)
private val EXTERNAL_MODEL_SHARD_ID_2 =
  apiIdToExternalId(ModelShardKey.fromName(MODEL_SHARD_NAME_2)!!.modelShardId)
private val EXTERNAL_MODEL_SHARD_ID_3 =
  apiIdToExternalId(ModelShardKey.fromName(MODEL_SHARD_NAME_3)!!.modelShardId)
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_PROVIDER_ID_2 =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME_2)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_SUITE_ID_2 =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME_2)!!.modelSuiteId)
private val EXTERNAL_MODEL_RELEASE_ID =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME)!!.modelReleaseId)
private val EXTERNAL_MODEL_RELEASE_ID_2 =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME_2)!!.modelReleaseId)

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val INTERNAL_MODEL_SHARD: InternalModelShard = internalModelShard {
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalModelShardId = EXTERNAL_MODEL_SHARD_ID
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
  modelBlobPath = "ModelBlobPath"
  createTime = CREATE_TIME
}

private val MODEL_SHARD: ModelShard = modelShard {
  name = MODEL_SHARD_NAME
  modelRelease = MODEL_RELEASE_NAME
  modelBlob = modelBlob { modelBlobPath = "ModelBlobPath" }
  createTime = CREATE_TIME
}

private val MODEL_SHARD_2: ModelShard = modelShard {
  name = MODEL_SHARD_NAME
  modelRelease = MODEL_RELEASE_NAME_2
  modelBlob = modelBlob { modelBlobPath = "ModelBlobPath" }
  createTime = CREATE_TIME
}

@RunWith(JUnit4::class)
class ModelShardsServiceTest {

  private val internalModelShardsMock: ModelShardsCoroutineImplBase = mockService {
    onBlocking { createModelShard(any()) }
      .thenAnswer {
        val request = it.getArgument<InternalModelShard>(0)
        if (request.externalDataProviderId != EXTERNAL_DATA_PROVIDER_ID) {
          failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
        } else {
          INTERNAL_MODEL_SHARD
        }
      }
    onBlocking { deleteModelShard(any()) }.thenReturn(INTERNAL_MODEL_SHARD)
    onBlocking { streamModelShards(any()) }
      .thenAnswer {
        val request = it.getArgument<InternalStreamModelShardsRequest>(0)
        if (
          request.hasFilter() &&
            request.filter.externalModelProviderId == EXTERNAL_MODEL_PROVIDER_ID_2
        ) {
          flowOf(
            INTERNAL_MODEL_SHARD.copy {
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID_2
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_2
              externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID_2
            }
          )
        } else {
          flowOf(
            INTERNAL_MODEL_SHARD,
            INTERNAL_MODEL_SHARD.copy { externalModelShardId = EXTERNAL_MODEL_SHARD_ID_2 },
            INTERNAL_MODEL_SHARD.copy { externalModelShardId = EXTERNAL_MODEL_SHARD_ID_3 },
          )
        }
      }
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelShardsMock) }

  private lateinit var service: ModelShardsService

  @Before
  fun initService() {
    service = ModelShardsService(ModelShardsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createModelShard returns model shard`() {
    val request = createModelShardRequest {
      parent = DATA_PROVIDER_NAME
      modelShard = MODEL_SHARD
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelShard(request) }
      }

    val expected = MODEL_SHARD

    verifyProtoArgument(internalModelShardsMock, ModelShardsCoroutineImplBase::createModelShard)
      .isEqualTo(
        INTERNAL_MODEL_SHARD.copy {
          clearCreateTime()
          clearExternalModelShardId()
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createModelShard throws UNAUTHENTICATED when no principal is found`() {
    val request = createModelShardRequest {
      parent = DATA_PROVIDER_NAME
      modelShard = MODEL_SHARD
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createModelShard(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createModelShard throws PERMISSION_DENIED when ModelRelease is owned by a different ModelProvider`() {
    val request = createModelShardRequest {
      parent = DATA_PROVIDER_NAME
      modelShard = MODEL_SHARD_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.createModelShard(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelShard throws PERMISSION_DENIED when principal is data provider`() {
    val request = createModelShardRequest {
      parent = DATA_PROVIDER_NAME
      modelShard = MODEL_SHARD
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createModelShard(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelShard throws PERMISSION_DENIED when principal is duchy`() {
    val request = createModelShardRequest {
      parent = DATA_PROVIDER_NAME
      modelShard = MODEL_SHARD
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.createModelShard(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelShard throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = createModelShardRequest {
      parent = DATA_PROVIDER_NAME
      modelShard = MODEL_SHARD
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createModelShard(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelShard throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelShard(createModelShardRequest { modelShard = MODEL_SHARD })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteModelShard succeeds`() {
    val request = deleteModelShardRequest { name = MODEL_SHARD_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.deleteModelShard(request) }
      }

    val expected = Empty.getDefaultInstance()

    verifyProtoArgument(internalModelShardsMock, ModelShardsCoroutineImplBase::deleteModelShard)
      .isEqualTo(
        internalDeleteModelShardRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalModelShardId = EXTERNAL_MODEL_SHARD_ID
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `deleteModelShard throws UNAUTHENTICATED when no principal is found`() {
    val request = deleteModelShardRequest { name = MODEL_SHARD_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.deleteModelShard(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteModelShard throws PERMISSION_DENIED when principal is data provider`() {
    val request = deleteModelShardRequest { name = MODEL_SHARD_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteModelShard(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelShard throws PERMISSION_DENIED when principal is duchy`() {
    val request = deleteModelShardRequest { name = MODEL_SHARD_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.deleteModelShard(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelShard throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = deleteModelShardRequest { name = MODEL_SHARD_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.deleteModelShard(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelShards with parent uses filter with parent succeeds for model provider caller`() {
    val request = listModelShardsRequest { parent = DATA_PROVIDER_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
        runBlocking { service.listModelShards(request) }
      }

    val expected = listModelShardsResponse { modelShards += MODEL_SHARD_2 }

    val streamModelLinesRequest =
      captureFirst<StreamModelShardsRequest> {
        verify(internalModelShardsMock).streamModelShards(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelShardsRequest {
          limit = DEFAULT_LIMIT + 1
          filter = filter {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID_2
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelShards with parent uses filter with parent succeeds for data provider caller`() {
    val request = listModelShardsRequest { parent = DATA_PROVIDER_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelShards(request) }
      }

    val expected = listModelShardsResponse {
      modelShards += MODEL_SHARD
      modelShards += MODEL_SHARD.copy { name = MODEL_SHARD_NAME_2 }
      modelShards += MODEL_SHARD.copy { name = MODEL_SHARD_NAME_3 }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelShardsRequest> {
        verify(internalModelShardsMock).streamModelShards(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelShardsRequest {
          limit = DEFAULT_LIMIT + 1
          filter = filter { externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelShards throws UNAUTHENTICATED when no principal is found`() {
    val request = listModelShardsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listModelShards(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listModelShards throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelShards(ListModelShardsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelShards throws INVALID_ARGUMENT when page size is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.listModelShards(
              listModelShardsRequest {
                parent = DATA_PROVIDER_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelShards throws invalid argument when parent doesn't match parent in page token`() {
    val request = listModelShardsRequest {
      parent = DATA_PROVIDER_NAME
      val listModelShardsPageToken = listModelShardsPageToken {
        pageSize = 2
        externalDataProviderId = 987L
        lastModelShard = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalModelShardId = EXTERNAL_MODEL_SHARD_ID
        }
      }
      pageToken = listModelShardsPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelShards(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelShards throws invalid argument when ModelProvider doesn't match in page token`() {
    val request = listModelShardsRequest {
      parent = DATA_PROVIDER_NAME
      val listModelShardsPageToken = listModelShardsPageToken {
        pageSize = 2
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        externalModelProviderId = 456L
        lastModelShard = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalModelShardId = EXTERNAL_MODEL_SHARD_ID
        }
      }
      pageToken = listModelShardsPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelShards(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelShards throws PERMISSION_DENIED when principal is duchy`() {
    val request = listModelShardsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.listModelShards(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelShards throws PERMISSION_DENIED DataProvider parent doesn't match principal`() {
    val request = listModelShardsRequest { parent = DATA_PROVIDER_NAME_2 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listModelShards(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelShards throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = listModelShardsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listModelShards(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelShards with page token gets the next page`() {
    val request = listModelShardsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      val listModelShardsPageToken = listModelShardsPageToken {
        pageSize = 2
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastModelShard = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalModelShardId = EXTERNAL_MODEL_SHARD_ID
        }
      }
      pageToken = listModelShardsPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelShards(request) }
      }

    val expected = listModelShardsResponse {
      modelShards += MODEL_SHARD
      modelShards += MODEL_SHARD.copy { name = MODEL_SHARD_NAME_2 }
      val listModelShardsPageToken = listModelShardsPageToken {
        pageSize = request.pageSize
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastModelShard = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalModelShardId = EXTERNAL_MODEL_SHARD_ID_2
        }
      }
      nextPageToken = listModelShardsPageToken.toByteArray().base64UrlEncode()
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelShardsRequest> {
        verify(internalModelShardsMock).streamModelShards(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelShardsRequest {
          limit = request.pageSize + 1
          filter = filter {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalDataProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelShardId = EXTERNAL_MODEL_SUITE_ID
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelShards with new page size replaces page size in page token`() {
    val request = listModelShardsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 4
      val listModelShardsPageToken = listModelShardsPageToken {
        pageSize = 2
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastModelShard = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalModelShardId = EXTERNAL_MODEL_SHARD_ID
        }
      }
      pageToken = listModelShardsPageToken.toByteArray().base64UrlEncode()
    }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listModelShards(request) }
    }

    val streamModelShardsRequest =
      captureFirst<StreamModelShardsRequest> {
        verify(internalModelShardsMock).streamModelShards(capture())
      }

    assertThat(streamModelShardsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelShardsRequest {
          limit = request.pageSize + 1
          filter = filter {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              externalModelShardId = EXTERNAL_MODEL_SHARD_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelShards with no page size uses page size in page token`() {
    val request = listModelShardsRequest {
      parent = DATA_PROVIDER_NAME
      val listModelShardsPageToken = listModelShardsPageToken {
        pageSize = 2
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastModelShard = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalModelShardId = EXTERNAL_MODEL_SHARD_ID
        }
      }
      pageToken = listModelShardsPageToken.toByteArray().base64UrlEncode()
    }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listModelShards(request) }
    }

    val streamModelShardsRequest =
      captureFirst<StreamModelShardsRequest> {
        verify(internalModelShardsMock).streamModelShards(capture())
      }

    assertThat(streamModelShardsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelShardsRequest {
          limit = 3
          filter = filter {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            after = afterFilter {
              createTime = CREATE_TIME
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              externalModelShardId = EXTERNAL_MODEL_SHARD_ID
            }
          }
        }
      )
  }

  @Test
  fun `createModelShard throws NOT_FOUND with data provider name when data provider not found`() {
    internalModelShardsMock.stub {
      onBlocking { createModelShard(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(EXTERNAL_DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelShard(
              createModelShardRequest {
                parent = DATA_PROVIDER_NAME
                modelShard = MODEL_SHARD
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  @Test
  fun `createModelShard throws NOT_FOUND with model suite name when model suite not found`() {
    internalModelShardsMock.stub {
      onBlocking { createModelShard(any()) }
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
            service.createModelShard(
              createModelShardRequest {
                parent = DATA_PROVIDER_NAME
                modelShard = MODEL_SHARD
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelSuite", MODEL_SUITE_NAME)
  }

  @Test
  fun `createModelShard throws NOT_FOUND with model release name when model release not found`() {
    internalModelShardsMock.stub {
      onBlocking { createModelShard(any()) }
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
            service.createModelShard(
              createModelShardRequest {
                parent = DATA_PROVIDER_NAME
                modelShard = MODEL_SHARD
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRelease", MODEL_RELEASE_NAME)
  }

  @Test
  fun `deleteModelShard throws NOT_FOUND with data provider name when data provider not found`() {
    internalModelShardsMock.stub {
      onBlocking { deleteModelShard(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(EXTERNAL_DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.deleteModelShard(deleteModelShardRequest { name = MODEL_SHARD_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  @Test
  fun `deleteModelShard throws NOT_FOUND with model shard name when model shard not found`() {
    internalModelShardsMock.stub {
      onBlocking { deleteModelShard(any()) }
        .thenThrow(
          ModelShardNotFoundException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SHARD_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelShard not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.deleteModelShard(deleteModelShardRequest { name = MODEL_SHARD_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelShard", MODEL_SHARD_NAME)
  }

  @Test
  fun `deleteModelShard throws INVALID_ARGUMENT with model shard and provider name when argument invalid`() {
    internalModelShardsMock.stub {
      onBlocking { deleteModelShard(any()) }
        .thenThrow(
          ModelShardInvalidArgsException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SHARD_ID),
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelShard invalid arguments.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.deleteModelShard(deleteModelShardRequest { name = MODEL_SHARD_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelShard", MODEL_SHARD_NAME)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelProvider", MODEL_PROVIDER_NAME)
  }
}
