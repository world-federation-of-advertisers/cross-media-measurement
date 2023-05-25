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

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateModelShardRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteModelShardRequest
import org.wfanet.measurement.api.v2alpha.ListModelShardsPageToken
import org.wfanet.measurement.api.v2alpha.ListModelShardsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelShardsRequest
import org.wfanet.measurement.api.v2alpha.ListModelShardsResponse
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelShard
import org.wfanet.measurement.api.v2alpha.ModelShardKey
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineImplBase as ModelShardsCoroutineService
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listModelShardsPageToken
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ModelShard as InternalModelShard
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequest
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.deleteModelShardRequest
import org.wfanet.measurement.internal.kingdom.streamModelShardsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelShardsService(private val internalClient: ModelShardsCoroutineStub) :
  ModelShardsCoroutineService() {

  override suspend fun createModelShard(request: CreateModelShardRequest): ModelShard {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val modelReleaseKey =
      grpcRequireNotNull(ModelReleaseKey.fromName(request.modelShard.modelRelease)) {
        "ModelRelease is either unspecified or invalid"
      }

    when (principalFromCurrentContext) {
      is ModelProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to create ModelShard"
        }
      }
    }

    val createModelShardRequest = request.modelShard.toInternal(parentKey, modelReleaseKey)
    return try {
      internalClient.createModelShard(createModelShardRequest).toModelShard()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "ModelShard not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) {
            ex.message ?: "Required field unspecified or invalid"
          }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

  override suspend fun deleteModelShard(request: DeleteModelShardRequest): Empty {
    val key =
      grpcRequireNotNull(ModelShardKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (principalFromCurrentContext) {
      is ModelProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to delete ModelShard"
        }
      }
    }

    val deleteModelShardRequest = deleteModelShardRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      externalModelShardId = apiIdToExternalId(key.modelShardId)
    }
    try {
      internalClient.deleteModelShard(deleteModelShardRequest)
      return Empty.getDefaultInstance()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "ModelShard not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) {
            ex.message ?: "Required field unspecified or invalid"
          }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

  override suspend fun listModelShards(request: ListModelShardsRequest): ListModelShardsResponse {
    grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid"
    }

    val listModelShardsPageToken = request.toListModelShardsPageToken()

    when (principalFromCurrentContext) {
      is ModelProviderPrincipal -> {}
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list ModelShards" }
      }
    }

    val results: List<InternalModelShard> =
      internalClient
        .streamModelShards(listModelShardsPageToken.toStreamModelShardsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListModelShardsResponse.getDefaultInstance()
    }

    return listModelShardsResponse {
      modelShard +=
        results.subList(0, min(results.size, listModelShardsPageToken.pageSize)).map {
          internalModelShard ->
          internalModelShard.toModelShard()
        }
      if (results.size > listModelShardsPageToken.pageSize) {
        val pageToken =
          listModelShardsPageToken.copy {
            lastModelShard = previousPageEnd {
              createTime = results[results.lastIndex - 1].createTime
              externalDataProviderId = results[results.lastIndex - 1].externalDataProviderId
              externalModelShardId = results[results.lastIndex - 1].externalModelShardId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  /** Converts a public [ListModelShardsRequest] to an internal [ListModelShardsPageToken]. */
  private fun ListModelShardsRequest.toListModelShardsPageToken(): ListModelShardsPageToken {
    val source = this

    val key =
      grpcRequireNotNull(DataProviderKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalDataProviderId = apiIdToExternalId(key.dataProviderId)

    return if (source.pageToken.isNotBlank()) {
      ListModelShardsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalDataProviderId == externalDataProviderId) {
          "Arguments must be kept the same when using a page token"
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listModelShardsPageToken {
        pageSize =
          when {
            source.pageSize == 0 -> DEFAULT_PAGE_SIZE
            source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
            else -> source.pageSize
          }
        this.externalDataProviderId = externalDataProviderId
      }
    }
  }

  /** Converts an internal [ListModelShardsPageToken] to an internal [StreamModelShardsRequest]. */
  private fun ListModelShardsPageToken.toStreamModelShardsRequest(): StreamModelShardsRequest {
    val source = this
    return streamModelShardsRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalDataProviderId = source.externalDataProviderId
        if (source.hasLastModelShard()) {
          after = afterFilter {
            createTime = source.lastModelShard.createTime
            externalDataProviderId = source.lastModelShard.externalDataProviderId
            externalModelShardId = source.lastModelShard.externalModelShardId
          }
        }
      }
    }
  }
}
