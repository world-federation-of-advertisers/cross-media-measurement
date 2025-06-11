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

import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.GetModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.ListModelSuitesPageToken
import org.wfanet.measurement.api.v2alpha.ListModelSuitesPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.ListModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase as ModelSuitesCoroutineService
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listModelSuitesPageToken
import org.wfanet.measurement.api.v2alpha.listModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ModelSuite as InternalModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.getModelSuiteRequest
import org.wfanet.measurement.internal.kingdom.streamModelSuitesRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelSuitesService(
  private val internalClient: ModelSuitesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelSuitesCoroutineService(coroutineContext) {

  override suspend fun createModelSuite(request: CreateModelSuiteRequest): ModelSuite {
    val parentKey =
      grpcRequireNotNull(ModelProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.toName() != request.parent) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create ModelSuite for another ModelProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to create ModelSuite"
        }
      }
    }

    val createModelSuiteRequest = request.modelSuite.toInternal(parentKey)
    return try {
      internalClient.createModelSuite(createModelSuiteRequest).toModelSuite()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun getModelSuite(request: GetModelSuiteRequest): ModelSuite {
    val key =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot get ModelSuite from another ModelProvider" }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to get ModelSuite" }
      }
    }

    val getModelSuiteRequest = getModelSuiteRequest {
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
    }

    try {
      return internalClient.getModelSuite(getModelSuiteRequest).toModelSuite()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listModelSuites(request: ListModelSuitesRequest): ListModelSuitesResponse {
    grpcRequireNotNull(ModelProviderKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid"
    }

    val listModelSuitesPageToken = request.toListModelSuitesPageToken()

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.toName() != request.parent) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ModelSuites from another ModelProvider"
          }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to get ModelSuite" }
      }
    }

    val results: List<InternalModelSuite> =
      try {
        internalClient
          .streamModelSuites(listModelSuitesPageToken.toStreamModelSuitesRequest())
          .toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    if (results.isEmpty()) {
      return ListModelSuitesResponse.getDefaultInstance()
    }

    return listModelSuitesResponse {
      modelSuites +=
        results.subList(0, min(results.size, listModelSuitesPageToken.pageSize)).map {
          internalModelSuite ->
          internalModelSuite.toModelSuite()
        }
      if (results.size > listModelSuitesPageToken.pageSize) {
        val pageToken =
          listModelSuitesPageToken.copy {
            lastModelSuite =
              ListModelSuitesPageTokenKt.previousPageEnd {
                createTime = results[results.lastIndex - 1].createTime
                externalModelSuiteId = results[results.lastIndex - 1].externalModelSuiteId
                externalModelProviderId = results[results.lastIndex - 1].externalModelProviderId
              }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  /** Converts a public [ListModelSuitesRequest] to an internal [ListModelSuitesPageToken]. */
  private fun ListModelSuitesRequest.toListModelSuitesPageToken(): ListModelSuitesPageToken {
    val source = this

    val key =
      grpcRequireNotNull(ModelProviderKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)

    return if (source.pageToken.isNotBlank()) {
      ListModelSuitesPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalModelProviderId == externalModelProviderId) {
          "Arguments must be kept the same when using a page token"
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listModelSuitesPageToken {
        pageSize =
          when {
            source.pageSize == 0 -> DEFAULT_PAGE_SIZE
            source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
            else -> source.pageSize
          }
        this.externalModelProviderId = externalModelProviderId
      }
    }
  }

  /** Converts an internal [ListModelSuitesPageToken] to an internal [StreamModelSuitesRequest]. */
  private fun ListModelSuitesPageToken.toStreamModelSuitesRequest(): StreamModelSuitesRequest {
    val source = this
    return streamModelSuitesRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalModelProviderId = source.externalModelProviderId
        if (source.hasLastModelSuite()) {
          after = afterFilter {
            createTime = source.lastModelSuite.createTime
            externalModelSuiteId = source.lastModelSuite.externalModelSuiteId
            externalModelProviderId = source.lastModelSuite.externalModelProviderId
          }
        }
      }
    }
  }
}
