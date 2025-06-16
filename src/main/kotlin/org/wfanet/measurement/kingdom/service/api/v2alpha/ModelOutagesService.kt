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

import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateModelOutageRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteModelOutageRequest
import org.wfanet.measurement.api.v2alpha.ListModelOutagesPageToken
import org.wfanet.measurement.api.v2alpha.ListModelOutagesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelOutagesRequest
import org.wfanet.measurement.api.v2alpha.ListModelOutagesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelOutage
import org.wfanet.measurement.api.v2alpha.ModelOutageKey
import org.wfanet.measurement.api.v2alpha.ModelOutagesGrpcKt.ModelOutagesCoroutineImplBase as ModelOutagesCoroutineService
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listModelOutagesPageToken
import org.wfanet.measurement.api.v2alpha.listModelOutagesResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ModelOutage as InternalModelOutage
import org.wfanet.measurement.internal.kingdom.ModelOutagesGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequestKt.outageInterval
import org.wfanet.measurement.internal.kingdom.deleteModelOutageRequest
import org.wfanet.measurement.internal.kingdom.streamModelOutagesRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelOutagesService(
  private val internalClient: ModelOutagesGrpcKt.ModelOutagesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelOutagesCoroutineService(coroutineContext) {

  override suspend fun createModelOutage(request: CreateModelOutageRequest): ModelOutage {
    val parentKey =
      grpcRequireNotNull(ModelLineKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parentKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create ModelOutage for another ModelProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to create ModelOutage"
        }
      }
    }

    val createModelOutageRequest = request.modelOutage.toInternal(parentKey)
    return try {
      internalClient.createModelOutage(createModelOutageRequest).toModelOutage()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun deleteModelOutage(request: DeleteModelOutageRequest): ModelOutage {
    val key =
      grpcRequireNotNull(ModelOutageKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot delete ModelOutage for another ModelProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to delete ModelOutage"
        }
      }
    }

    val deleteRequest = deleteModelOutageRequest {
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelOutageId = apiIdToExternalId(key.modelOutageId)
    }

    return try {
      internalClient.deleteModelOutage(deleteRequest).toModelOutage()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listModelOutages(
    request: ListModelOutagesRequest
  ): ListModelOutagesResponse {
    val parent =
      grpcRequireNotNull(ModelLineKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parent.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ModelOutages for another ModelProvider"
          }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to list ModelOutages"
        }
      }
    }

    val listModelOutagesPageToken = request.toListModelOutagesPageToken()

    val results: List<InternalModelOutage> =
      internalClient
        .streamModelOutages(listModelOutagesPageToken.toStreamModelOutagesRequest())
        .toList()

    if (results.isEmpty()) {
      return ListModelOutagesResponse.getDefaultInstance()
    }

    return listModelOutagesResponse {
      modelOutages +=
        results.subList(0, min(results.size, listModelOutagesPageToken.pageSize)).map {
          internalModelOutage ->
          internalModelOutage.toModelOutage()
        }
      if (results.size > listModelOutagesPageToken.pageSize) {
        val pageToken =
          listModelOutagesPageToken.copy {
            lastModelOutage = previousPageEnd {
              createTime = results[results.lastIndex - 1].createTime
              externalModelProviderId = results[results.lastIndex - 1].externalModelProviderId
              externalModelSuiteId = results[results.lastIndex - 1].externalModelSuiteId
              externalModelLineId = results[results.lastIndex - 1].externalModelLineId
              externalModelOutageId = results[results.lastIndex - 1].externalModelOutageId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  /** Converts a public [ListModelOutagesRequest] to an internal [ListModelOutagesPageToken]. */
  private fun ListModelOutagesRequest.toListModelOutagesPageToken(): ListModelOutagesPageToken {
    val source = this

    val key =
      grpcRequireNotNull(ModelLineKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
    val externalModelLineId = apiIdToExternalId(key.modelLineId)

    return if (source.pageToken.isNotBlank()) {
      ListModelOutagesPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalModelProviderId == externalModelProviderId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.externalModelSuiteId == externalModelSuiteId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.externalModelLineId == externalModelLineId) {
          "Arguments must be kept the same when using a page token"
        }
        if (this.hasOutageInterval()) {
          grpcRequire(
            source.hasFilter() &&
              source.filter.hasOutageIntervalOverlapping() &&
              Timestamps.compare(
                source.filter.outageIntervalOverlapping.startTime,
                this.outageInterval.startTime,
              ) == 0 &&
              Timestamps.compare(
                source.filter.outageIntervalOverlapping.endTime,
                this.outageInterval.endTime,
              ) == 0
          ) {
            "Arguments must be kept the same when using a page token"
          }
        }
        grpcRequire(this.showDeleted == source.showDeleted) {
          "Arguments must be kept the same when using a page token"
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listModelOutagesPageToken {
        pageSize =
          when {
            source.pageSize == 0 -> DEFAULT_PAGE_SIZE
            source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
            else -> source.pageSize
          }
        this.externalModelProviderId = externalModelProviderId
        this.externalModelSuiteId = externalModelSuiteId
        this.externalModelLineId = externalModelLineId
        if (source.hasFilter() && source.filter.hasOutageIntervalOverlapping()) {
          this.outageInterval = source.filter.outageIntervalOverlapping
        }
        this.showDeleted = source.showDeleted
      }
    }
  }

  /**
   * Converts an internal [ListModelOutagesPageToken] to an internal [StreamModelOutagesRequest].
   */
  private fun ListModelOutagesPageToken.toStreamModelOutagesRequest(): StreamModelOutagesRequest {
    val source = this
    return streamModelOutagesRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalModelProviderId = source.externalModelProviderId
        externalModelSuiteId = source.externalModelSuiteId
        externalModelLineId = source.externalModelLineId
        showDeleted = source.showDeleted
        if (source.hasOutageInterval()) {
          outageInterval = outageInterval {
            modelOutageStartTime = source.outageInterval.startTime
            modelOutageEndTime = source.outageInterval.endTime
          }
        }
        if (source.hasLastModelOutage()) {
          after = afterFilter {
            externalModelProviderId = source.lastModelOutage.externalModelProviderId
            externalModelSuiteId = source.lastModelOutage.externalModelSuiteId
            externalModelLineId = source.lastModelOutage.externalModelLineId
            externalModelOutageId = source.lastModelOutage.externalModelOutageId
            createTime = source.lastModelOutage.createTime
          }
        }
      }
    }
  }
}
