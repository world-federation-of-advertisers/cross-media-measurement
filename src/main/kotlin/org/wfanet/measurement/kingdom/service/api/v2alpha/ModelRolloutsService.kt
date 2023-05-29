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
import org.wfanet.measurement.api.v2alpha.CreateModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsPageToken
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelRollout
import org.wfanet.measurement.api.v2alpha.ModelRolloutKey
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase as ModelRolloutsCoroutineService
import org.wfanet.measurement.api.v2alpha.ScheduleModelRolloutFreezeRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listModelRolloutsPageToken
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ModelRollout as InternalModelRollout
import org.wfanet.measurement.api.v2alpha.TimeInterval
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequest
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.rolloutPeriod
import org.wfanet.measurement.internal.kingdom.deleteModelRolloutRequest
import org.wfanet.measurement.internal.kingdom.scheduleModelRolloutFreezeRequest
import org.wfanet.measurement.internal.kingdom.streamModelRolloutsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelRolloutsService(private val internalClient: ModelRolloutsCoroutineStub) :
  ModelRolloutsCoroutineService() {

  override suspend fun createModelRollout(request: CreateModelRolloutRequest): ModelRollout {
    val parentKey =
      grpcRequireNotNull(ModelLineKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val modelReleaseKey =
      grpcRequireNotNull(ModelReleaseKey.fromName(request.modelRollout.modelRelease)) {
        "ModelRelease is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parentKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create ModelRollout for another ModelProvider"
          }
        }
        if (principal.resourceKey.modelProviderId != modelReleaseKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create ModelRollout having ModelRelease owned by another ModelProvider."
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to create ModelRollout"
        }
      }
    }

    val modelRolloutKey =
      if (request.modelRollout.previousModelRollout.isNotBlank()) {
        ModelRolloutKey.fromName(request.modelRollout.previousModelRollout)
      } else {
        null
      }

    val createModelRolloutRequest =
      request.modelRollout.toInternal(parentKey, modelReleaseKey, modelRolloutKey)
    return try {
      internalClient.createModelRollout(createModelRolloutRequest).toModelRollout()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, ex) { "Either ModelLine or ModelRelease were not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) {
            ex.message ?: "Required field unspecified or invalid"
          }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

  override suspend fun scheduleModelRolloutFreeze(
    request: ScheduleModelRolloutFreezeRequest
  ): ModelRollout {
    val key =
      grpcRequireNotNull(ModelRolloutKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot update ModelRollout for another ModelProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to update ModelRollout"
        }
      }
    }

    val internalScheduleModelRolloutFreezeRequest = scheduleModelRolloutFreezeRequest {
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      externalModelRolloutId = apiIdToExternalId(key.modelRolloutId)
      rolloutFreezeTime = request.rolloutFreezeTime
    }

    try {
      return internalClient
        .scheduleModelRolloutFreeze(internalScheduleModelRolloutFreezeRequest)
        .toModelRollout()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "ModelRollout not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) {
            ex.message ?: "Required field unspecified or invalid"
          }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

  override suspend fun deleteModelRollout(request: DeleteModelRolloutRequest): Empty {
    val key =
      grpcRequireNotNull(ModelRolloutKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot delete ModelRollout for another ModelProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to delete ModelRollout"
        }
      }
    }

    val deleteModelRolloutRequest = deleteModelRolloutRequest {
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelRolloutId = apiIdToExternalId(key.modelRolloutId)
    }
    try {
      internalClient.deleteModelRollout(deleteModelRolloutRequest)
      return Empty.getDefaultInstance()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "ModelRollout not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) {
            ex.message ?: "Required field unspecified or invalid"
          }
        Status.Code.FAILED_PRECONDITION ->
          failGrpc(Status.FAILED_PRECONDITION, ex) {
            ex.message ?: "RolloutStartTime already passed"
          }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

  override suspend fun listModelRollouts(
    request: ListModelRolloutsRequest
  ): ListModelRolloutsResponse {
    val parent =
      grpcRequireNotNull(ModelLineKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parent.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ModelRollouts for another ModelProvider"
          }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to list ModelRollouts"
        }
      }
    }

    val listModelRolloutsPageToken = request.toListModelRolloutsPageToken(request.filter.rolloutPeriodOverlapping)

    val results: List<InternalModelRollout> =
      internalClient
        .streamModelRollouts(listModelRolloutsPageToken.toStreamModelRolloutsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListModelRolloutsResponse.getDefaultInstance()
    }

    return listModelRolloutsResponse {
      modelRollout +=
        results.subList(0, min(results.size, listModelRolloutsPageToken.pageSize)).map {
          internalModelRollout ->
          internalModelRollout.toModelRollout()
        }
      if (results.size > listModelRolloutsPageToken.pageSize) {
        val pageToken =
          listModelRolloutsPageToken.copy {
            lastModelRollout = previousPageEnd {
              rolloutPeriodStartTime = results[results.lastIndex - 1].rolloutPeriodStartTime
              externalModelProviderId = results[results.lastIndex - 1].externalModelProviderId
              externalModelSuiteId = results[results.lastIndex - 1].externalModelSuiteId
              externalModelLineId = results[results.lastIndex - 1].externalModelLineId
              externalModelRolloutId = results[results.lastIndex - 1].externalModelRolloutId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  /** Converts a public [ListModelRolloutsRequest] to an internal [ListModelRolloutsPageToken]. */
  private fun ListModelRolloutsRequest.toListModelRolloutsPageToken(rolloutPeriodOverlapping: TimeInterval?): ListModelRolloutsPageToken {
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
      ListModelRolloutsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalModelProviderId == externalModelProviderId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.externalModelSuiteId == externalModelSuiteId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.externalModelLineId == externalModelLineId) {
          "Arguments must be kept the same when using a page token"
        }
        if (rolloutPeriodOverlapping != null) {
          grpcRequire(this.rolloutPeriodOverlapping == rolloutPeriodOverlapping) {
            "Arguments must be kept the same when using a page token"
          }
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listModelRolloutsPageToken {
        pageSize =
          when {
            source.pageSize == 0 -> DEFAULT_PAGE_SIZE
            source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
            else -> source.pageSize
          }
        this.externalModelProviderId = externalModelProviderId
        this.externalModelSuiteId = externalModelSuiteId
        this.externalModelLineId = externalModelLineId
        this.rolloutPeriodOverlapping = source.filter.rolloutPeriodOverlapping
      }
    }
  }

  /**
   * Converts an internal [ListModelRolloutsPageToken] to an internal [StreamModelRolloutsRequest].
   */
  private fun ListModelRolloutsPageToken.toStreamModelRolloutsRequest():
    StreamModelRolloutsRequest {
    val source = this
    return streamModelRolloutsRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalModelProviderId = source.externalModelProviderId
        externalModelSuiteId = source.externalModelSuiteId
        externalModelLineId = source.externalModelLineId
        if (source.hasRolloutPeriodOverlapping()) {
          rolloutPeriod = rolloutPeriod {
            rolloutPeriodStartTime = source.rolloutPeriodOverlapping.startTime
            rolloutPeriodEndTime = source.rolloutPeriodOverlapping.endTime
          }
        }
        if (source.hasLastModelRollout()) {
          after = afterFilter {
            externalModelProviderId = source.lastModelRollout.externalModelProviderId
            externalModelSuiteId = source.lastModelRollout.externalModelSuiteId
            externalModelLineId = source.lastModelRollout.externalModelLineId
            externalModelRolloutId = source.lastModelRollout.externalModelRolloutId
            rolloutPeriodStartTime = source.lastModelRollout.rolloutPeriodStartTime
          }
        }
      }
    }
  }
}
