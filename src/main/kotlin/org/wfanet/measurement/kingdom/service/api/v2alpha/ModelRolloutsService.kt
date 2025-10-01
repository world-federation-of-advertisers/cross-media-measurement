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
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import java.time.ZoneOffset
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
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
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelRollout as InternalModelRollout
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

class ModelRolloutsService(
  private val internalClient: ModelRolloutsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelRolloutsCoroutineService(coroutineContext) {

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

    if (
      request.modelRollout.rolloutDeployPeriodCase ==
        ModelRollout.RolloutDeployPeriodCase.ROLLOUTDEPLOYPERIOD_NOT_SET
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "RolloutDeployPeriod is unspecified" }
    }

    val createModelRolloutRequest = request.modelRollout.toInternal(parentKey, modelReleaseKey)
    return try {
      internalClient.createModelRollout(createModelRolloutRequest).toModelRollout()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
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
      rolloutFreezeTime =
        request.rolloutFreezeDate
          .toLocalDate()
          .atStartOfDay()
          .toInstant(ZoneOffset.UTC)
          .toProtoTime()
    }

    try {
      return internalClient
        .scheduleModelRolloutFreeze(internalScheduleModelRolloutFreezeRequest)
        .toModelRollout()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
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
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
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

    val listModelRolloutsPageToken = request.toListModelRolloutsPageToken()

    val results: List<InternalModelRollout> =
      internalClient
        .streamModelRollouts(listModelRolloutsPageToken.toStreamModelRolloutsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListModelRolloutsResponse.getDefaultInstance()
    }

    return listModelRolloutsResponse {
      modelRollouts +=
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
  private fun ListModelRolloutsRequest.toListModelRolloutsPageToken(): ListModelRolloutsPageToken {
    val source = this

    val key =
      grpcRequireNotNull(ModelLineKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
    val externalModelLineId: Long =
      if (key.modelLineId == ResourceKey.WILDCARD_ID) {
        0L
      } else {
        apiIdToExternalId(key.modelLineId)
      }
    val externalModelReleaseIds: List<Long> =
      source.filter.modelReleaseInList.map { modelReleaseName ->
        val modelReleaseKey =
          grpcRequireNotNull(ModelReleaseKey.fromName(modelReleaseName)) {
            "ModelRelease name invalid or unspecified"
          }
        if (modelReleaseKey.parentKey != key.parentKey) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "ModelRelease does not belong to ancestor ModelSuite"
            )
            .asRuntimeException()
        }
        ApiId(modelReleaseKey.modelReleaseId).externalId.value
      }

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
        grpcRequire(this.externalModelReleaseIdIn == externalModelReleaseIds) {
          "Arguments must be kept the same when using a page token"
        }
        if (this.hasRolloutPeriodOverlapping()) {
          grpcRequire(
            source.hasFilter() &&
              source.filter.hasRolloutPeriodOverlapping() &&
              source.filter.rolloutPeriodOverlapping.startDate ==
                this.rolloutPeriodOverlapping.startTime
                  .toInstant()
                  .atZone(ZoneOffset.UTC)
                  .toLocalDate()
                  .toProtoDate() &&
              source.filter.rolloutPeriodOverlapping.endDate ==
                this.rolloutPeriodOverlapping.endTime
                  .toInstant()
                  .atZone(ZoneOffset.UTC)
                  .toLocalDate()
                  .toProtoDate()
          ) {
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
        this.externalModelReleaseIdIn += externalModelReleaseIds
        if (source.filter.hasRolloutPeriodOverlapping()) {
          this.rolloutPeriodOverlapping = interval {
            startTime =
              source.filter.rolloutPeriodOverlapping.startDate
                .toLocalDate()
                .atStartOfDay()
                .toInstant(ZoneOffset.UTC)
                .toProtoTime()
            endTime =
              source.filter.rolloutPeriodOverlapping.endDate
                .toLocalDate()
                .atStartOfDay()
                .toInstant(ZoneOffset.UTC)
                .toProtoTime()
          }
        }
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
        externalModelReleaseIdIn += source.externalModelReleaseIdInList
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
