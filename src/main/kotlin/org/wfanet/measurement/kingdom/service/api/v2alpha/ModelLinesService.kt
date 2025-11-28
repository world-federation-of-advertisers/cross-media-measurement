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
import io.grpc.StatusRuntimeException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateModelLineRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.EnumerateValidModelLinesRequest
import org.wfanet.measurement.api.v2alpha.EnumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.GetModelLineRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.ListModelLinesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase as ModelLinesCoroutineService
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.SetModelLineActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.SetModelLineTypeRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.asRuntimeException
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesRequest
import org.wfanet.measurement.internal.kingdom.getModelLineRequest
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest as internalSetActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.setModelLineTypeRequest as internalSetModelLineTypeRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelLinesService(
  private val internalClient: ModelLinesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelLinesCoroutineService(coroutineContext) {

  override suspend fun createModelLine(request: CreateModelLineRequest): ModelLine {
    val parentKey =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parentKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot create ModelLine for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to create ModelLine" }
      }
    }

    if (!request.modelLine.hasActiveStartTime()) {
      throw Status.INVALID_ARGUMENT.withDescription("active_start_time not set")
        .asRuntimeException()
    }
    if (
      request.modelLine.hasActiveEndTime() &&
        Timestamps.compare(request.modelLine.activeStartTime, request.modelLine.activeEndTime) > 0
    ) {
      throw Status.INVALID_ARGUMENT.withDescription("active_end_time is before active_start_time")
        .asRuntimeException()
    }

    val createModelLineRequest = request.modelLine.toInternal(parentKey)
    return try {
      internalClient.createModelLine(createModelLineRequest).toModelLine()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun getModelLine(request: GetModelLineRequest): ModelLine {
    val modelLineKey =
      grpcRequireNotNull(ModelLineKey.fromName(request.name)) {
        "name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != modelLineKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot list ModelLines for another ModelProvider" }
        }
      }
      is DataProviderPrincipal,
      is MeasurementConsumerPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list ModelLines" }
      }
    }

    return try {
      internalClient
        .getModelLine(
          getModelLineRequest {
            externalModelProviderId = apiIdToExternalId(modelLineKey.modelProviderId)
            externalModelSuiteId = apiIdToExternalId(modelLineKey.modelSuiteId)
            externalModelLineId = apiIdToExternalId(modelLineKey.modelLineId)
          }
        )
        .toModelLine()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.INTERNAL
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun setModelLineActiveEndTime(
    request: SetModelLineActiveEndTimeRequest
  ): ModelLine {
    val key =
      grpcRequireNotNull(ModelLineKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot update ModelLine for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to update ModelLine" }
      }
    }

    val internalRequest = internalSetActiveEndTimeRequest {
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      activeEndTime = request.activeEndTime
    }

    try {
      return internalClient.setActiveEndTime(internalRequest).toModelLine()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  @Deprecated("Service method is deprecated")
  override suspend fun setModelLineHoldbackModelLine(
    request: SetModelLineHoldbackModelLineRequest
  ): ModelLine {
    val key =
      grpcRequireNotNull(ModelLineKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot update ModelLine for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to update ModelLine" }
      }
    }

    val holdbackModelLineKey =
      if (request.holdbackModelLine.isEmpty()) {
        null
      } else {
        grpcRequireNotNull(ModelLineKey.fromName(request.holdbackModelLine)) {
          "Holdback model line name is invalid"
        }
      }

    val internalSetHoldbackModelLineRequest = setModelLineHoldbackModelLineRequest {
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      if (holdbackModelLineKey != null) {
        externalHoldbackModelLineId = apiIdToExternalId(holdbackModelLineKey.modelLineId)
        externalHoldbackModelSuiteId = apiIdToExternalId(holdbackModelLineKey.modelSuiteId)
        externalHoldbackModelProviderId = apiIdToExternalId(holdbackModelLineKey.modelProviderId)
      }
    }

    try {
      return internalClient
        .setModelLineHoldbackModelLine(internalSetHoldbackModelLineRequest)
        .toModelLine()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun setModelLineType(request: SetModelLineTypeRequest): ModelLine {
    val key =
      grpcRequireNotNull(ModelLineKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }
    when (request.type) {
      ModelLine.Type.DEV,
      ModelLine.Type.PROD -> Unit
      ModelLine.Type.TYPE_UNSPECIFIED ->
        throw Status.INVALID_ARGUMENT.withDescription("type not specified").asRuntimeException()
      ModelLine.Type.HOLDBACK,
      ModelLine.Type.UNRECOGNIZED ->
        throw Status.INVALID_ARGUMENT.withDescription("Invalid value for type").asRuntimeException()
    }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot update ModelLine for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to update ModelLine" }
      }
    }

    val internalRequest = internalSetModelLineTypeRequest {
      externalModelProviderId = ApiId(key.modelProviderId).externalId.value
      externalModelSuiteId = ApiId(key.modelSuiteId).externalId.value
      externalModelLineId = ApiId(key.modelLineId).externalId.value
      type = request.type.toInternalType()
    }
    val internalResponse: InternalModelLine =
      try {
        internalClient.setModelLineType(internalRequest)
      } catch (e: StatusException) {
        val exception: StatusRuntimeException =
          when (Errors.getErrorCode(e)) {
            ErrorCode.MODEL_LINE_NOT_FOUND ->
              ModelLineNotFoundException(request.name, e)
                .asStatusRuntimeException(Status.Code.NOT_FOUND)
            ErrorCode.MODEL_LINE_TYPE_ILLEGAL ->
              ModelLineTypeIllegalException.fromInternal(e)
                .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
            ErrorCode.MODEL_LINE_INVALID_ARGS ->
              ModelLineInvalidArgsException(
                  request.name,
                  "${request.name} has a holdback ModeLine",
                  e,
                )
                .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
            ErrorCode.UNKNOWN_ERROR,
            ErrorCode.REQUIRED_FIELD_NOT_SET,
            ErrorCode.INVALID_FIELD_VALUE,
            ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
            ErrorCode.DATA_PROVIDER_NOT_FOUND,
            ErrorCode.MODEL_PROVIDER_NOT_FOUND,
            ErrorCode.DUCHY_NOT_FOUND,
            ErrorCode.MEASUREMENT_NOT_FOUND,
            ErrorCode.MEASUREMENT_STATE_ILLEGAL,
            ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
            ErrorCode.CERTIFICATE_NOT_FOUND,
            ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
            ErrorCode.CERTIFICATE_IS_INVALID,
            ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
            ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
            ErrorCode.COMPUTATION_PARTICIPANT_ETAG_MISMATCH,
            ErrorCode.REQUISITION_NOT_FOUND,
            ErrorCode.REQUISITION_STATE_ILLEGAL,
            ErrorCode.REQUISITION_ETAG_MISMATCH,
            ErrorCode.ACCOUNT_NOT_FOUND,
            ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
            ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
            ErrorCode.PERMISSION_DENIED,
            ErrorCode.API_KEY_NOT_FOUND,
            ErrorCode.EVENT_GROUP_NOT_FOUND,
            ErrorCode.EVENT_GROUP_INVALID_ARGS,
            ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
            ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_ALREADY_EXISTS_WITH_TYPE,
            ErrorCode.RECURRING_EXCHANGE_NOT_FOUND,
            ErrorCode.EXCHANGE_STEP_ATTEMPT_NOT_FOUND,
            ErrorCode.EXCHANGE_STEP_NOT_FOUND,
            ErrorCode.EVENT_GROUP_STATE_ILLEGAL,
            ErrorCode.DUCHY_NOT_ACTIVE,
            ErrorCode.MEASUREMENT_ETAG_MISMATCH,
            ErrorCode.MODEL_SUITE_NOT_FOUND,
            ErrorCode.MODEL_LINE_NOT_ACTIVE,
            ErrorCode.MODEL_OUTAGE_NOT_FOUND,
            ErrorCode.MODEL_OUTAGE_STATE_ILLEGAL,
            ErrorCode.MODEL_OUTAGE_INVALID_ARGS,
            ErrorCode.MODEL_SHARD_NOT_FOUND,
            ErrorCode.MODEL_RELEASE_NOT_FOUND,
            ErrorCode.MODEL_ROLLOUT_OLDER_THAN_PREVIOUS,
            ErrorCode.MODEL_ROLLOUT_NOT_FOUND,
            ErrorCode.MODEL_ROLLOUT_ALREADY_STARTED,
            ErrorCode.MODEL_ROLLOUT_FREEZE_SCHEDULED,
            ErrorCode.MODEL_ROLLOUT_FREEZE_TIME_OUT_OF_RANGE,
            ErrorCode.EXCHANGE_NOT_FOUND,
            ErrorCode.MODEL_SHARD_INVALID_ARGS,
            ErrorCode.POPULATION_NOT_FOUND,
            ErrorCode.UNRECOGNIZED,
            null -> Status.INTERNAL.withCause(e).asRuntimeException()
          }
        throw exception
      }

    return internalResponse.toModelLine()
  }

  override suspend fun listModelLines(request: ListModelLinesRequest): ListModelLinesResponse {
    val parent =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val listModelLinesPageToken = request.toListModelLinesPageToken()

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parent.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot list ModelLines for another ModelProvider" }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list ModelLines" }
      }
    }

    val results: List<InternalModelLine> =
      internalClient.streamModelLines(listModelLinesPageToken.toStreamModelLinesRequest()).toList()

    if (results.isEmpty()) {
      return ListModelLinesResponse.getDefaultInstance()
    }

    return listModelLinesResponse {
      modelLines +=
        results.subList(0, min(results.size, listModelLinesPageToken.pageSize)).map {
          internalModelLine ->
          internalModelLine.toModelLine()
        }
      if (results.size > listModelLinesPageToken.pageSize) {
        val pageToken =
          listModelLinesPageToken.copy {
            lastModelLine = previousPageEnd {
              createTime = results[results.lastIndex - 1].createTime
              externalModelProviderId = results[results.lastIndex - 1].externalModelProviderId
              externalModelSuiteId = results[results.lastIndex - 1].externalModelSuiteId
              externalModelLineId = results[results.lastIndex - 1].externalModelLineId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun enumerateValidModelLines(
    request: EnumerateValidModelLinesRequest
  ): EnumerateValidModelLinesResponse {
    val parent =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parent.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot list ModelLines for another ModelProvider" }
        }
      }
      is DataProviderPrincipal,
      is MeasurementConsumerPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list ModelLines" }
      }
    }

    if (!request.hasTimeInterval()) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing time_interval" }
    }

    if (!Timestamps.isValid(request.timeInterval.startTime)) {
      failGrpc(Status.INVALID_ARGUMENT) { "time_interval.start_time is invalid" }
    }

    if (!Timestamps.isValid(request.timeInterval.endTime)) {
      failGrpc(Status.INVALID_ARGUMENT) { "time_interval.end_time is invalid" }
    }

    if (Timestamps.compare(request.timeInterval.startTime, request.timeInterval.endTime) >= 0) {
      failGrpc(Status.INVALID_ARGUMENT) {
        "time_interval.start_time must be before time_interval.end_time"
      }
    }

    if (request.dataProvidersList.isEmpty()) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing data_providers" }
    }

    val dataProvidersKeySet = buildSet {
      for (dataProviderName in request.dataProvidersList) {
        add(
          DataProviderKey.fromName(dataProviderName)
            ?: failGrpc(Status.INVALID_ARGUMENT) {
              "$dataProviderName in data_providers is invalid"
            }
        )
      }
    }

    val internalModelLines: List<InternalModelLine> =
      try {
        internalClient
          .enumerateValidModelLines(
            enumerateValidModelLinesRequest {
              if (parent.modelProviderId != ResourceKey.WILDCARD_ID) {
                externalModelProviderId = apiIdToExternalId(parent.modelProviderId)
              }
              if (parent.modelSuiteId != ResourceKey.WILDCARD_ID) {
                externalModelSuiteId = apiIdToExternalId(parent.modelSuiteId)
              }
              timeInterval = request.timeInterval
              for (dataProviderKey in dataProvidersKeySet) {
                externalDataProviderIds += apiIdToExternalId(dataProviderKey.dataProviderId)
              }
              if (request.typesList.isEmpty()) {
                types += InternalModelLine.Type.PROD
              } else {
                for (type in request.typesList) {
                  types += type.toInternalType()
                }
              }
            }
          )
          .modelLinesList
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.asRuntimeException()
      }

    return enumerateValidModelLinesResponse {
      for (internalModelLine in internalModelLines) {
        modelLines += internalModelLine.toModelLine()
      }
    }
  }

  /** Converts a public [ListModelLinesRequest] to an internal [ListModelLinesPageToken]. */
  private fun ListModelLinesRequest.toListModelLinesPageToken(): ListModelLinesPageToken {
    val source = this

    val key =
      grpcRequireNotNull(ModelSuiteKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalModelProviderId =
      if (key.modelProviderId == ResourceKey.WILDCARD_ID) {
        0L
      } else {
        apiIdToExternalId(key.modelProviderId)
      }
    val externalModelSuiteId =
      if (key.modelSuiteId == ResourceKey.WILDCARD_ID) {
        0L
      } else {
        apiIdToExternalId(key.modelSuiteId)
      }

    return if (source.pageToken.isNotBlank()) {
      ListModelLinesPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(
          this.externalModelProviderId == externalModelProviderId &&
            this.externalModelSuiteId == externalModelSuiteId &&
            this.typeIn == source.filter.typeInList &&
            this.activeIntervalContains == source.filter.activeIntervalContains
        ) {
          "Arguments must be kept the same when using a page token"
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listModelLinesPageToken {
        pageSize =
          when {
            source.pageSize == 0 -> DEFAULT_PAGE_SIZE
            source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
            else -> source.pageSize
          }
        this.externalModelProviderId = externalModelProviderId
        this.externalModelSuiteId = externalModelSuiteId
        this.typeIn += source.filter.typeInList
        if (source.filter.hasActiveIntervalContains()) {
          activeIntervalContains = source.filter.activeIntervalContains
        }
      }
    }
  }

  /** Converts an internal [ListModelLinesPageToken] to an internal [StreamModelLinesRequest]. */
  private fun ListModelLinesPageToken.toStreamModelLinesRequest(): StreamModelLinesRequest {
    val source = this
    return streamModelLinesRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalModelProviderId = source.externalModelProviderId
        externalModelSuiteId = source.externalModelSuiteId
        type += source.typeInList.map { type -> type.toInternalType() }
        if (source.hasActiveIntervalContains()) {
          activeIntervalContains = source.activeIntervalContains
        }
        if (source.hasLastModelLine()) {
          after = afterFilter {
            createTime = source.lastModelLine.createTime
            externalModelProviderId = source.lastModelLine.externalModelProviderId
            externalModelSuiteId = source.lastModelLine.externalModelSuiteId
            externalModelLineId = source.lastModelLine.externalModelLineId
          }
        }
      }
    }
  }
}
