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
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateModelLineRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ListModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.ListModelLinesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase as ModelLinesCoroutineService
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.SetActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelLinesService(private val internalClient: ModelLinesCoroutineStub) :
  ModelLinesCoroutineService() {

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

    val createModelLineRequest = request.modelLine.toInternal(parentKey)
    return try {
      internalClient.createModelLine(createModelLineRequest).toModelLine()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "ModelProvider not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "Required field unspecified or invalid" }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

  override suspend fun setActiveEndTime(request: SetActiveEndTimeRequest): ModelLine {
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

    val internalSetActiveEndTimeRequest = setActiveEndTimeRequest {
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      activeEndTime = request.activeEndTime
    }

    try {
      return internalClient.setActiveEndTime(internalSetActiveEndTimeRequest).toModelLine()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "ModelLine not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) { ex.message ?: "ActiveEndTime is invalid" }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
  }

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
      grpcRequireNotNull(ModelLineKey.fromName(request.holdbackModelLine)) {
        "Holdback model line name is either unspecified or invalid"
      }

    val internalSetHoldbackModelLineRequest = setModelLineHoldbackModelLineRequest {
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      externalHoldbackModelLineId = apiIdToExternalId(holdbackModelLineKey.modelLineId)
      externalHoldbackModelSuiteId = apiIdToExternalId(holdbackModelLineKey.modelSuiteId)
      externalHoldbackModelProviderId = apiIdToExternalId(holdbackModelLineKey.modelProviderId)
    }

    try {
      return internalClient
        .setModelLineHoldbackModelLine(internalSetHoldbackModelLineRequest)
        .toModelLine()
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "ModelLine not found" }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) { ex.message ?: "ModelLine has wrong type" }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception" }
      }
    }
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
      modelLine +=
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

  /** Converts a public [ListModelLinesRequest] to an internal [ListModelLinesPageToken]. */
  private fun ListModelLinesRequest.toListModelLinesPageToken(): ListModelLinesPageToken {
    val source = this

    val key =
      grpcRequireNotNull(ModelSuiteKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)

    return if (source.pageToken.isNotBlank()) {
      ListModelLinesPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalModelProviderId == externalModelProviderId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.externalModelSuiteId == externalModelSuiteId) {
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
        this.types += source.filter.typeList
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
        type += source.typesList.map { type -> type.toInternalType() }
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
