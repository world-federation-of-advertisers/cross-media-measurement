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
import org.wfanet.measurement.api.v2alpha.CreateModelLineRequest
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase as ModelLinesCoroutineService
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.SetActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.modelLine
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest

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
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "ModelProvider not found." }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
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
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "ModelLine not found." }
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) { "ActiveEndTime is invalid." }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
      }
    }
  }

  /*override suspend fun createModelSuite(request: CreateModelSuiteRequest): ModelSuite {
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
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "ModelProvider not found." }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
      }
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
      internalClient
        .streamModelSuites(listModelSuitesPageToken.toStreamModelSuitesRequest())
        .toList()

    if (results.isEmpty()) {
      return ListModelSuitesResponse.getDefaultInstance()
    }

    return listModelSuitesResponse {
      modelSuite +=
        results.subList(0, min(results.size, listModelSuitesPageToken.pageSize)).map {
            internalModelSuite ->
          internalModelSuite.toModelSuite()
        }
      if (results.size > listModelSuitesPageToken.pageSize) {
        val pageToken =
          listModelSuitesPageToken.copy {
            lastModelSuite =
              ListModelSuitesPageTokenKt.previousPageEnd {
                createdAfter = results[results.lastIndex - 1].createTime
                externalModelSuiteId = results[results.lastIndex - 1].externalModelSuiteId
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
      filter =
        StreamModelSuitesRequestKt.filter {
          externalModelProviderId = source.externalModelProviderId
          if (source.hasLastModelSuite()) {
            createdAfter = source.lastModelSuite.createdAfter
            externalModelSuiteId = source.lastModelSuite.externalModelSuiteId
          }
        }
    }
  }*/
}
