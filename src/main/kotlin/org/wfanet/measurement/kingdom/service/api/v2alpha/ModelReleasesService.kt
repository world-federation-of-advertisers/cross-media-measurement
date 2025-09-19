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
import org.wfanet.measurement.api.v2alpha.CreateModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.GetModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.ListModelReleasesPageToken
import org.wfanet.measurement.api.v2alpha.ListModelReleasesPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListModelReleasesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.ListModelReleasesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase as ModelReleasesCoroutineService
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listModelReleasesPageToken
import org.wfanet.measurement.api.v2alpha.listModelReleasesResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.ModelRelease as InternalModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.getModelReleaseRequest
import org.wfanet.measurement.internal.kingdom.populationKey
import org.wfanet.measurement.internal.kingdom.streamModelReleasesRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelReleasesService(
  private val internalClient: ModelReleasesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelReleasesCoroutineService(coroutineContext) {

  override suspend fun createModelRelease(request: CreateModelReleaseRequest): ModelRelease {
    val parentKey =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }
    val populationKey =
      grpcRequireNotNull(PopulationKey.fromName(request.modelRelease.population)) {
        "Population is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parentKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create ModelRelease for another ModelProvider"
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to create ModelRelease"
        }
      }
    }

    val createModelReleaseRequest = request.modelRelease.toInternal(parentKey, populationKey)
    return try {
      internalClient.createModelRelease(createModelReleaseRequest).toModelRelease()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun getModelRelease(request: GetModelReleaseRequest): ModelRelease {
    val key =
      grpcRequireNotNull(ModelReleaseKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot get ModelRelease of another ModelProvider" }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to get ModelRelease" }
      }
    }

    val internalGetModelReleaseRequest = getModelReleaseRequest {
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelReleaseId = apiIdToExternalId(key.modelReleaseId)
    }

    try {
      return internalClient.getModelRelease(internalGetModelReleaseRequest).toModelRelease()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listModelReleases(
    request: ListModelReleasesRequest
  ): ListModelReleasesResponse {
    val parent =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val listModelReleasesPageToken = request.toListModelReleasesPageToken()

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parent.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ModelReleases for another ModelProvider"
          }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to list ModelReleases"
        }
      }
    }

    val results: List<InternalModelRelease> =
      internalClient
        .streamModelReleases(listModelReleasesPageToken.toStreamModelReleasesRequest())
        .toList()

    if (results.isEmpty()) {
      return ListModelReleasesResponse.getDefaultInstance()
    }

    return listModelReleasesResponse {
      modelReleases +=
        results.subList(0, min(results.size, listModelReleasesPageToken.pageSize)).map {
          internalModelRelease ->
          internalModelRelease.toModelRelease()
        }
      if (results.size > listModelReleasesPageToken.pageSize) {
        val pageToken =
          listModelReleasesPageToken.copy {
            lastModelRelease = previousPageEnd {
              createTime = results[results.lastIndex - 1].createTime
              externalModelProviderId = results[results.lastIndex - 1].externalModelProviderId
              externalModelSuiteId = results[results.lastIndex - 1].externalModelSuiteId
              externalModelReleaseId = results[results.lastIndex - 1].externalModelReleaseId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  /** Converts a public [ListModelReleasesRequest] to an internal [ListModelReleasesPageToken]. */
  private fun ListModelReleasesRequest.toListModelReleasesPageToken(): ListModelReleasesPageToken {
    val source = this

    val key =
      grpcRequireNotNull(ModelSuiteKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalModelSuiteId =
      if (key.modelSuiteId == ResourceKey.WILDCARD_ID) {
        0
      } else {
        apiIdToExternalId(key.modelSuiteId)
      }
    val populationKeys =
      source.filter.populationInList.map { population ->
        val populationKey =
          grpcRequireNotNull(PopulationKey.fromName(population)) {
            "Population resource name unspecified or invalid"
          }
        ListModelReleasesPageTokenKt.populationKey {
          externalDataProviderId = ApiId(populationKey.dataProviderId).externalId.value
          externalPopulationId = ApiId(populationKey.populationId).externalId.value
        }
      }

    return if (source.pageToken.isNotBlank()) {
      ListModelReleasesPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalModelProviderId == externalModelProviderId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.externalModelSuiteId == externalModelSuiteId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.populationKeys == populationKeys) {
          "Arguments must be kept the same when using a page token"
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listModelReleasesPageToken {
        pageSize =
          when {
            source.pageSize == 0 -> DEFAULT_PAGE_SIZE
            source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
            else -> source.pageSize
          }
        this.externalModelProviderId = externalModelProviderId
        this.externalModelSuiteId = externalModelSuiteId
        this.populationKeys += populationKeys
      }
    }
  }

  /**
   * Converts an internal [ListModelReleasesPageToken] to an internal [StreamModelReleasesRequest].
   */
  private fun ListModelReleasesPageToken.toStreamModelReleasesRequest():
    StreamModelReleasesRequest {
    val source = this
    return streamModelReleasesRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalModelProviderId = source.externalModelProviderId
        externalModelSuiteId = source.externalModelSuiteId
        for (populationKey: ListModelReleasesPageToken.PopulationKey in source.populationKeysList) {
          populationKeyIn += populationKey {
            externalDataProviderId = populationKey.externalDataProviderId
            externalPopulationId = populationKey.externalPopulationId
          }
        }
        if (source.hasLastModelRelease()) {
          after = afterFilter {
            createTime = source.lastModelRelease.createTime
            externalModelProviderId = source.lastModelRelease.externalModelProviderId
            externalModelSuiteId = source.lastModelRelease.externalModelSuiteId
            externalModelReleaseId = source.lastModelRelease.externalModelReleaseId
          }
        }
      }
    }
  }
}
