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
import org.wfanet.measurement.api.v2alpha.CreatePopulationRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.GetPopulationRequest
import org.wfanet.measurement.api.v2alpha.ListPopulationsPageToken
import org.wfanet.measurement.api.v2alpha.ListPopulationsPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListPopulationsRequest
import org.wfanet.measurement.api.v2alpha.ListPopulationsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt.PopulationsCoroutineImplBase as PopulationsCoroutineService
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listPopulationsPageToken
import org.wfanet.measurement.api.v2alpha.listPopulationsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.Population as InternalPopulation
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequest
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.createPopulationRequest
import org.wfanet.measurement.internal.kingdom.getPopulationRequest
import org.wfanet.measurement.internal.kingdom.streamPopulationsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class PopulationsService(
  private val internalClient: PopulationsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PopulationsCoroutineService(coroutineContext) {

  private enum class Permission {
    CREATE,
    GET,
    LIST;

    fun deniedStatus(name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)"
      )
    }
  }

  override suspend fun createPopulation(request: CreatePopulationRequest): Population {
    fun permissionDeniedStatus() = Permission.CREATE.deniedStatus("${request.parent}/populations")

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext

    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    if (authenticatedPrincipal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    when (request.population.populationSpecRefCase) {
      Population.PopulationSpecRefCase.POPULATION_SPEC -> {
        val validationResult =
          PopulationSpecValidator.validateVidRangesList(request.population.populationSpec)
        if (validationResult.isFailure) {
          throw Status.INVALID_ARGUMENT.withDescription("Invalid PopulationSpec")
            .withCause(validationResult.exceptionOrNull()!!)
            .asRuntimeException()
        }
      }

      Population.PopulationSpecRefCase.POPULATION_BLOB -> {
        // No-op.
      }

      Population.PopulationSpecRefCase.POPULATIONSPECREF_NOT_SET ->
        throw Status.INVALID_ARGUMENT.withDescription("population_spec_ref not set")
          .asRuntimeException()
    }

    val internalRequest = createPopulationRequest {
      population = request.population.toInternal(parentKey)
      requestId = request.requestId
    }

    val internalResponse: InternalPopulation =
      try {
        internalClient.createPopulation(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.INTERNAL
        }.toExternalStatusRuntimeException(e)
      }

    return internalResponse.toPopulation()
  }

  override suspend fun getPopulation(request: GetPopulationRequest): Population {
    fun permissionDeniedStatus() = Permission.GET.deniedStatus("${request.name}")

    val key =
      grpcRequireNotNull(PopulationKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    // TODO(jojijac0b) Once the population resource has a field to limit access to only certain MPs,
    // update this permission as well.
    when (val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (authenticatedPrincipal.resourceKey != key.parentKey) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      is ModelProviderPrincipal -> {}
      else -> {
        throw permissionDeniedStatus().asRuntimeException()
      }
    }

    val getPopulationRequest = getPopulationRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      externalPopulationId = apiIdToExternalId(key.populationId)
    }

    try {
      return internalClient.getPopulation(getPopulationRequest).toPopulation()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listPopulations(request: ListPopulationsRequest): ListPopulationsResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    fun permissionDeniedStatus() = Permission.LIST.deniedStatus("${request.parent}/populations")

    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    // TODO(jojijac0b) Once the population resource has a field to limit access to only certain MPs,
    // update this permission as well.
    when (val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (authenticatedPrincipal.resourceKey != parentKey) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      }
      is ModelProviderPrincipal -> {}
      else -> {
        throw permissionDeniedStatus().asRuntimeException()
      }
    }

    val listPopulationsPageToken = request.toListPopulationsPageToken()

    val results: List<InternalPopulation> =
      try {
        internalClient
          .streamPopulations(listPopulationsPageToken.toStreamPopulationsRequest())
          .toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    if (results.isEmpty()) {
      return ListPopulationsResponse.getDefaultInstance()
    }

    return listPopulationsResponse {
      populations +=
        results.subList(0, min(results.size, listPopulationsPageToken.pageSize)).map {
          internalPopulation ->
          internalPopulation.toPopulation()
        }
      if (results.size > listPopulationsPageToken.pageSize) {
        val pageToken =
          listPopulationsPageToken.copy {
            lastPopulation =
              ListPopulationsPageTokenKt.previousPageEnd {
                createTime = results[results.lastIndex - 1].createTime
                externalDataProviderId = results[results.lastIndex - 1].externalDataProviderId
                externalPopulationId = results[results.lastIndex - 1].externalPopulationId
              }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  /** Converts a public [ListPopulationsRequest] to an internal [ListPopulationsPageToken]. */
  private fun ListPopulationsRequest.toListPopulationsPageToken(): ListPopulationsPageToken {
    val source = this

    val key =
      grpcRequireNotNull(DataProviderKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }

    val externalDataProviderId = apiIdToExternalId(key.dataProviderId)

    return if (source.pageToken.isNotBlank()) {
      ListPopulationsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalDataProviderId == externalDataProviderId) {
          "Arguments must be kept the same when using a page token"
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listPopulationsPageToken {
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

  /** Converts an internal [ListPopulationsPageToken] to an internal [StreamPopulationsRequest]. */
  private fun ListPopulationsPageToken.toStreamPopulationsRequest(): StreamPopulationsRequest {
    val source = this
    return streamPopulationsRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalDataProviderId = source.externalDataProviderId
        if (source.hasLastPopulation()) {
          after = afterFilter {
            createTime = source.lastPopulation.createTime
            externalDataProviderId = source.lastPopulation.externalDataProviderId
            externalPopulationId = source.lastPopulation.externalPopulationId
          }
        }
      }
    }
  }
}
