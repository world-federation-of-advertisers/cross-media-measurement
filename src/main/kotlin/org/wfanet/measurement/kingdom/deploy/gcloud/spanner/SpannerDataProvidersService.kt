// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.BatchGetDataProvidersRequest
import org.wfanet.measurement.internal.kingdom.BatchGetDataProvidersResponse
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.ReplaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.internal.kingdom.ReplaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.internal.kingdom.ReplaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.internal.kingdom.batchGetDataProvidersResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotActiveException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateDataProvider
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceDataAvailabilityInterval
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceDataAvailabilityIntervals
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceDataProviderCapabilities
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceDataProviderRequiredDuchies

class SpannerDataProvidersService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : DataProvidersCoroutineImplBase(coroutineContext) {
  override suspend fun createDataProvider(request: DataProvider): DataProvider {
    grpcRequireNotNull(Version.fromStringOrNull(request.details.apiVersion)) {
      "details.api_version is invalid or unspecified"
    }
    grpcRequire(!request.details.publicKey.isEmpty && !request.details.publicKeySignature.isEmpty) {
      "Details field of DataProvider is missing fields."
    }

    try {
      validateDataAvailabilityIntervals(request.dataAvailabilityIntervalsList)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      CreateDataProvider(request).execute(client, idGenerator)
    } catch (e: ModelLineNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ModelLineNotActiveException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override suspend fun getDataProvider(request: GetDataProviderRequest): DataProvider {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    return DataProviderReader()
      .readByExternalDataProviderId(client.singleUse(), externalDataProviderId)
      ?.dataProvider
      ?: throw DataProviderNotFoundException(externalDataProviderId)
        .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
  }

  override suspend fun batchGetDataProviders(
    request: BatchGetDataProvidersRequest
  ): BatchGetDataProvidersResponse {
    val dataProviders =
      try {
        DataProviderReader()
          .readByExternalDataProviderIds(
            client.singleUse(),
            request.externalDataProviderIdsList.map(::ExternalId),
          )
          .map(DataProviderReader.Result::dataProvider)
      } catch (e: DataProviderNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: KingdomInternalException) {
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      }

    return batchGetDataProvidersResponse { this.dataProviders += dataProviders }
  }

  override suspend fun replaceDataProviderRequiredDuchies(
    request: ReplaceDataProviderRequiredDuchiesRequest
  ): DataProvider {
    grpcRequire(request.externalDataProviderId != 0L) { "externalDataProviderId field is missing." }
    try {
      return ReplaceDataProviderRequiredDuchies(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    }
  }

  override suspend fun replaceDataAvailabilityIntervals(
    request: ReplaceDataAvailabilityIntervalsRequest
  ): DataProvider {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    try {
      validateDataAvailabilityIntervals(request.dataAvailabilityIntervalsList)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    try {
      return ReplaceDataAvailabilityIntervals(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ModelLineNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ModelLineNotActiveException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override suspend fun replaceDataAvailabilityInterval(
    request: ReplaceDataAvailabilityIntervalRequest
  ): DataProvider {
    grpcRequire(request.externalDataProviderId != 0L) { "external_data_provider_id is missing." }

    try {
      return ReplaceDataAvailabilityInterval(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    }
  }

  override suspend fun replaceDataProviderCapabilities(
    request: ReplaceDataProviderCapabilitiesRequest
  ): DataProvider {
    grpcRequire(request.externalDataProviderId != 0L) { "external_data_provider_id is missing." }

    try {
      return ReplaceDataProviderCapabilities(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
    }
  }

  companion object {
    /**
     * Validates a `data_availability_intervals` field.
     *
     * @throws RequiredFieldNotSetException
     */
    private fun validateDataAvailabilityIntervals(
      value: List<DataProvider.DataAvailabilityMapEntry>
    ) {
      value.forEachIndexed { index: Int, entry: DataProvider.DataAvailabilityMapEntry ->
        if (!entry.hasKey()) {
          throw RequiredFieldNotSetException("data_availability_intervals[$index].key")
        }
        if (!entry.value.hasStartTime()) {
          throw RequiredFieldNotSetException("data_availability_intervals[$index].value.start_time")
        }
        if (!entry.value.hasEndTime()) {
          throw RequiredFieldNotSetException("data_availability_intervals[$index].value.end_time")
        }
      }
    }
  }
}
