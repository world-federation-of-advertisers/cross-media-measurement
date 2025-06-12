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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.accountFromCurrentContext
import org.wfanet.measurement.api.v2alpha.ApiKey
import org.wfanet.measurement.api.v2alpha.ApiKeyKey
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CreateApiKeyRequest
import org.wfanet.measurement.api.v2alpha.DeleteApiKeyRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ApiKey as InternalApiKey
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.apiKey as internalApiKey
import org.wfanet.measurement.internal.kingdom.deleteApiKeyRequest

class ApiKeysService(
  private val internalApiKeysStub: ApiKeysCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ApiKeysCoroutineImplBase(coroutineContext) {

  override suspend fun createApiKey(request: CreateApiKeyRequest): ApiKey {
    val account = accountFromCurrentContext

    val measurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Measurement Consumer resource name unspecified or invalid"
      }

    val externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerKey.measurementConsumerId)
    if (!account.externalOwnedMeasurementConsumerIdsList.contains(externalMeasurementConsumerId)) {
      failGrpc(Status.PERMISSION_DENIED) { "Account doesn't own Measurement Consumer" }
    }

    grpcRequire(request.apiKey.nickname.isNotBlank()) { "Nickname is unspecified" }

    val internalCreateApiKeyRequest = internalApiKey {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      nickname = request.apiKey.nickname
      description = request.apiKey.description
    }

    val result =
      try {
        internalApiKeysStub.createApiKey(internalCreateApiKeyRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return result.toApiKey()
  }

  override suspend fun deleteApiKey(request: DeleteApiKeyRequest): ApiKey {
    val account = accountFromCurrentContext

    val key =
      grpcRequireNotNull(ApiKeyKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    if (!account.externalOwnedMeasurementConsumerIdsList.contains(externalMeasurementConsumerId)) {
      failGrpc(Status.PERMISSION_DENIED) { "Account doesn't own Measurement Consumer" }
    }

    val deleteApiKeyRequest = deleteApiKeyRequest {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalApiKeyId = apiIdToExternalId(key.apiKeyId)
    }

    val result =
      try {
        internalApiKeysStub.deleteApiKey(deleteApiKeyRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return result.toApiKey()
  }

  /** Converts an internal [InternalApiKey] to a public [ApiKey]. */
  private fun InternalApiKey.toApiKey(): ApiKey {
    val source = this

    return apiKey {
      name =
        ApiKeyKey(
            externalIdToApiId(source.externalMeasurementConsumerId),
            externalIdToApiId(source.externalApiKeyId),
          )
          .toName()
      nickname = source.nickname
      description = source.description
      authenticationKey = externalIdToApiId(source.authenticationKey)
    }
  }
}
