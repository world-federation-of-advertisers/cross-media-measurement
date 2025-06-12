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
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt
import org.wfanet.measurement.internal.kingdom.AuthenticateApiKeyRequest
import org.wfanet.measurement.internal.kingdom.DeleteApiKeyRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ApiKeyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerApiKeyReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateApiKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteApiKey

class SpannerApiKeysService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ApiKeysGrpcKt.ApiKeysCoroutineImplBase(coroutineContext) {
  override suspend fun createApiKey(request: ApiKey): ApiKey {
    try {
      return CreateApiKey(request).execute(client, idGenerator)
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error")
    }
  }

  override suspend fun deleteApiKey(request: DeleteApiKeyRequest): ApiKey {
    try {
      return DeleteApiKey(
          externalApiKeyId = ExternalId(request.externalApiKeyId),
          externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId),
        )
        .execute(client, idGenerator)
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
    } catch (e: ApiKeyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Api Key not found.")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun authenticateApiKey(request: AuthenticateApiKeyRequest): MeasurementConsumer {
    if (request.authenticationKeyHash.isEmpty) {
      failGrpc(Status.INVALID_ARGUMENT) { "authentication_key_hash is missing" }
    }

    client.readOnlyTransaction().use { txn ->
      val apiKey =
        MeasurementConsumerApiKeyReader()
          .readByAuthenticationKeyHash(txn, request.authenticationKeyHash)
          ?.apiKey ?: failGrpc(Status.NOT_FOUND) { "ApiKey not found for hash" }

      return MeasurementConsumerReader()
        .readByExternalMeasurementConsumerId(txn, ExternalId(apiKey.externalMeasurementConsumerId))
        ?.measurementConsumer
        ?: failGrpc(Status.INTERNAL) { "MeasurementConsumer not found for ApiKey" }
    }
  }
}
