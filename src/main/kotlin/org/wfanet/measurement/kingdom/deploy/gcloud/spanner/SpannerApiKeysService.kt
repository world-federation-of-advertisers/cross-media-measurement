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
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt
import org.wfanet.measurement.internal.kingdom.AuthenticateApiKeyRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.RevokeApiKeyRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerApiKeyReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateApiKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.RevokeApiKey

class SpannerApiKeysService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ApiKeysGrpcKt.ApiKeysCoroutineImplBase() {
  override suspend fun createApiKey(request: ApiKey): ApiKey {
    try {
      return CreateApiKey(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Measurement Consumer not found" }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.API_KEY_NOT_FOUND,
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND,
        KingdomInternalException.Code.PERMISSION_DENIED,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL -> throw e
      }
    }
  }

  override suspend fun revokeApiKey(request: RevokeApiKeyRequest): ApiKey {
    try {
      return RevokeApiKey(
          externalApiKeyId = ExternalId(request.externalApiKeyId),
          externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Measurement Consumer not found" }
        KingdomInternalException.Code.API_KEY_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Api Key not found" }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND,
        KingdomInternalException.Code.PERMISSION_DENIED,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL -> throw e
      }
    }
  }

  override suspend fun authenticateApiKey(request: AuthenticateApiKeyRequest): MeasurementConsumer {
    if (request.authenticationKeyHash.isEmpty) {
      failGrpc(Status.UNAUTHENTICATED) { "Authentication Key hash is missing" }
    }

    val apiKey =
      MeasurementConsumerApiKeyReader()
        .readByAuthenticationKeyHash(client.singleUse(), request.authenticationKeyHash)
        ?.apiKey
        ?: failGrpc(Status.UNAUTHENTICATED) { "Authentication Key is not valid" }

    if (apiKey.revocationState == ApiKey.RevocationState.REVOCATION_STATE_UNSPECIFIED) {
      return MeasurementConsumerReader()
        .readByExternalMeasurementConsumerId(
          client.singleUse(),
          ExternalId(apiKey.externalMeasurementConsumerId)
        )
        ?.measurementConsumer
        ?: failGrpc(Status.NOT_FOUND) { "Measurement Consumer not found" }
    } else {
      failGrpc(Status.UNAUTHENTICATED) { "Authentication Key is not valid" }
    }
  }
}
