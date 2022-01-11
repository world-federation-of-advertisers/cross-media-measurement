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
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.AddMeasurementConsumerOwnerRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RemoveMeasurementConsumerOwnerRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.AddMeasurementConsumerOwner
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurementConsumer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.RemoveMeasurementConsumerOwner

class SpannerMeasurementConsumersService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : MeasurementConsumersCoroutineImplBase() {
  override suspend fun createMeasurementConsumer(
    request: CreateMeasurementConsumerRequest
  ): MeasurementConsumer {
    val measurementConsumer = request.measurementConsumer
    grpcRequire(
      measurementConsumer.details.apiVersion.isNotEmpty() &&
        !measurementConsumer.details.publicKey.isEmpty &&
        !measurementConsumer.details.publicKeySignature.isEmpty
    ) { "Details field of MeasurementConsumer is missing fields." }
    try {
      return CreateMeasurementConsumer(
          measurementConsumer,
          ExternalId(request.externalAccountId),
          request.measurementConsumerCreationTokenHash
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.PERMISSION_DENIED ->
          failGrpc(Status.PERMISSION_DENIED) { "Measurement Consumer creation token is not valid" }
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Account not found" }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) { "Account has not been activated yet" }
        KingdomInternalException.Code.API_KEY_NOT_FOUND,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.EVENT_GROUP_INVALID_ARGS,
        KingdomInternalException.Code.EVENT_GROUP_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun getMeasurementConsumer(
    request: GetMeasurementConsumerRequest
  ): MeasurementConsumer {
    return MeasurementConsumerReader()
      .readByExternalMeasurementConsumerId(
        client.singleUse(),
        ExternalId(request.externalMeasurementConsumerId)
      )
      ?.measurementConsumer
      ?: failGrpc(Status.NOT_FOUND) { "MeasurementConsumer not found" }
  }

  override suspend fun addMeasurementConsumerOwner(
    request: AddMeasurementConsumerOwnerRequest
  ): MeasurementConsumer {
    try {
      return AddMeasurementConsumerOwner(
          externalAccountId = ExternalId(request.externalAccountId),
          externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Account not found" }
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "MeasurementConsumer not found" }
        KingdomInternalException.Code.API_KEY_NOT_FOUND,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.PERMISSION_DENIED,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
        KingdomInternalException.Code.EVENT_GROUP_INVALID_ARGS,
        KingdomInternalException.Code.EVENT_GROUP_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun removeMeasurementConsumerOwner(
    request: RemoveMeasurementConsumerOwnerRequest
  ): MeasurementConsumer {
    try {
      return RemoveMeasurementConsumerOwner(
          externalAccountId = ExternalId(request.externalAccountId),
          externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Account not found" }
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "MeasurementConsumer not found" }
        KingdomInternalException.Code.PERMISSION_DENIED ->
          failGrpc(Status.FAILED_PRECONDITION) { "Account doesn't own MeasurementConsumer" }
        KingdomInternalException.Code.API_KEY_NOT_FOUND,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
        KingdomInternalException.Code.EVENT_GROUP_INVALID_ARGS,
        KingdomInternalException.Code.EVENT_GROUP_NOT_FOUND -> throw e
      }
    }
  }
}
