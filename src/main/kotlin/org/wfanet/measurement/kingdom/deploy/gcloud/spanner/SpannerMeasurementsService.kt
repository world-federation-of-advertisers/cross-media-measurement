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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.CancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamMeasurements
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CancelMeasurement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetMeasurementResult

class SpannerMeasurementsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : MeasurementsCoroutineImplBase() {

  override suspend fun createMeasurement(request: Measurement): Measurement {
    validateCreateMeasurementRequest(request)
    try {
      return CreateMeasurement(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.CERTIFICATE_IS_INVALID ->
          failGrpc(Status.FAILED_PRECONDITION) { "Certificate is invalid" }
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "MeasurementConsumer not found" }
        ErrorCode.DATA_PROVIDER_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "DataProvider not found" }
        ErrorCode.DUCHY_NOT_FOUND -> failGrpc(Status.INVALID_ARGUMENT) { "Duchy not found" }
        ErrorCode.CERTIFICATE_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "Certificate not found" }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  private fun validateCreateMeasurementRequest(request: Measurement) {
    grpcRequire(request.externalMeasurementConsumerCertificateId != 0L) {
      "external_measurement_consumer_certificate_id unspecified"
    }
    grpcRequire(request.details.apiVersion.isNotEmpty()) { "api_version unspecified" }
    grpcRequire(!request.details.measurementSpec.isEmpty) { "measurement_spec unspecified" }
    grpcRequire(!request.details.measurementSpecSignature.isEmpty) {
      "measurement_spec_signature unspecified"
    }
    for ((externalDataProviderId, dataProvider) in request.dataProvidersMap) {
      grpcRequire(!dataProvider.dataProviderPublicKey.isEmpty) {
        "data_provider_public_key unspecified for ${ExternalId(externalDataProviderId)}"
      }
      grpcRequire(!dataProvider.dataProviderPublicKeySignature.isEmpty) {
        "data_provider_public_key_signature unspecified for ${ExternalId(externalDataProviderId)}"
      }
      grpcRequire(!dataProvider.encryptedRequisitionSpec.isEmpty) {
        "encrypted_requisition_spec unspecified for ${ExternalId(externalDataProviderId)}"
      }
      grpcRequire(!dataProvider.nonceHash.isEmpty) {
        "nonce_hash unspecified for ${ExternalId(externalDataProviderId)}"
      }
    }
  }

  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    return MeasurementReader(Measurement.View.DEFAULT)
      .readByExternalIds(
        client.singleUse(),
        ExternalId(request.externalMeasurementConsumerId),
        ExternalId(request.externalMeasurementId)
      )
      ?.measurement
      ?: failGrpc(Status.NOT_FOUND) { "Measurement not found" }
  }

  override suspend fun getMeasurementByComputationId(
    request: GetMeasurementByComputationIdRequest
  ): Measurement {
    return MeasurementReader(Measurement.View.COMPUTATION)
      .readByExternalComputationId(client.singleUse(), ExternalId(request.externalComputationId))
      ?.measurement
      ?: failGrpc(Status.NOT_FOUND) { "Measurement not found" }
  }

  override fun streamMeasurements(request: StreamMeasurementsRequest): Flow<Measurement> {
    return StreamMeasurements(request.measurementView, request.filter, request.limit)
      .execute(client.singleUse())
      .map { it.measurement }
  }

  override suspend fun setMeasurementResult(request: SetMeasurementResultRequest): Measurement {
    try {
      return SetMeasurementResult(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.MEASUREMENT_NOT_FOUND -> failGrpc(Status.NOT_FOUND, e) { "Measurement not found" }
        ErrorCode.DUCHY_NOT_FOUND -> failGrpc(Status.NOT_FOUND, e) { "Duchy not found" }
        ErrorCode.CERTIFICATE_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND, e) { "Aggregator certificate not found" }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun cancelMeasurement(request: CancelMeasurementRequest): Measurement {
    with(request) {
      grpcRequire(externalMeasurementConsumerId != 0L) {
        "external_measurement_consumer_id not specified"
      }
      grpcRequire(externalMeasurementId != 0L) { "external_measurement_id not specified" }
    }

    val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
    val externalMeasurementId = ExternalId(request.externalMeasurementId)

    try {
      return CancelMeasurement(externalMeasurementConsumerId, externalMeasurementId)
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      val status =
        when (e.code) {
          ErrorCode.MEASUREMENT_NOT_FOUND -> Status.NOT_FOUND
          ErrorCode.MEASUREMENT_STATE_ILLEGAL -> Status.FAILED_PRECONDITION
          ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
          ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
          ErrorCode.ACCOUNT_NOT_FOUND,
          ErrorCode.API_KEY_NOT_FOUND,
          ErrorCode.PERMISSION_DENIED,
          ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
          ErrorCode.DATA_PROVIDER_NOT_FOUND,
          ErrorCode.MODEL_PROVIDER_NOT_FOUND,
          ErrorCode.DUCHY_NOT_FOUND,
          ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
          ErrorCode.CERTIFICATE_NOT_FOUND,
          ErrorCode.CERTIFICATE_IS_INVALID,
          ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
          ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
          ErrorCode.REQUISITION_NOT_FOUND,
          ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
          ErrorCode.REQUISITION_STATE_ILLEGAL,
          ErrorCode.EVENT_GROUP_INVALID_ARGS,
          ErrorCode.EVENT_GROUP_NOT_FOUND,
          ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
          ErrorCode.UNKNOWN_ERROR,
          ErrorCode.UNRECOGNIZED -> throw e
        }
      throw status.withCause(e).asRuntimeException()
    }
  }
}
