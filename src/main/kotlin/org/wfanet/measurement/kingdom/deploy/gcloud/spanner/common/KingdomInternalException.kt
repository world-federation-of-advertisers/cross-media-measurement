// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.ProtoUtils
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition

sealed class KingdomInternalException : Exception {
  val code: ErrorCode
  abstract val context: Map<String, String>

  constructor(code: ErrorCode) : super() {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
  }

  fun throwStatusRuntimeException(
    status: Status = Status.INVALID_ARGUMENT,
    provideDescription: () -> String,
  ): Nothing = throwStatusRuntimeException(status, code, context, provideDescription)
}

private fun throwStatusRuntimeException(
  status: Status = Status.INVALID_ARGUMENT,
  code: ErrorCode,
  context: Map<String, String>,
  provideDescription: () -> String,
): Nothing {
  val info = errorInfo {
    reason = code.toString()
    domain = ErrorInfo::class.qualifiedName.toString()
    metadata.putAll(context)
  }

  val metadata = Metadata()
  metadata.put(ProtoUtils.keyForProto(info), info)

  throw status.withDescription(provideDescription()).asRuntimeException(metadata)
}

fun StatusRuntimeException.getErrorInfo(): ErrorInfo? {
  val key = ProtoUtils.keyForProto(ErrorInfo.getDefaultInstance())
  return trailers?.get(key)
}

class MeasurementConsumerNotFound(
  val externalMeasurementConsumerId: Long,
  provideDescription: () -> String = { "MeasurementConsumer not found" }
) : KingdomInternalException(ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_measurement_consumer_id" to externalMeasurementConsumerId.toString())
}

class DataProviderNotFound(
  val externalDataProviderId: Long,
  provideDescription: () -> String = { "DataProvider not found" }
) : KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_data_provider_id" to externalDataProviderId.toString())
}

class ModelProviderNotFound(
  val externalModelProviderId: Long,
  provideDescription: () -> String = { "ModelProvider not found" }
) : KingdomInternalException(ErrorCode.MODEL_PROVIDER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_model_provider_id" to externalModelProviderId.toString())
}

class DuchyNotFound(
  val externalDuchyId: String,
  provideDescription: () -> String = { "Duchy not found" }
) : KingdomInternalException(ErrorCode.DUCHY_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_duchy_id" to externalDuchyId)
}

class MeasurementNotFoundByComputation(
  val externalComputationId: Long,
  provideDescription: () -> String = { "Measurement not found by ComputationId" }
) : KingdomInternalException(ErrorCode.MEASUREMENT_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_computation_id" to externalComputationId.toString())
}

class MeasurementNotFoundByMeasurementConsumer(
  val externalMeasurementConsumerId: Long,
  val externalMeasurementId: Long,
  provideDescription: () -> String = { "Measurement not found by MeasurementConsumerId" }
) : KingdomInternalException(ErrorCode.MEASUREMENT_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.toString(),
        "external_measurement_id" to externalMeasurementId.toString()
      )
}

class MeasurementStateIllegal(
  val externalMeasurementConsumerId: Long,
  val externalMeasurementId: Long,
  val state: Measurement.State,
  provideDescription: () -> String = { "Measurement state illegal" }
) : KingdomInternalException(ErrorCode.MEASUREMENT_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.toString(),
        "external_measurement_id" to externalMeasurementId.toString(),
        "measurement_state" to state.toString()
      )
}

class CertSubjectKeyIdAlreadyExists(
  provideDescription: () -> String = { "Cert subject key id already exists" }
) : KingdomInternalException(ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class DataProviderCertificateNotFoundByExternal(
  val externalDataProviderId: Long,
  val externalCertificateId: Long,
  provideDescription: () -> String = { "DataProvider's Certificate not found by external id" }
) : KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class DataProviderCertificateNotFoundByInternal(
  val internalDataProviderId: Long,
  val externalCertificateId: Long,
  provideDescription: () -> String = { "DataProvider's Certificate not found by internal id" }
) : KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "internal_data_provider_id" to internalDataProviderId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class MeasurementConsumerCertificateNotFoundByExternal(
  val externalMeasurementConsumerId: Long,
  val externalCertificateId: Long,
  provideDescription: () -> String = {
    "MeasurementConsumer's Certificate not found by external id"
  }
) : KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class MeasurementConsumerCertificateNotFoundByInternal(
  val internalMeasurementConsumerId: Long,
  val externalCertificateId: Long,
  provideDescription: () -> String = {
    "MeasurementConsumer's Certificate not found by internal id"
  }
) : KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "internal_measurement_consumer_id" to internalMeasurementConsumerId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class DuchyCertificateNotFound(
  val internalDuchyId: Long,
  val externalCertificateId: Long,
  provideDescription: () -> String = { "Duchy's Certificate not found" }
) : KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "internal_duchy_id" to internalDuchyId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class CertificateRevocationStateIllegal(
  val externalCertificateId: Long,
  val state: Certificate.RevocationState,
  provideDescription: () -> String = { "Certificate revocation state illegal" }
) : KingdomInternalException(ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_certificate_id" to externalCertificateId.toString(),
        "certificate_revocation_state" to state.toString()
      )
}

class CertificateIsInvalid(provideDescription: () -> String = { "Certificate is invalid" }) :
  KingdomInternalException(ErrorCode.CERTIFICATE_IS_INVALID, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ComputationParticipantStateIllegal(
  val externalComputationId: Long,
  val externalDuchyId: String,
  val state: ComputationParticipant.State,
  provideDescription: () -> String = { "ComputationParticipant state illegal" }
) : KingdomInternalException(ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.toString(),
        "external_duchy_id" to externalDuchyId,
        "computation_participant_state" to state.toString()
      )
}

class ComputationParticipantNotFoundByComputation(
  val externalComputationId: Long,
  val externalDuchyId: String,
  provideDescription: () -> String = { "ComputationParticipant not found by ComputationId" }
) : KingdomInternalException(ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.toString(),
        "external_duchy_id" to externalDuchyId
      )
}

class ComputationParticipantNotFoundByMeasurement(
  val internalMeasurementConsumerId: Long,
  val internalMeasurementId: Long,
  val internalDuchyId: Long,
  provideDescription: () -> String = { "ComputationParticipant not found by MeasurementId" }
) : KingdomInternalException(ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "internal_measurement_consumer_id" to internalMeasurementConsumerId.toString(),
        "internal_measurement_id" to internalMeasurementId.toString(),
        "internal_duchy_id" to internalDuchyId.toString()
      )
}

class RequisitionNotFoundByComputation(
  val externalComputationId: Long,
  val externalRequisitionId: Long,
  provideDescription: () -> String = { "Requisition not found by Computation" }
) : KingdomInternalException(ErrorCode.REQUISITION_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.toString(),
        "external_requisition_id" to externalRequisitionId.toString()
      )
}

class RequisitionNotFoundByDataProvider(
  val externalDataProviderId: Long,
  val externalRequisitionId: Long,
  provideDescription: () -> String = { "Requisition not found by DataProvider" }
) : KingdomInternalException(ErrorCode.REQUISITION_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_requisition_id" to externalRequisitionId.toString()
      )
}

class RequisitionStateIllegal(
  val externalRequisitionId: Long,
  val state: Requisition.State,
  provideDescription: () -> String = { "ComputationParticipant state illegal" }
) : KingdomInternalException(ErrorCode.REQUISITION_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_requisition_id" to externalRequisitionId.toString(),
        "requisition_state" to state.toString()
      )
}

class AccountNotFound(
  val externalAccountId: Long,
  provideDescription: () -> String = { "Account not found" }
) : KingdomInternalException(ErrorCode.ACCOUNT_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_account_id" to externalAccountId.toString())
}

class DuplicateAccountIdentity(
  val externalAccountId: Long,
  val issuer: String,
  val subject: String,
  provideDescription: () -> String = { "Duplicated account identity" }
) : KingdomInternalException(ErrorCode.DUPLICATE_ACCOUNT_IDENTITY, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_account_id" to externalAccountId.toString(),
        "issuer" to issuer,
        "subject" to subject
      )
}

class AccountActivationStateIllegal(
  val externalAccountId: Long,
  val state: Account.ActivationState,
  provideDescription: () -> String = { "Account activation state illegal" }
) : KingdomInternalException(ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_account_id" to externalAccountId.toString(),
        "account_activation_state" to state.toString()
      )
}

class PermissionDenied(provideDescription: () -> String = { "Permission Denied" }) :
  KingdomInternalException(ErrorCode.PERMISSION_DENIED, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ApiKeyNotFound(
  val externalApiKeyId: Long,
  provideDescription: () -> String = { "ApiKey not found" }
) : KingdomInternalException(ErrorCode.API_KEY_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_api_key_id" to externalApiKeyId.toString())
}

class EventGroupNotFound(
  val externalDataProviderId: Long,
  val externalEventGroupId: Long,
  provideDescription: () -> String = { "EventGroup not found" }
) : KingdomInternalException(ErrorCode.EVENT_GROUP_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_event_group_id" to externalEventGroupId.toString()
      )
}

class EventGroupInvalidArgs(
  val originalExternalMeasurementId: Long,
  val providedExternalMeasurementId: Long,
  provideDescription: () -> String = { "EventGroup invalid arguments" }
) : KingdomInternalException(ErrorCode.EVENT_GROUP_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "original_external_measurement_id" to originalExternalMeasurementId.toString(),
        "provided_external_measurement_id" to providedExternalMeasurementId.toString()
      )
}

class EventGroupMetadataDescriptorNotFound(
  val externalDataProviderId: Long,
  val externalEventGroupMetadataDescriptorId: Long,
  provideDescription: () -> String = { "EventGroup metadata descriptor not found" }
) :
  KingdomInternalException(
    ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
    provideDescription
  ) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_event_group_metadata_descriptor_id" to
          externalEventGroupMetadataDescriptorId.toString()
      )
}
