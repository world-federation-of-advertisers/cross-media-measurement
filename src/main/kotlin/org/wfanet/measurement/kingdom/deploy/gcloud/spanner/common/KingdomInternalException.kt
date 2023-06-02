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

import com.google.protobuf.Any
import com.google.rpc.errorInfo
import com.google.rpc.status
import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.Requisition

sealed class KingdomInternalException : Exception {
  val code: ErrorCode
  protected abstract val context: Map<String, String>

  constructor(code: ErrorCode) : super() {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
  }

  fun asStatusRuntimeException(
    statusCode: Status.Code,
    message: String = this.message!!
  ): StatusRuntimeException {
    val statusProto = status {
      code = statusCode.value()
      this.message = message
      details +=
        Any.pack(
          errorInfo {
            reason = this@KingdomInternalException.code.toString()
            domain = ErrorCode.getDescriptor().fullName
            metadata.putAll(context)
          }
        )
    }

    // Unpack exception to add cause.
    // TODO(grpc/grpc-java#10230): Use new API when available.
    val exception = StatusProto.toStatusRuntimeException(statusProto)
    return exception.status.withCause(this).asRuntimeException(exception.trailers)
  }

  override fun toString(): String {
    return super.toString() + " " + context.toString()
  }
}

class MeasurementConsumerNotFoundException(
  val externalMeasurementConsumerId: ExternalId,
  provideDescription: () -> String = { "MeasurementConsumer not found" }
) : KingdomInternalException(ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_measurement_consumer_id" to externalMeasurementConsumerId.toString())
}

class ModelSuiteNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  provideDescription: () -> String = { "ModelSuite not found" }
) : KingdomInternalException(ErrorCode.MODEL_SUITE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString()
      )
}

class ModelLineNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  provideDescription: () -> String = { "ModelLine not found" }
) : KingdomInternalException(ErrorCode.MODEL_LINE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString()
      )
}

class ModelLineTypeIllegalException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val type: ModelLine.Type,
  provideDescription: () -> String = { "ModelLine type illegal" }
) : KingdomInternalException(ErrorCode.MODEL_LINE_TYPE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString(),
        "model_line_type" to type.toString()
      )
}

class ModelLineInvalidArgsException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId? = null,
  provideDescription: () -> String = { "ModelLine invalid active time arguments" }
) : KingdomInternalException(ErrorCode.MODEL_LINE_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString()
      )
}

class ModelReleaseNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelReleaseId: ExternalId,
  provideDescription: () -> String = { "ModelRelease not found" }
) : KingdomInternalException(ErrorCode.MODEL_RELEASE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_release_id" to externalModelReleaseId.toString(),
      )
}

class ModelRolloutNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelRolloutId: ExternalId? = null,
  provideDescription: () -> String = { "ModelRollout not found" }
) : KingdomInternalException(ErrorCode.MODEL_ROLLOUT_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString(),
        "external_model_rollout_id" to externalModelRolloutId.toString()
      )
}

class ModelRolloutInvalidArgsException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelRolloutId: ExternalId? = null,
  provideDescription: () -> String = { "ModelRollout invalid rollout period time arguments" }
) : KingdomInternalException(ErrorCode.MODEL_ROLLOUT_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString(),
        "external_model_rollout_i     d" to externalModelRolloutId.toString()
      )
}

class DataProviderNotFoundException(
  val externalDataProviderId: ExternalId,
  provideDescription: () -> String = { "DataProvider not found" }
) : KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_data_provider_id" to externalDataProviderId.toString())
}

class ModelProviderNotFoundException(
  val externalModelProviderId: ExternalId,
  provideDescription: () -> String = { "ModelProvider not found" }
) : KingdomInternalException(ErrorCode.MODEL_PROVIDER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_model_provider_id" to externalModelProviderId.toString())
}

class DuchyNotFoundException(
  val externalDuchyId: String,
  provideDescription: () -> String = { "Duchy not found" }
) : KingdomInternalException(ErrorCode.DUCHY_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_duchy_id" to externalDuchyId)
}

class DuchyNotActiveException(
  val externalDuchyId: String,
  provideDescription: () -> String = {
    "One or more required duchies were inactive at measurement creation time"
  }
) : KingdomInternalException(ErrorCode.DUCHY_NOT_ACTIVE, provideDescription) {
  override val context
    get() = mapOf("external_duchy_id" to externalDuchyId)
}

open class MeasurementNotFoundException(
  provideDescription: () -> String = { "Measurement not found" }
) : KingdomInternalException(ErrorCode.MEASUREMENT_NOT_FOUND, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementNotFoundByComputationException(
  val externalComputationId: ExternalId,
  provideDescription: () -> String = { "Measurement not found by ComputationId" }
) : MeasurementNotFoundException(provideDescription) {
  override val context
    get() = mapOf("external_computation_id" to externalComputationId.toString())
}

class MeasurementNotFoundByMeasurementConsumerException(
  val externalMeasurementConsumerId: ExternalId,
  val externalMeasurementId: ExternalId,
  provideDescription: () -> String = { "Measurement not found by MeasurementConsumerId" }
) : MeasurementNotFoundException(provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.toString(),
        "external_measurement_id" to externalMeasurementId.toString()
      )
}

class MeasurementStateIllegalException(
  val externalMeasurementConsumerId: ExternalId,
  val externalMeasurementId: ExternalId,
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

class MeasurementEtagMismatchException(
  val requestedMeasurementEtag: String,
  val actualMeasurementEtag: String,
  provideDescription: () -> String = { "Measurement etag mismatch" }
) : KingdomInternalException(ErrorCode.MEASUREMENT_ETAG_MISMATCH, provideDescription) {
  override val context
    get() =
      mapOf(
        "actual_measurement_etag" to actualMeasurementEtag,
        "requested_measurement_etag" to requestedMeasurementEtag
      )
}

class CertSubjectKeyIdAlreadyExistsException(
  provideDescription: () -> String = { "Cert subject key id already exists" }
) : KingdomInternalException(ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

open class CertificateNotFoundException(
  val externalCertificateId: ExternalId,
  provideDescription: () -> String = { "Certificate not found" }
) : KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_certificate_id" to externalCertificateId.toString())
}

class DataProviderCertificateNotFoundException(
  val externalDataProviderId: ExternalId,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "DataProvider's Certificate not found" }
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class MeasurementConsumerCertificateNotFoundException(
  val externalMeasurementConsumerId: ExternalId,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "MeasurementConsumer's Certificate not found" }
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class DuchyCertificateNotFoundException(
  val externalDuchyId: String,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "Duchy's Certificate not found" }
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_duchy_id" to externalDuchyId,
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class ModelProviderCertificateNotFoundException(
  val externalModelProviderId: ExternalId,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "ModelProvider's Certificate not found" }
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_certificate_id" to externalCertificateId.toString()
      )
}

class CertificateRevocationStateIllegalException(
  val externalCertificateId: ExternalId,
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

class CertificateIsInvalidException(
  provideDescription: () -> String = { "Certificate is invalid" }
) : KingdomInternalException(ErrorCode.CERTIFICATE_IS_INVALID, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ComputationParticipantStateIllegalException(
  val externalComputationId: ExternalId,
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

open class ComputationParticipantNotFoundException(
  provideDescription: () -> String = { "ComputationParticipant not found" }
) : KingdomInternalException(ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ComputationParticipantNotFoundByComputationException(
  val externalComputationId: ExternalId,
  val externalDuchyId: String,
  provideDescription: () -> String = { "ComputationParticipant not found by ComputationId" }
) : ComputationParticipantNotFoundException(provideDescription) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.toString(),
        "external_duchy_id" to externalDuchyId
      )
}

class ComputationParticipantNotFoundByMeasurementException(
  val internalMeasurementConsumerId: InternalId,
  val internalMeasurementId: InternalId,
  val internalDuchyId: InternalId,
  provideDescription: () -> String = { "ComputationParticipant not found by MeasurementId" }
) : ComputationParticipantNotFoundException(provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

open class RequisitionNotFoundException(
  val externalRequisitionId: ExternalId,
  provideDescription: () -> String = { "Requisition not found" }
) : KingdomInternalException(ErrorCode.REQUISITION_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_requisition_id" to externalRequisitionId.toString())
}

class RequisitionNotFoundByComputationException(
  val externalComputationId: ExternalId,
  externalRequisitionId: ExternalId,
  provideDescription: () -> String = { "Requisition not found by Computation" }
) : RequisitionNotFoundException(externalRequisitionId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.toString(),
        "external_requisition_id" to externalRequisitionId.toString()
      )
}

class RequisitionNotFoundByDataProviderException(
  val externalDataProviderId: ExternalId,
  externalRequisitionId: ExternalId,
  provideDescription: () -> String = { "Requisition not found by DataProvider" }
) : RequisitionNotFoundException(externalRequisitionId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_requisition_id" to externalRequisitionId.toString()
      )
}

class RequisitionStateIllegalException(
  val externalRequisitionId: ExternalId,
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

class AccountNotFoundException(
  val externalAccountId: ExternalId,
  provideDescription: () -> String = { "Account not found" }
) : KingdomInternalException(ErrorCode.ACCOUNT_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_account_id" to externalAccountId.toString())
}

class DuplicateAccountIdentityException(
  val externalAccountId: ExternalId,
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

class AccountActivationStateIllegalException(
  val externalAccountId: ExternalId,
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

class PermissionDeniedException(provideDescription: () -> String = { "Permission Denied" }) :
  KingdomInternalException(ErrorCode.PERMISSION_DENIED, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ApiKeyNotFoundException(
  val externalApiKeyId: ExternalId,
  provideDescription: () -> String = { "ApiKey not found" }
) : KingdomInternalException(ErrorCode.API_KEY_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_api_key_id" to externalApiKeyId.toString())
}

class EventGroupNotFoundException(
  val externalDataProviderId: ExternalId,
  val externalEventGroupId: ExternalId,
  provideDescription: () -> String = { "EventGroup not found" }
) : KingdomInternalException(ErrorCode.EVENT_GROUP_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_event_group_id" to externalEventGroupId.toString()
      )
}

class EventGroupInvalidArgsException(
  val originalExternalMeasurementId: ExternalId,
  val providedExternalMeasurementId: ExternalId,
  provideDescription: () -> String = { "EventGroup invalid arguments" }
) : KingdomInternalException(ErrorCode.EVENT_GROUP_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "original_external_measurement_id" to originalExternalMeasurementId.toString(),
        "provided_external_measurement_id" to providedExternalMeasurementId.toString()
      )
}

class EventGroupStateIllegalException(
  val externalDataProviderId: ExternalId,
  val externalEventGroupId: ExternalId,
  val state: EventGroup.State,
  provideDescription: () -> String = { "EventGroup state illegal" }
) : KingdomInternalException(ErrorCode.EVENT_GROUP_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_event_group_id" to externalEventGroupId.toString(),
        "event_group_state" to state.toString()
      )
}

class EventGroupMetadataDescriptorNotFoundException(
  val externalDataProviderId: ExternalId,
  val externalEventGroupMetadataDescriptorId: ExternalId,
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

class RecurringExchangeNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  provideDescription: () -> String = { "RecurringExchange not found" }
) : KingdomInternalException(ErrorCode.RECURRING_EXCHANGE_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() = mapOf("external_recurring_exchange_id" to externalRecurringExchangeId.value.toString())
}

class ExchangeStepAttemptNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  val date: Date,
  val stepIndex: Int,
  val attemptNumber: Int,
  provideDescription: () -> String = { "ExchangeStepAttempt not found" }
) : KingdomInternalException(ErrorCode.EXCHANGE_STEP_ATTEMPT_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "external_recurring_exchange_id" to externalRecurringExchangeId.value.toString(),
        "date" to date.toString(),
        "step_index" to stepIndex.toString(),
        "attempt_number" to attemptNumber.toString()
      )
}

class ExchangeStepNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  val date: Date,
  val stepIndex: Int,
  provideDescription: () -> String = { "ExchangeStep not found" }
) : KingdomInternalException(ErrorCode.EXCHANGE_STEP_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "external_recurring_exchange_id" to externalRecurringExchangeId.value.toString(),
        "date" to date.toString(),
        "step_index" to stepIndex.toString(),
      )
}

class ModelOutageNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelOutageId: ExternalId,
  provideDescription: () -> String = { "ModelOutage not found" }
) : KingdomInternalException(ErrorCode.MODEL_OUTAGE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString(),
        "external_model_outage_id" to externalModelOutageId.toString()
      )
}

class ModelOutageStateIllegalException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelOutageId: ExternalId,
  val state: ModelOutage.State,
  provideDescription: () -> String = { "ModelOutage state illegal" }
) : KingdomInternalException(ErrorCode.MODEL_OUTAGE_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString(),
        "external_model_outage_id" to externalModelOutageId.toString(),
        "model_outage_state" to state.toString()
      )
}

class ModelOutageInvalidArgsException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelOutageId: ExternalId? = null,
  provideDescription: () -> String = { "ModelOutage invalid outage interval arguments" }
) : KingdomInternalException(ErrorCode.MODEL_OUTAGE_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineId.toString(),
        "external_model_outage_id" to externalModelOutageId.toString()
      )
}

class ModelShardNotFoundException(
  val externalDataProviderId: ExternalId,
  val externalModelShardId: ExternalId,
  provideDescription: () -> String = { "ModelShard not found" }
) : KingdomInternalException(ErrorCode.MODEL_SHARD_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.toString(),
        "external_model_shard_id" to externalModelShardId.toString()
      )
}

class ExchangeNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  val date: Date,
  provideDescription: () -> String = { "Exchange not found" }
) : KingdomInternalException(ErrorCode.EXCHANGE_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "external_recurring_exchange_id" to externalRecurringExchangeId.value.toString(),
        "date" to date.toString(),
      )
}
