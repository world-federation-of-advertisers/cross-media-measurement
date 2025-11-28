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

import com.google.rpc.errorInfo
import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import org.wfanet.measurement.common.grpc.Errors
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLineKey
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.Requisition

/**
 * [Exception] type for known Kingdom internal API service errors.
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#2187): Move out of `spanner`
 *   package, since it's not Spanner-specific.
 */
sealed class KingdomInternalException : Exception {
  val code: ErrorCode
  protected abstract val context: Map<String, String>

  constructor(code: ErrorCode, message: String, cause: Throwable? = null) : super(message, cause) {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : this(code, message = buildMessage())

  fun asStatusRuntimeException(
    statusCode: Status.Code,
    message: String = this.message!!,
  ): StatusRuntimeException {
    val errorInfo = errorInfo {
      reason = this@KingdomInternalException.code.toString()
      domain = DOMAIN
      metadata.putAll(context)
    }
    return Errors.buildStatusRuntimeException(statusCode, message, errorInfo, this)
  }

  override fun toString(): String {
    return super.toString() + " " + context.toString()
  }

  companion object {
    val DOMAIN = ErrorCode.getDescriptor().fullName
  }
}

class RequiredFieldNotSetException(
  val fieldName: String,
  provideDescription: (fieldName: String) -> String = { "Required field $fieldName not set" },
) : KingdomInternalException(ErrorCode.REQUIRED_FIELD_NOT_SET, provideDescription(fieldName)) {
  override val context: Map<String, String>
    get() = mapOf("field_name" to fieldName)
}

class InvalidFieldValueException(
  val fieldName: String,
  provideDescription: (fieldName: String) -> String = { "Value of $fieldName is invalid" },
) : KingdomInternalException(ErrorCode.INVALID_FIELD_VALUE, provideDescription(fieldName)) {
  override val context: Map<String, String>
    get() = mapOf("field_name" to fieldName)
}

class MeasurementConsumerNotFoundException(
  val externalMeasurementConsumerId: ExternalId,
  provideDescription: () -> String = { "MeasurementConsumer not found" },
) : KingdomInternalException(ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf("external_measurement_consumer_id" to externalMeasurementConsumerId.value.toString())
}

class ModelSuiteNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  provideDescription: () -> String = { "ModelSuite not found" },
) : KingdomInternalException(ErrorCode.MODEL_SUITE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
      )
}

class ModelLineNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
) :
  KingdomInternalException(
    ErrorCode.MODEL_LINE_NOT_FOUND,
    "ModelLine with key ($externalModelProviderId, $externalModelSuiteId, $externalModelLineId) " +
      "not found",
  ) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
      )
}

class ModelLineTypeIllegalException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val type: ModelLine.Type,
  message: String =
    "ModelLine with external key (${externalModelLineId.value}, ${externalModelSuiteId.value}, " +
      "${externalModelLineId.value}) has type $type which is illegal for the operation",
) : KingdomInternalException(ErrorCode.MODEL_LINE_TYPE_ILLEGAL, message) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
        "model_line_type" to type.toString(),
      )
}

class ModelLineInvalidArgsException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  message: String,
) : KingdomInternalException(ErrorCode.MODEL_LINE_INVALID_ARGS, message) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
      )
}

class ModelLineNotActiveException
private constructor(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val activeRange: OpenEndRange<Instant>,
  override val context: Map<String, String>,
) : KingdomInternalException(ErrorCode.MODEL_LINE_NOT_ACTIVE, buildMessage(context)) {

  constructor(
    externalModelLineKey: ModelLineKey,
    activeRange: OpenEndRange<Instant>,
  ) : this(
    ExternalId(externalModelLineKey.externalModelProviderId),
    ExternalId(externalModelLineKey.externalModelSuiteId),
    ExternalId(externalModelLineKey.externalModelLineId),
    activeRange,
    buildContext(externalModelLineKey, activeRange),
  )

  companion object {
    private fun buildContext(
      externalModelLineKey: ModelLineKey,
      activeRange: OpenEndRange<Instant>,
    ): Map<String, String> {
      return mapOf(
        "external_model_provider_id" to externalModelLineKey.externalModelProviderId.toString(),
        "external_model_suite_id" to externalModelLineKey.externalModelSuiteId.toString(),
        "external_model_line_id" to externalModelLineKey.externalModelLineId.toString(),
        "active_start_time" to activeRange.start.toString(),
        "active_end_time" to activeRange.endExclusive.toString(),
      )
    }

    private fun buildMessage(context: Map<String, String>): String {
      val externalKey =
        "(${context.getValue("external_model_provider_id")}, " +
          "${context.getValue("external_model_suite_id")}, " +
          "${context.getValue("external_model_line_id")})"
      val activeRange =
        "[${context.getValue("active_start_time")}, ${context.getValue("active_end_time")})"
      return "ModelLine with external key $externalKey not active outside of range $activeRange"
    }
  }
}

class ModelReleaseNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelReleaseId: ExternalId,
  provideDescription: () -> String = { "ModelRelease not found" },
) : KingdomInternalException(ErrorCode.MODEL_RELEASE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_release_id" to externalModelReleaseId.value.toString(),
      )
}

class ModelRolloutNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelRolloutId: ExternalId? = null,
  provideDescription: () -> String = { "ModelRollout not found" },
) : KingdomInternalException(ErrorCode.MODEL_ROLLOUT_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
        "external_model_rollout_id" to externalModelRolloutId?.value.toString(),
      )
}

class ModelRolloutOlderThanPreviousException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val previousExternalModelRolloutId: ExternalId,
  message: String = "Rollout period start is older than that of previous ModelRollout for ModelLine",
) : KingdomInternalException(ErrorCode.MODEL_ROLLOUT_OLDER_THAN_PREVIOUS, message) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
        "previous_external_model_rollout_id" to previousExternalModelRolloutId.value.toString(),
      )
}

class ModelRolloutAlreadyStartedException(
  val rolloutPeriodStartTime: Instant,
  message: String = "Rollout already started at $rolloutPeriodStartTime",
) : KingdomInternalException(ErrorCode.MODEL_ROLLOUT_ALREADY_STARTED, message) {
  override val context: Map<String, String>
    get() = mapOf("rolloutPeriodStartTime" to rolloutPeriodStartTime.toString())
}

class ModelRolloutFreezeScheduledException(
  val rolloutFreezeTime: Instant,
  message: String = "Rollout scheduled for freeze at $rolloutFreezeTime",
) : KingdomInternalException(ErrorCode.MODEL_ROLLOUT_FREEZE_SCHEDULED, message) {
  override val context: Map<String, String>
    get() = mapOf("rolloutFreezeTime" to rolloutFreezeTime.toString())
}

class ModelRolloutFreezeTimeOutOfRangeException(
  val rolloutPeriodStartTime: Instant,
  val rolloutPeriodEndTime: Instant,
  message: String =
    "Rollout freeze time outside of rollout period [$rolloutPeriodStartTime, $rolloutPeriodEndTime)",
) : KingdomInternalException(ErrorCode.MODEL_ROLLOUT_FREEZE_TIME_OUT_OF_RANGE, message) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "rolloutPeriodStartTime" to rolloutPeriodStartTime.toString(),
        "rolloutPeriodEndTime" to rolloutPeriodEndTime.toString(),
      )
}

class DataProviderNotFoundException(
  val externalDataProviderId: ExternalId,
  provideDescription: () -> String = { "DataProvider not found" },
) : KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_data_provider_id" to externalDataProviderId.value.toString())
}

class ModelProviderNotFoundException(
  val externalModelProviderId: ExternalId,
  provideDescription: () -> String = { "ModelProvider not found" },
) : KingdomInternalException(ErrorCode.MODEL_PROVIDER_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_model_provider_id" to externalModelProviderId.value.toString())
}

class DuchyNotFoundException(
  val externalDuchyId: String,
  provideDescription: () -> String = { "Duchy not found" },
) : KingdomInternalException(ErrorCode.DUCHY_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_duchy_id" to externalDuchyId)
}

class DuchyNotActiveException(
  val externalDuchyId: String,
  provideDescription: () -> String = {
    "One or more required duchies were inactive at measurement creation time"
  },
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
  provideDescription: () -> String = { "Measurement not found by ComputationId" },
) : MeasurementNotFoundException(provideDescription) {
  override val context
    get() = mapOf("external_computation_id" to externalComputationId.value.toString())
}

class MeasurementNotFoundByMeasurementConsumerException(
  val externalMeasurementConsumerId: ExternalId,
  val externalMeasurementId: ExternalId,
  provideDescription: () -> String = { "Measurement not found by MeasurementConsumerId" },
) : MeasurementNotFoundException(provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.value.toString(),
        "external_measurement_id" to externalMeasurementId.value.toString(),
      )
}

class MeasurementStateIllegalException(
  val externalMeasurementConsumerId: ExternalId,
  val externalMeasurementId: ExternalId,
  val state: Measurement.State,
  provideDescription: () -> String = { "Measurement state illegal" },
) : KingdomInternalException(ErrorCode.MEASUREMENT_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.value.toString(),
        "external_measurement_id" to externalMeasurementId.value.toString(),
        "measurement_state" to state.toString(),
      )
}

class MeasurementEtagMismatchException(
  val requestedMeasurementEtag: String,
  val actualMeasurementEtag: String,
  provideDescription: () -> String = { "Measurement etag mismatch" },
) : KingdomInternalException(ErrorCode.MEASUREMENT_ETAG_MISMATCH, provideDescription) {
  override val context
    get() =
      mapOf(
        "actual_measurement_etag" to actualMeasurementEtag,
        "requested_measurement_etag" to requestedMeasurementEtag,
      )
}

class CertSubjectKeyIdAlreadyExistsException(
  cause: Throwable? = null,
  message: String = "Cert subject key id already exists",
) : KingdomInternalException(ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS, message, cause) {
  override val context
    get() = emptyMap<String, String>()
}

open class CertificateNotFoundException(
  val externalCertificateId: ExternalId,
  provideDescription: () -> String = { "Certificate not found" },
) : KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_certificate_id" to externalCertificateId.value.toString())
}

class DataProviderCertificateNotFoundException(
  val externalDataProviderId: ExternalId,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "DataProvider's Certificate not found" },
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_certificate_id" to externalCertificateId.value.toString(),
      )
}

class MeasurementConsumerCertificateNotFoundException(
  val externalMeasurementConsumerId: ExternalId,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "MeasurementConsumer's Certificate not found" },
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.value.toString(),
        "external_certificate_id" to externalCertificateId.value.toString(),
      )
}

class DuchyCertificateNotFoundException(
  val externalDuchyId: String,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "Duchy's Certificate not found" },
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_duchy_id" to externalDuchyId,
        "external_certificate_id" to externalCertificateId.value.toString(),
      )
}

class ModelProviderCertificateNotFoundException(
  val externalModelProviderId: ExternalId,
  externalCertificateId: ExternalId,
  provideDescription: () -> String = { "ModelProvider's Certificate not found" },
) : CertificateNotFoundException(externalCertificateId, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_certificate_id" to externalCertificateId.value.toString(),
      )
}

class CertificateRevocationStateIllegalException(
  val externalCertificateId: ExternalId,
  val state: Certificate.RevocationState,
  provideDescription: () -> String = { "Certificate revocation state illegal" },
) : KingdomInternalException(ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_certificate_id" to externalCertificateId.value.toString(),
        "certificate_revocation_state" to state.toString(),
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
  provideDescription: () -> String = { "ComputationParticipant state illegal" },
) : KingdomInternalException(ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.value.toString(),
        "external_duchy_id" to externalDuchyId,
        "computation_participant_state" to state.toString(),
      )
}

open class ComputationParticipantNotFoundException(
  provideDescription: () -> String = { "ComputationParticipant not found" }
) : KingdomInternalException(ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ComputationParticipantETagMismatchException(
  val requestETag: String,
  val actualETag: String,
  message: String =
    "ComputationParticipant etag mismatch. Expected ${requestETag}, actual ${actualETag}",
) : KingdomInternalException(ErrorCode.COMPUTATION_PARTICIPANT_ETAG_MISMATCH, message) {
  override val context
    get() = mapOf("actual_etag" to actualETag, "request_etag" to requestETag)
}

class ComputationParticipantNotFoundByComputationException(
  val externalComputationId: ExternalId,
  val externalDuchyId: String,
  provideDescription: () -> String = { "ComputationParticipant not found by ComputationId" },
) : ComputationParticipantNotFoundException(provideDescription) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.value.toString(),
        "external_duchy_id" to externalDuchyId,
      )
}

class ComputationParticipantNotFoundByMeasurementException(
  val internalMeasurementConsumerId: InternalId,
  val internalMeasurementId: InternalId,
  val internalDuchyId: InternalId,
  provideDescription: () -> String = { "ComputationParticipant not found by MeasurementId" },
) : ComputationParticipantNotFoundException(provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

abstract class RequisitionNotFoundException(
  val externalRequisitionId: ExternalId,
  message: String,
  cause: Throwable?,
) : KingdomInternalException(ErrorCode.REQUISITION_NOT_FOUND, message, cause)

class RequisitionNotFoundByComputationException(
  val externalComputationId: ExternalId,
  externalRequisitionId: ExternalId,
  message: String =
    "Requisition with external Computation ID ${externalComputationId.value} and external " +
      "Requisition ID ${externalRequisitionId.value} not found",
  cause: Throwable? = null,
) : RequisitionNotFoundException(externalRequisitionId, message, cause) {
  override val context
    get() =
      mapOf(
        "external_computation_id" to externalComputationId.value.toString(),
        "external_requisition_id" to externalRequisitionId.value.toString(),
      )
}

class RequisitionNotFoundByDataProviderException(
  val externalDataProviderId: ExternalId,
  externalRequisitionId: ExternalId,
  message: String =
    "Requisition with external DataProvider ID ${externalDataProviderId.value} and external " +
      "Requisition ID ${externalRequisitionId.value} not found",
  cause: Throwable? = null,
) : RequisitionNotFoundException(externalRequisitionId, message, cause) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_requisition_id" to externalRequisitionId.value.toString(),
      )
}

class RequisitionStateIllegalException(
  val externalRequisitionId: ExternalId,
  val state: Requisition.State,
  provideDescription: () -> String = { "Requisition state illegal" },
) : KingdomInternalException(ErrorCode.REQUISITION_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_requisition_id" to externalRequisitionId.value.toString(),
        "requisition_state" to state.toString(),
      )
}

class RequisitionEtagMismatchException(
  val requestedEtag: String,
  val actualEtag: String,
  message: String = "Request etag $requestedEtag does not match actual etag $actualEtag",
) : KingdomInternalException(ErrorCode.REQUISITION_ETAG_MISMATCH, message) {
  override val context
    get() = mapOf("request_etag" to requestedEtag, "actual_etag" to actualEtag)
}

class AccountNotFoundException(
  val externalAccountId: ExternalId,
  provideDescription: () -> String = { "Account not found" },
) : KingdomInternalException(ErrorCode.ACCOUNT_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_account_id" to externalAccountId.value.toString())
}

class DuplicateAccountIdentityException(
  val externalAccountId: ExternalId,
  val issuer: String,
  val subject: String,
  provideDescription: () -> String = { "Duplicated account identity" },
) : KingdomInternalException(ErrorCode.DUPLICATE_ACCOUNT_IDENTITY, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_account_id" to externalAccountId.value.toString(),
        "issuer" to issuer,
        "subject" to subject,
      )
}

class AccountActivationStateIllegalException(
  val externalAccountId: ExternalId,
  val state: Account.ActivationState,
  provideDescription: () -> String = { "Account activation state illegal" },
) : KingdomInternalException(ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_account_id" to externalAccountId.value.toString(),
        "account_activation_state" to state.toString(),
      )
}

class PermissionDeniedException(provideDescription: () -> String = { "Permission Denied" }) :
  KingdomInternalException(ErrorCode.PERMISSION_DENIED, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ApiKeyNotFoundException(
  val externalApiKeyId: ExternalId,
  provideDescription: () -> String = { "ApiKey not found" },
) : KingdomInternalException(ErrorCode.API_KEY_NOT_FOUND, provideDescription) {
  override val context
    get() = mapOf("external_api_key_id" to externalApiKeyId.value.toString())
}

class EventGroupNotFoundException(
  val externalDataProviderId: ExternalId,
  val externalEventGroupId: ExternalId,
  provideDescription: () -> String = { "EventGroup not found" },
) : KingdomInternalException(ErrorCode.EVENT_GROUP_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_event_group_id" to externalEventGroupId.value.toString(),
      )
}

class EventGroupNotFoundByMeasurementConsumerException(
  val externalMeasurementConsumerId: ExternalId,
  val externalEventGroupId: ExternalId,
  provideDescription: () -> String = { "EventGroup not found" },
) : KingdomInternalException(ErrorCode.EVENT_GROUP_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_measurement_consumer_id" to externalMeasurementConsumerId.value.toString(),
        "external_event_group_id" to externalEventGroupId.value.toString(),
      )
}

class EventGroupInvalidArgsException(
  val originalExternalMeasurementConsumerId: ExternalId,
  val providedExternalMeasurementConsumerId: ExternalId,
  provideDescription: () -> String = { "EventGroup invalid arguments" },
) : KingdomInternalException(ErrorCode.EVENT_GROUP_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "original_external_measurement_consumer_id" to
          originalExternalMeasurementConsumerId.value.toString(),
        "provided_external_measurement_consumer_id" to
          providedExternalMeasurementConsumerId.value.toString(),
      )
}

class EventGroupStateIllegalException(
  val externalDataProviderId: ExternalId,
  val externalEventGroupId: ExternalId,
  val state: EventGroup.State,
  provideDescription: () -> String = { "EventGroup state is $state" },
) : KingdomInternalException(ErrorCode.EVENT_GROUP_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_event_group_id" to externalEventGroupId.value.toString(),
        "event_group_state" to state.toString(),
      )
}

class EventGroupMetadataDescriptorNotFoundException(
  val externalDataProviderId: ExternalId,
  val externalEventGroupMetadataDescriptorId: ExternalId,
  provideDescription: () -> String = { "EventGroup metadata descriptor not found" },
) :
  KingdomInternalException(
    ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
    provideDescription,
  ) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_event_group_metadata_descriptor_id" to
          externalEventGroupMetadataDescriptorId.value.toString(),
      )
}

class EventGroupMetadataDescriptorAlreadyExistsWithTypeException(
  message: String = "EventGroupMetadataDescriptor with same protobuf type already exists",
  cause: Throwable? = null,
) :
  KingdomInternalException(
    ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_ALREADY_EXISTS_WITH_TYPE,
    message,
    cause,
  ) {
  override val context: Map<String, String>
    get() = emptyMap()
}

class RecurringExchangeNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  provideDescription: () -> String = { "RecurringExchange not found" },
) : KingdomInternalException(ErrorCode.RECURRING_EXCHANGE_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() = mapOf("external_recurring_exchange_id" to externalRecurringExchangeId.value.toString())
}

class ExchangeStepAttemptNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  val date: Date,
  val stepIndex: Int,
  val attemptNumber: Int,
  provideDescription: () -> String = { "ExchangeStepAttempt not found" },
) : KingdomInternalException(ErrorCode.EXCHANGE_STEP_ATTEMPT_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "external_recurring_exchange_id" to externalRecurringExchangeId.value.toString(),
        "date" to date.toLocalDate().toString(),
        "step_index" to stepIndex.toString(),
        "attempt_number" to attemptNumber.toString(),
      )
}

class ExchangeStepNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  val date: Date,
  val stepIndex: Int,
  provideDescription: () -> String = { "ExchangeStep not found" },
) : KingdomInternalException(ErrorCode.EXCHANGE_STEP_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "external_recurring_exchange_id" to externalRecurringExchangeId.value.toString(),
        "date" to date.toLocalDate().toString(),
        "step_index" to stepIndex.toString(),
      )
}

class ModelOutageNotFoundException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelOutageId: ExternalId,
  provideDescription: () -> String = { "ModelOutage not found" },
) : KingdomInternalException(ErrorCode.MODEL_OUTAGE_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
        "external_model_outage_id" to externalModelOutageId.value.toString(),
      )
}

class ModelOutageStateIllegalException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelOutageId: ExternalId,
  val state: ModelOutage.State,
  provideDescription: () -> String = { "ModelOutage state illegal" },
) : KingdomInternalException(ErrorCode.MODEL_OUTAGE_STATE_ILLEGAL, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
        "external_model_outage_id" to externalModelOutageId.value.toString(),
        "model_outage_state" to state.toString(),
      )
}

class ModelOutageInvalidArgsException(
  val externalModelProviderId: ExternalId,
  val externalModelSuiteId: ExternalId,
  val externalModelLineId: ExternalId,
  val externalModelOutageId: ExternalId? = null,
  provideDescription: () -> String = { "ModelOutage invalid outage interval arguments" },
) : KingdomInternalException(ErrorCode.MODEL_OUTAGE_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_model_provider_id" to externalModelProviderId.value.toString(),
        "external_model_suite_id" to externalModelSuiteId.value.toString(),
        "external_model_line_id" to externalModelLineId.value.toString(),
        "external_model_outage_id" to externalModelOutageId?.value.toString(),
      )
}

class ModelShardNotFoundException(
  val externalDataProviderId: ExternalId,
  val externalModelShardId: ExternalId,
  provideDescription: () -> String = { "ModelShard not found" },
) : KingdomInternalException(ErrorCode.MODEL_SHARD_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_model_shard_id" to externalModelShardId.value.toString(),
      )
}

class ModelShardInvalidArgsException(
  val externalDataProviderId: ExternalId,
  val externalModelShardId: ExternalId,
  val externalModelProviderId: ExternalId,
  provideDescription: () -> String = { "ModelShard invalid arguments" },
) : KingdomInternalException(ErrorCode.MODEL_SHARD_INVALID_ARGS, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_model_shard_id" to externalModelShardId.value.toString(),
        "external_model_provider_id" to externalModelProviderId.value.toString(),
      )
}

class ExchangeNotFoundException(
  val externalRecurringExchangeId: ExternalId,
  val date: Date,
  provideDescription: () -> String = { "Exchange not found" },
) : KingdomInternalException(ErrorCode.EXCHANGE_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "external_recurring_exchange_id" to externalRecurringExchangeId.value.toString(),
        "date" to date.toLocalDate().toString(),
      )
}

class PopulationNotFoundException(
  val externalDataProviderId: ExternalId,
  val externalPopulationId: ExternalId,
  provideDescription: () -> String = { "Population not found" },
) : KingdomInternalException(ErrorCode.POPULATION_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf(
        "external_data_provider_id" to externalDataProviderId.value.toString(),
        "external_population_id" to externalPopulationId.value.toString(),
      )
}
