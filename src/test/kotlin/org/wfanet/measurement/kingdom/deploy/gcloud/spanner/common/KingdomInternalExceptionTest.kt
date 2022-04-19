// Copyright 2022 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition

private const val ERROR_DOMAIN = "com.google.rpc.ErrorInfo"
private const val EXTERNAL_ACCOUNT_ID = 100L
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 101L
private const val INTERNAL_MEASUREMENT_CONSUMER_ID = 102L
private const val EXTERNAL_MEASUREMENT_ID = 103L
private const val EXTERNAL_MEASUREMENT_ID_2 = 1030L
private const val INTERNAL_MEASUREMENT_ID = 104L
private const val EXTERNAL_REQUISITION_ID = 105L
private const val EXTERNAL_DATA_PROVIDER_ID = 106L
private const val EXTERNAL_MODEL_PROVIDER_ID = 107L
private const val EXTERNAL_COMPUTATION_ID = 108L
private const val EXTERNAL_CERTIFICATE_ID = 109L
private const val EXTERNAL_DUCHY_ID = "worker1"
private const val INTERNAL_DUCHY_ID = 110L
private const val ISSUER = "issuer"
private const val SUBJECT = "subject"
private const val EXTERNAL_API_KEY_ID = 111L
private const val EXTERNAL_EVENT_GROUP_ID = 112L
private const val EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID = 113L

@RunWith(JUnit4::class)
class KingdomInternalExceptionTest {

  private fun verifyErrorInfo(
    exception: StatusRuntimeException,
    expectedReason: String,
    expectedMetadata: Map<String, String>,
    message: String = ""
  ) {
    assertThat(exception.message).isEqualTo(message)
    val info = exception.getErrorInfo()
    assertNotNull(info)
    assertThat(info.reason).isEqualTo(expectedReason)
    assertThat(info.domain).isEqualTo(ERROR_DOMAIN)
    val metadata = info.metadataMap
    assertNotNull(metadata)
    assertThat(metadata).isEqualTo(expectedMetadata)
  }

  @Test
  fun `MeasurementConsumerNotFound works  as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = MeasurementConsumerNotFound(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalMeasurementConsumerId)
          .isEqualTo(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "MeasurementConsumer with external ID ${internalException.externalMeasurementConsumerId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_CONSUMER_NOT_FOUND",
      mapOf("external_measurement_consumer_id" to EXTERNAL_MEASUREMENT_CONSUMER_ID.toString()),
      "FAILED_PRECONDITION: MeasurementConsumer with external ID $EXTERNAL_MEASUREMENT_CONSUMER_ID not found"
    )
  }

  @Test
  fun `DataProviderNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = DataProviderNotFound(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "DataProvider with external ID ${internalException.externalDataProviderId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "DATA_PROVIDER_NOT_FOUND",
      mapOf("external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString()),
      "FAILED_PRECONDITION: DataProvider with external ID $EXTERNAL_DATA_PROVIDER_ID not found"
    )
  }

  @Test
  fun `ModelProviderNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = ModelProviderNotFound(EXTERNAL_MODEL_PROVIDER_ID)
        assertThat(internalException.externalModelProviderId).isEqualTo(EXTERNAL_MODEL_PROVIDER_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "ModelProvider with external ID ${internalException.externalModelProviderId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "MODEL_PROVIDER_NOT_FOUND",
      mapOf("external_model_provider_id" to EXTERNAL_MODEL_PROVIDER_ID.toString()),
      "FAILED_PRECONDITION: ModelProvider with external ID $EXTERNAL_MODEL_PROVIDER_ID not found"
    )
  }

  @Test
  fun `DuchyNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = DuchyNotFound(EXTERNAL_DUCHY_ID)
        assertThat(internalException.externalDuchyId).isEqualTo(EXTERNAL_DUCHY_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Duchy with external ID ${internalException.externalDuchyId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "DUCHY_NOT_FOUND",
      mapOf("external_duchy_id" to EXTERNAL_DUCHY_ID),
      "FAILED_PRECONDITION: Duchy with external ID $EXTERNAL_DUCHY_ID not found"
    )
  }

  @Test
  fun `MeasurementNotFoundByComputation with computationId works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = MeasurementNotFoundByComputation(EXTERNAL_COMPUTATION_ID)
        assertThat(internalException.externalComputationId).isEqualTo(EXTERNAL_COMPUTATION_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement with external computation ID ${internalException.externalComputationId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_NOT_FOUND_BY_COMPUTATION",
      mapOf("external_computation_id" to EXTERNAL_COMPUTATION_ID.toString()),
      "FAILED_PRECONDITION: Measurement with external computation ID $EXTERNAL_COMPUTATION_ID not found"
    )
  }

  @Test
  fun `MeasurementNotFoundByMeasurementConsumer works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementNotFoundByMeasurementConsumer(
            EXTERNAL_MEASUREMENT_CONSUMER_ID,
            EXTERNAL_MEASUREMENT_ID
          )
        assertThat(internalException.externalMeasurementConsumerId)
          .isEqualTo(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalMeasurementId).isEqualTo(EXTERNAL_MEASUREMENT_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement with external measurement consumer ID ${internalException.externalMeasurementConsumerId}," +
            "measurement ID ${internalException.externalMeasurementId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_NOT_FOUND_BY_MEASUREMENT_CONSUMER",
      mapOf(
        "external_measurement_consumer_id" to EXTERNAL_MEASUREMENT_CONSUMER_ID.toString(),
        "external_measurement_id" to EXTERNAL_MEASUREMENT_ID.toString()
      ),
      "FAILED_PRECONDITION: Measurement with external measurement consumer ID $EXTERNAL_MEASUREMENT_CONSUMER_ID," +
        "measurement ID $EXTERNAL_MEASUREMENT_ID not found"
    )
  }

  @Test
  fun `MeasurementStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementStateIllegal(Measurement.State.PENDING_REQUISITION_PARAMS)
        assertThat(internalException.state)
          .isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement is in wrong state of ${internalException.state}"
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_STATE_ILLEGAL",
      mapOf("measurement_state" to Measurement.State.PENDING_REQUISITION_PARAMS.toString()),
      "FAILED_PRECONDITION: Measurement is in wrong state of ${Measurement.State.PENDING_REQUISITION_PARAMS}"
    )
  }

  @Test
  fun `CertSubjectKeyIdAlreadyExists works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = CertSubjectKeyIdAlreadyExists()
        internalException.throwStatusRuntimeException(Status.ALREADY_EXISTS) {
          "Certificate with the same subject key identifier (SKID) already exists."
        }
      }
    verifyErrorInfo(
      exception,
      "CERT_SUBJECT_KEY_ID_ALREADY_EXISTS",
      mapOf(),
      "ALREADY_EXISTS: Certificate with the same subject key identifier (SKID) already exists."
    )
  }

  @Test
  fun `DataProviderCertificateNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          DataProviderCertificateNotFound(EXTERNAL_DATA_PROVIDER_ID, EXTERNAL_CERTIFICATE_ID)
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "DataProvider's Certificate with ExternalDataProviderId ${internalException.externalDataProviderId}, " +
            "ExternalCertificateId ${internalException.externalCertificateId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "DATA_PROVIDER_CERTIFICATE_NOT_FOUND",
      mapOf(
        "external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString(),
        "external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()
      ),
      "FAILED_PRECONDITION: DataProvider's Certificate with ExternalDataProviderId ${EXTERNAL_DATA_PROVIDER_ID}, ExternalCertificateId $EXTERNAL_CERTIFICATE_ID not found"
    )
  }

  @Test
  fun `MeasurementConsumerCertificateNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementConsumerCertificateNotFound(
            EXTERNAL_MEASUREMENT_CONSUMER_ID,
            EXTERNAL_CERTIFICATE_ID
          )
        assertThat(internalException.externalMeasurementConsumerId)
          .isEqualTo(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "MeasurementConsumer's Certificate with ExternalMeasurementConsumerId ${internalException.externalMeasurementConsumerId}, " +
            "ExternalCertificateId ${internalException.externalCertificateId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_CONSUMER_CERTIFICATE_NOT_FOUND",
      mapOf(
        "external_measurement_consumer_id" to EXTERNAL_MEASUREMENT_CONSUMER_ID.toString(),
        "external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()
      ),
      "FAILED_PRECONDITION: MeasurementConsumer's Certificate with ExternalMeasurementConsumerId $EXTERNAL_MEASUREMENT_CONSUMER_ID, " +
        "ExternalCertificateId $EXTERNAL_CERTIFICATE_ID not found"
    )
  }

  @Test
  fun `DuchyCertificateNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = DuchyCertificateNotFound(INTERNAL_DUCHY_ID, EXTERNAL_CERTIFICATE_ID)
        assertThat(internalException.internalDuchyId).isEqualTo(INTERNAL_DUCHY_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Duchy's Certificate with InternalDuchyId ${internalException.internalDuchyId}, " +
            "ExternalCertificateId ${internalException.externalCertificateId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "DUCHY_CERTIFICATE_NOT_FOUND",
      mapOf(
        "internal_duchy_id" to INTERNAL_DUCHY_ID.toString(),
        "external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()
      ),
      "FAILED_PRECONDITION: Duchy's Certificate with InternalDuchyId ${INTERNAL_DUCHY_ID}, " +
        "ExternalCertificateId $EXTERNAL_CERTIFICATE_ID not found"
    )
  }

  @Test
  fun `CertificateRevocationStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          CertificateRevocationStateIllegal(Certificate.RevocationState.HOLD)
        assertThat(internalException.state).isEqualTo(Certificate.RevocationState.HOLD)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Certificate revocation is in wrong state of ${internalException.state}"
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_REVOCATION_STATE_ILLEGAL",
      mapOf("certificate_revocation_state" to Certificate.RevocationState.HOLD.toString()),
      "FAILED_PRECONDITION: Certificate revocation is in wrong state of ${Certificate.RevocationState.HOLD}"
    )
  }

  @Test
  fun `CertificateIsInvalid works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = CertificateIsInvalid()
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Certificate is invalid"
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_IS_INVALID",
      mapOf(),
      "FAILED_PRECONDITION: Certificate is invalid"
    )
  }

  @Test
  fun `ComputationParticipantStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          ComputationParticipantStateIllegal(ComputationParticipant.State.FAILED)
        assertThat(internalException.state)
          .isEqualTo(ComputationParticipant.State.FAILED)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "ComputationParticipant is in wrong state of ${internalException.state}"
        }
      }
    verifyErrorInfo(
      exception,
      "COMPUTATION_PARTICIPANT_STATE_ILLEGAL",
      mapOf("computation_participant_state" to ComputationParticipant.State.FAILED.toString()),
      "FAILED_PRECONDITION: ComputationParticipant is in wrong state of ${ComputationParticipant.State.FAILED}"
    )
  }

  @Test
  fun `ComputationParticipantNotFoundByComputation works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          ComputationParticipantNotFoundByComputation(EXTERNAL_COMPUTATION_ID, EXTERNAL_DUCHY_ID)
        assertThat(internalException.externalComputationId).isEqualTo(EXTERNAL_COMPUTATION_ID)
        assertThat(internalException.externalDuchyId).isEqualTo(EXTERNAL_DUCHY_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "ComputationParticipant with ExternalComputationId ${internalException.externalComputationId}, " +
            "ExternalDuchyId ${internalException.externalDuchyId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "COMPUTATION_PARTICIPANT_NOT_FOUND_BY_COMPUTATION",
      mapOf(
        "external_computation_id" to EXTERNAL_COMPUTATION_ID.toString(),
        "external_duchy_id" to EXTERNAL_DUCHY_ID
      ),
      "NOT_FOUND: ComputationParticipant with ExternalComputationId $EXTERNAL_COMPUTATION_ID, " +
        "ExternalDuchyId $EXTERNAL_DUCHY_ID not found"
    )
  }

  @Test
  fun `ComputationParticipantNotFoundByMeasurement works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          ComputationParticipantNotFoundByMeasurement(
            INTERNAL_MEASUREMENT_CONSUMER_ID,
            INTERNAL_MEASUREMENT_ID,
            INTERNAL_DUCHY_ID
          )
        assertThat(internalException.internalMeasurementConsumerId)
          .isEqualTo(INTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.internalMeasurementId).isEqualTo(INTERNAL_MEASUREMENT_ID)
        assertThat(internalException.internalDuchyId).isEqualTo(INTERNAL_DUCHY_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "ComputationParticipant with InternalMeasurementConsumerId ${internalException.internalMeasurementConsumerId}, " +
            "InternalMeasurementId ${internalException.internalMeasurementId}, InternalDuchyId ${internalException.internalDuchyId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "COMPUTATION_PARTICIPANT_NOT_FOUND_BY_MEASUREMENT",
      mapOf(
        "internal_measurement_consumer_id" to INTERNAL_MEASUREMENT_CONSUMER_ID.toString(),
        "internal_measurement_id" to INTERNAL_MEASUREMENT_ID.toString(),
        "internal_duchy_id" to INTERNAL_DUCHY_ID.toString()
      ),
      "NOT_FOUND: ComputationParticipant with InternalMeasurementConsumerId $INTERNAL_MEASUREMENT_CONSUMER_ID, " +
        "InternalMeasurementId $INTERNAL_MEASUREMENT_ID, InternalDuchyId $INTERNAL_DUCHY_ID not found"
    )
  }

  @Test
  fun `RequisitionNotFoundByComputation works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          RequisitionNotFoundByComputation(EXTERNAL_COMPUTATION_ID, EXTERNAL_REQUISITION_ID)
        assertThat(internalException.externalComputationId).isEqualTo(EXTERNAL_COMPUTATION_ID)
        assertThat(internalException.externalRequisitionId).isEqualTo(EXTERNAL_REQUISITION_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Requisition with ExternalComputationId ${internalException.externalComputationId}, " +
            "ExternalRequisitionId ${internalException.externalRequisitionId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "REQUISITION_NOT_FOUND_BY_COMPUTATION",
      mapOf(
        "external_computation_id" to EXTERNAL_COMPUTATION_ID.toString(),
        "external_requisition_id" to EXTERNAL_REQUISITION_ID.toString(),
      ),
      "NOT_FOUND: Requisition with ExternalComputationId $EXTERNAL_COMPUTATION_ID, " +
        "ExternalRequisitionId $EXTERNAL_REQUISITION_ID not found"
    )
  }

  @Test
  fun `RequisitionNotFoundByDataProvider works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          RequisitionNotFoundByDataProvider(EXTERNAL_DATA_PROVIDER_ID, EXTERNAL_REQUISITION_ID)
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalRequisitionId).isEqualTo(EXTERNAL_REQUISITION_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Requisition with ExternalDataProviderId ${internalException.externalDataProviderId}, " +
            "ExternalRequisitionId ${internalException.externalRequisitionId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "REQUISITION_NOT_FOUND_BY_DATA_PROVIDER",
      mapOf(
        "external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString(),
        "external_requisition_id" to EXTERNAL_REQUISITION_ID.toString(),
      ),
      "NOT_FOUND: Requisition with ExternalDataProviderId $EXTERNAL_DATA_PROVIDER_ID, " +
        "ExternalRequisitionId $EXTERNAL_REQUISITION_ID not found"
    )
  }

  @Test
  fun `RequisitionStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = RequisitionStateIllegal(Requisition.State.PENDING_PARAMS)
        assertThat(internalException.state).isEqualTo(Requisition.State.PENDING_PARAMS)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Requisition is in wrong state of ${internalException.state}"
        }
      }
    verifyErrorInfo(
      exception,
      "REQUISITION_STATE_ILLEGAL",
      mapOf("requisition_state" to Requisition.State.PENDING_PARAMS.toString()),
      "FAILED_PRECONDITION: Requisition is in wrong state of ${Requisition.State.PENDING_PARAMS}"
    )
  }

  @Test
  fun `AccountNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = AccountNotFound(EXTERNAL_ACCOUNT_ID)
        assertThat(internalException.externalAccountId).isEqualTo(EXTERNAL_ACCOUNT_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Account with external ID ${internalException.externalAccountId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "ACCOUNT_NOT_FOUND",
      mapOf("external_account_id" to EXTERNAL_ACCOUNT_ID.toString()),
      "FAILED_PRECONDITION: Account with external ID $EXTERNAL_ACCOUNT_ID not found"
    )
  }

  @Test
  fun `DuplicateAccountIdentity works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = DuplicateAccountIdentity(EXTERNAL_ACCOUNT_ID, ISSUER, SUBJECT)
        assertThat(internalException.externalAccountId).isEqualTo(EXTERNAL_ACCOUNT_ID)
        assertThat(internalException.issuer).isEqualTo(ISSUER)
        assertThat(internalException.subject).isEqualTo(SUBJECT)
        internalException.throwStatusRuntimeException(Status.INVALID_ARGUMENT) {
          "Issuer and subject pair already exists"
        }
      }
    verifyErrorInfo(
      exception,
      "DUPLICATE_ACCOUNT_IDENTITY",
      mapOf(
        "external_account_id" to EXTERNAL_ACCOUNT_ID.toString(),
        "issuer" to ISSUER,
        "subject" to SUBJECT
      ),
      "INVALID_ARGUMENT: Issuer and subject pair already exists"
    )
  }

  @Test
  fun `AccountActivationStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          AccountActivationStateIllegal(Account.ActivationState.UNRECOGNIZED)
        assertThat(internalException.state)
          .isEqualTo(Account.ActivationState.UNRECOGNIZED)
        internalException.throwStatusRuntimeException(Status.PERMISSION_DENIED) {
          "Account Activation is in wrong state of ${Account.ActivationState.UNRECOGNIZED}"
        }
      }
    verifyErrorInfo(
      exception,
      "ACCOUNT_ACTIVATION_STATE_ILLEGAL",
      mapOf("account_activation_state" to Account.ActivationState.UNRECOGNIZED.toString()),
      "PERMISSION_DENIED: Account Activation is in wrong state of ${Account.ActivationState.UNRECOGNIZED}"
    )
  }

  @Test
  fun `PermissionDenied works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = PermissionDenied()
        internalException.throwStatusRuntimeException(Status.PERMISSION_DENIED) {
          "Permission Denied"
        }
      }
    verifyErrorInfo(exception, "PERMISSION_DENIED", mapOf(), "PERMISSION_DENIED: Permission Denied")
  }

  @Test
  fun `ApiKeyNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = ApiKeyNotFound(EXTERNAL_API_KEY_ID)
        assertThat(internalException.externalApiKeyId).isEqualTo(EXTERNAL_API_KEY_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "API key with external ID ${internalException.externalApiKeyId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "API_KEY_NOT_FOUND",
      mapOf("external_api_key_id" to EXTERNAL_API_KEY_ID.toString()),
      "FAILED_PRECONDITION: API key with external ID $EXTERNAL_API_KEY_ID not found"
    )
  }

  @Test
  fun `EventGroupNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          EventGroupNotFound(EXTERNAL_DATA_PROVIDER_ID, EXTERNAL_EVENT_GROUP_ID)
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalEventGroupId).isEqualTo(EXTERNAL_EVENT_GROUP_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "EventGroup with ExternalDataProviderId ${internalException.externalDataProviderId}, " +
            "ExternalEventGroupId ${internalException.externalEventGroupId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "EVENT_GROUP_NOT_FOUND",
      mapOf(
        "external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString(),
        "external_event_group_id" to EXTERNAL_EVENT_GROUP_ID.toString()
      ),
      "NOT_FOUND: EventGroup with ExternalDataProviderId $EXTERNAL_DATA_PROVIDER_ID, " +
        "ExternalEventGroupId $EXTERNAL_EVENT_GROUP_ID not found"
    )
  }

  @Test
  fun `EventGroupInvalidArgs works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          EventGroupInvalidArgs(EXTERNAL_MEASUREMENT_ID, EXTERNAL_MEASUREMENT_ID_2)
        assertThat(internalException.originalExternalMeasurementId)
          .isEqualTo(EXTERNAL_MEASUREMENT_ID)
        assertThat(internalException.providedExternalMeasurementId)
          .isEqualTo(EXTERNAL_MEASUREMENT_ID_2)
        internalException.throwStatusRuntimeException(Status.INVALID_ARGUMENT) {
          "EventGroup invalid arguments with original MeasurementId ${internalException.originalExternalMeasurementId}, " +
            "provided MeasurementId ${internalException.providedExternalMeasurementId}"
        }
      }
    verifyErrorInfo(
      exception,
      "EVENT_GROUP_INVALID_ARGS",
      mapOf(
        "original_external_measurement_id" to EXTERNAL_MEASUREMENT_ID.toString(),
        "provided_external_measurement_id" to EXTERNAL_MEASUREMENT_ID_2.toString()
      ),
      "INVALID_ARGUMENT: EventGroup invalid arguments with original MeasurementId $EXTERNAL_MEASUREMENT_ID, " +
        "provided MeasurementId $EXTERNAL_MEASUREMENT_ID_2"
    )
  }

  @Test
  fun `EventGroupMetadataDescriptorNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          EventGroupMetadataDescriptorNotFound(
            EXTERNAL_DATA_PROVIDER_ID,
            EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID
          )
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalEventGroupMetadataDescriptorId)
          .isEqualTo(EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "EventGroupMetadataDescriptor with ExternalDataProviderId ${internalException.externalDataProviderId}, " +
            "ExternalEventGroupMetadataDescriptorId ${internalException.externalEventGroupMetadataDescriptorId} not found"
        }
      }
    verifyErrorInfo(
      exception,
      "EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND",
      mapOf(
        "external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString(),
        "external_event_group_metadata_descriptor_id" to
          EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID.toString()
      ),
      "NOT_FOUND: EventGroupMetadataDescriptor with ExternalDataProviderId $EXTERNAL_DATA_PROVIDER_ID, " +
        "ExternalEventGroupMetadataDescriptorId $EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID not found"
    )
  }
}
