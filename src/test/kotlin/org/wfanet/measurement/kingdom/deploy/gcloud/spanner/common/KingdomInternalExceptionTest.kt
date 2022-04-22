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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition

private const val ERROR_DOMAIN = "com.google.rpc.ErrorInfo"
private val EXTERNAL_ACCOUNT_ID = ExternalId(100L)
private val EXTERNAL_MEASUREMENT_CONSUMER_ID = ExternalId(101L)
private val INTERNAL_MEASUREMENT_CONSUMER_ID = InternalId(1010L)
private val EXTERNAL_MEASUREMENT_ID = ExternalId(102L)
private val INTERNAL_MEASUREMENT_ID = InternalId(1020L)
private val EXTERNAL_MEASUREMENT_ID_2 = ExternalId(1022L)
private val EXTERNAL_REQUISITION_ID = ExternalId(103L)
private val EXTERNAL_DATA_PROVIDER_ID = ExternalId(104L)
private val INTERNAL_DATA_PROVIDER_ID = InternalId(1040L)
private val EXTERNAL_MODEL_PROVIDER_ID = ExternalId(105L)
private val EXTERNAL_COMPUTATION_ID = ExternalId(106L)
private val EXTERNAL_CERTIFICATE_ID = ExternalId(107L)
private const val EXTERNAL_DUCHY_ID = "worker1"
private val INTERNAL_DUCHY_ID = InternalId(108L)
private const val ISSUER = "issuer"
private const val SUBJECT = "subject"
private val EXTERNAL_API_KEY_ID = ExternalId(109L)
private val EXTERNAL_EVENT_GROUP_ID = ExternalId(110L)
private val EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID = ExternalId(111L)

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
  fun `MeasurementConsumerNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementConsumerNotFoundException(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalMeasurementConsumerId.value)
          .isEqualTo(EXTERNAL_MEASUREMENT_CONSUMER_ID.value)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement Consumer not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_CONSUMER_NOT_FOUND",
      mapOf("external_measurement_consumer_id" to EXTERNAL_MEASUREMENT_CONSUMER_ID.toString()),
      "FAILED_PRECONDITION: Measurement Consumer not found. " +
        "external_measurement_consumer_id=$EXTERNAL_MEASUREMENT_CONSUMER_ID"
    )
  }

  @Test
  fun `DataProviderNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = DataProviderNotFoundException(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Data Provider not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "DATA_PROVIDER_NOT_FOUND",
      mapOf("external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString()),
      "FAILED_PRECONDITION: Data Provider not found. " +
        "external_data_provider_id=$EXTERNAL_DATA_PROVIDER_ID"
    )
  }

  @Test
  fun `ModelProviderNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = ModelProviderNotFoundException(EXTERNAL_MODEL_PROVIDER_ID)
        assertThat(internalException.externalModelProviderId).isEqualTo(EXTERNAL_MODEL_PROVIDER_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Model Provider not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "MODEL_PROVIDER_NOT_FOUND",
      mapOf("external_model_provider_id" to EXTERNAL_MODEL_PROVIDER_ID.toString()),
      "FAILED_PRECONDITION: Model Provider not found. " +
        "external_model_provider_id=$EXTERNAL_MODEL_PROVIDER_ID"
    )
  }

  @Test
  fun `DuchyNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = DuchyNotFoundException(EXTERNAL_DUCHY_ID)
        assertThat(internalException.externalDuchyId).isEqualTo(EXTERNAL_DUCHY_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Duchy not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "DUCHY_NOT_FOUND",
      mapOf("external_duchy_id" to EXTERNAL_DUCHY_ID),
      "FAILED_PRECONDITION: Duchy not found. " + "external_duchy_id=$EXTERNAL_DUCHY_ID"
    )
  }

  @Test
  fun `MeasurementNotFoundByComputation with computationId works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = MeasurementNotFoundByComputationException(EXTERNAL_COMPUTATION_ID)
        assertThat(internalException.externalComputationId).isEqualTo(EXTERNAL_COMPUTATION_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_NOT_FOUND",
      mapOf("external_computation_id" to EXTERNAL_COMPUTATION_ID.toString()),
      "FAILED_PRECONDITION: Measurement not found. " +
        "external_computation_id=$EXTERNAL_COMPUTATION_ID"
    )
  }

  @Test
  fun `MeasurementNotFoundByMeasurementConsumer works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementNotFoundByMeasurementConsumerException(
            EXTERNAL_MEASUREMENT_CONSUMER_ID,
            EXTERNAL_MEASUREMENT_ID
          )
        assertThat(internalException.externalMeasurementConsumerId)
          .isEqualTo(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalMeasurementId).isEqualTo(EXTERNAL_MEASUREMENT_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_NOT_FOUND",
      mapOf(
        "external_measurement_consumer_id" to EXTERNAL_MEASUREMENT_CONSUMER_ID.toString(),
        "external_measurement_id" to EXTERNAL_MEASUREMENT_ID.toString()
      ),
      "FAILED_PRECONDITION: Measurement not found. " +
        "external_measurement_consumer_id=$EXTERNAL_MEASUREMENT_CONSUMER_ID " +
        "external_measurement_id=$EXTERNAL_MEASUREMENT_ID"
    )
  }

  @Test
  fun `MeasurementStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementStateIllegalException(
            EXTERNAL_MEASUREMENT_CONSUMER_ID,
            EXTERNAL_MEASUREMENT_ID,
            Measurement.State.PENDING_REQUISITION_PARAMS
          )
        assertThat(internalException.externalMeasurementConsumerId)
          .isEqualTo(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalMeasurementId).isEqualTo(EXTERNAL_MEASUREMENT_ID)
        assertThat(internalException.state).isEqualTo(Measurement.State.PENDING_REQUISITION_PARAMS)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement state illegal. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "MEASUREMENT_STATE_ILLEGAL",
      mapOf(
        "external_measurement_consumer_id" to EXTERNAL_MEASUREMENT_CONSUMER_ID.toString(),
        "external_measurement_id" to EXTERNAL_MEASUREMENT_ID.toString(),
        "measurement_state" to Measurement.State.PENDING_REQUISITION_PARAMS.toString()
      ),
      "FAILED_PRECONDITION: Measurement state illegal. " +
        "external_measurement_consumer_id=$EXTERNAL_MEASUREMENT_CONSUMER_ID " +
        "external_measurement_id=$EXTERNAL_MEASUREMENT_ID " +
        "measurement_state=${Measurement.State.PENDING_REQUISITION_PARAMS}"
    )
  }

  @Test
  fun `CertSubjectKeyIdAlreadyExists works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = CertSubjectKeyIdAlreadyExistsException()
        internalException.throwStatusRuntimeException(Status.ALREADY_EXISTS) {
          "Certificate subject key id already exists."
        }
      }
    verifyErrorInfo(
      exception,
      "CERT_SUBJECT_KEY_ID_ALREADY_EXISTS",
      mapOf(),
      "ALREADY_EXISTS: Certificate subject key id already exists."
    )
  }

  @Test
  fun `DataProviderCertificateNotFoundByExternal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          DataProviderCertificateNotFoundByExternalException(
            EXTERNAL_DATA_PROVIDER_ID,
            EXTERNAL_CERTIFICATE_ID
          )
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Data Provider's Certificate not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_NOT_FOUND",
      mapOf(
        "external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString(),
        "external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()
      ),
      "FAILED_PRECONDITION: Data Provider's Certificate not found. " +
        "external_data_provider_id=$EXTERNAL_DATA_PROVIDER_ID " +
        "external_certificate_id=$EXTERNAL_CERTIFICATE_ID"
    )
  }

  @Test
  fun `DataProviderCertificateNotFoundByInternal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          DataProviderCertificateNotFoundByInternalException(
            INTERNAL_DATA_PROVIDER_ID,
            EXTERNAL_CERTIFICATE_ID
          )
        assertThat(internalException.internalDataProviderId).isEqualTo(INTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Data Provider's Certificate not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_NOT_FOUND",
      mapOf("external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()),
      "FAILED_PRECONDITION: Data Provider's Certificate not found. " +
        "external_certificate_id=$EXTERNAL_CERTIFICATE_ID"
    )
  }

  @Test
  fun `MeasurementConsumerCertificateNotFoundByExternal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementConsumerCertificateNotFoundByExternalException(
            EXTERNAL_MEASUREMENT_CONSUMER_ID,
            EXTERNAL_CERTIFICATE_ID
          )
        assertThat(internalException.externalMeasurementConsumerId)
          .isEqualTo(EXTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement Consumer's Certificate not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_NOT_FOUND",
      mapOf(
        "external_measurement_consumer_id" to EXTERNAL_MEASUREMENT_CONSUMER_ID.toString(),
        "external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()
      ),
      "FAILED_PRECONDITION: Measurement Consumer's Certificate not found. " +
        "external_measurement_consumer_id=$EXTERNAL_MEASUREMENT_CONSUMER_ID " +
        "external_certificate_id=$EXTERNAL_CERTIFICATE_ID"
    )
  }

  @Test
  fun `MeasurementConsumerCertificateNotFoundByInternal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          MeasurementConsumerCertificateNotFoundByInternalException(
            INTERNAL_MEASUREMENT_CONSUMER_ID,
            EXTERNAL_CERTIFICATE_ID
          )
        assertThat(internalException.internalMeasurementConsumerId)
          .isEqualTo(INTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Measurement Consumer's Certificate not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_NOT_FOUND",
      mapOf("external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()),
      "FAILED_PRECONDITION: Measurement Consumer's Certificate not found. " +
        "external_certificate_id=$EXTERNAL_CERTIFICATE_ID"
    )
  }

  @Test
  fun `DuchyCertificateNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          DuchyCertificateNotFoundException(INTERNAL_DUCHY_ID, EXTERNAL_CERTIFICATE_ID)
        assertThat(internalException.internalDuchyId).isEqualTo(INTERNAL_DUCHY_ID)
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Duchy's Certificate not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_NOT_FOUND",
      mapOf("external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString()),
      "FAILED_PRECONDITION: Duchy's Certificate not found. " +
        "external_certificate_id=$EXTERNAL_CERTIFICATE_ID"
    )
  }

  @Test
  fun `CertificateRevocationStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          CertificateRevocationStateIllegalException(
            EXTERNAL_CERTIFICATE_ID,
            Certificate.RevocationState.HOLD
          )
        assertThat(internalException.externalCertificateId).isEqualTo(EXTERNAL_CERTIFICATE_ID)
        assertThat(internalException.state).isEqualTo(Certificate.RevocationState.HOLD)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Certificate revocation state illegal. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_REVOCATION_STATE_ILLEGAL",
      mapOf(
        "external_certificate_id" to EXTERNAL_CERTIFICATE_ID.toString(),
        "certificate_revocation_state" to Certificate.RevocationState.HOLD.toString()
      ),
      "FAILED_PRECONDITION: Certificate revocation state illegal. " +
        "external_certificate_id=$EXTERNAL_CERTIFICATE_ID " +
        "certificate_revocation_state=${Certificate.RevocationState.HOLD}"
    )
  }

  @Test
  fun `CertificateIsInvalid works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = CertificateIsInvalidException()
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Certificate is invalid."
        }
      }
    verifyErrorInfo(
      exception,
      "CERTIFICATE_IS_INVALID",
      mapOf(),
      "FAILED_PRECONDITION: Certificate is invalid."
    )
  }

  @Test
  fun `ComputationParticipantStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          ComputationParticipantStateIllegalException(
            EXTERNAL_COMPUTATION_ID,
            EXTERNAL_DUCHY_ID,
            ComputationParticipant.State.FAILED
          )
        assertThat(internalException.externalComputationId).isEqualTo(EXTERNAL_COMPUTATION_ID)
        assertThat(internalException.externalDuchyId).isEqualTo(EXTERNAL_DUCHY_ID)
        assertThat(internalException.state).isEqualTo(ComputationParticipant.State.FAILED)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Computation Participant state illegal. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "COMPUTATION_PARTICIPANT_STATE_ILLEGAL",
      mapOf(
        "external_computation_id" to EXTERNAL_COMPUTATION_ID.toString(),
        "external_duchy_id" to EXTERNAL_DUCHY_ID,
        "computation_participant_state" to ComputationParticipant.State.FAILED.toString()
      ),
      "FAILED_PRECONDITION: Computation Participant state illegal. " +
        "external_computation_id=$EXTERNAL_COMPUTATION_ID " +
        "external_duchy_id=$EXTERNAL_DUCHY_ID " +
        "computation_participant_state=${ComputationParticipant.State.FAILED}"
    )
  }

  @Test
  fun `ComputationParticipantNotFoundByComputation works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          ComputationParticipantNotFoundByComputationException(
            EXTERNAL_COMPUTATION_ID,
            EXTERNAL_DUCHY_ID
          )
        assertThat(internalException.externalComputationId).isEqualTo(EXTERNAL_COMPUTATION_ID)
        assertThat(internalException.externalDuchyId).isEqualTo(EXTERNAL_DUCHY_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Computation Participant not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "COMPUTATION_PARTICIPANT_NOT_FOUND",
      mapOf(
        "external_computation_id" to EXTERNAL_COMPUTATION_ID.toString(),
        "external_duchy_id" to EXTERNAL_DUCHY_ID
      ),
      "NOT_FOUND: Computation Participant not found. " +
        "external_computation_id=$EXTERNAL_COMPUTATION_ID " +
        "external_duchy_id=$EXTERNAL_DUCHY_ID"
    )
  }

  @Test
  fun `ComputationParticipantNotFoundByMeasurement works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          ComputationParticipantNotFoundByMeasurementException(
            INTERNAL_MEASUREMENT_CONSUMER_ID,
            INTERNAL_MEASUREMENT_ID,
            INTERNAL_DUCHY_ID
          )
        assertThat(internalException.internalMeasurementConsumerId)
          .isEqualTo(INTERNAL_MEASUREMENT_CONSUMER_ID)
        assertThat(internalException.internalMeasurementId).isEqualTo(INTERNAL_MEASUREMENT_ID)
        assertThat(internalException.internalDuchyId).isEqualTo(INTERNAL_DUCHY_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Computation Participant not found."
        }
      }
    verifyErrorInfo(
      exception,
      "COMPUTATION_PARTICIPANT_NOT_FOUND",
      mapOf(),
      "NOT_FOUND: Computation Participant not found."
    )
  }

  @Test
  fun `RequisitionNotFoundByComputation works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          RequisitionNotFoundByComputationException(
            EXTERNAL_COMPUTATION_ID,
            EXTERNAL_REQUISITION_ID
          )
        assertThat(internalException.externalComputationId).isEqualTo(EXTERNAL_COMPUTATION_ID)
        assertThat(internalException.externalRequisitionId).isEqualTo(EXTERNAL_REQUISITION_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Requisition not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "REQUISITION_NOT_FOUND",
      mapOf(
        "external_computation_id" to EXTERNAL_COMPUTATION_ID.toString(),
        "external_requisition_id" to EXTERNAL_REQUISITION_ID.toString(),
      ),
      "NOT_FOUND: Requisition not found. " +
        "external_computation_id=$EXTERNAL_COMPUTATION_ID " +
        "external_requisition_id=$EXTERNAL_REQUISITION_ID"
    )
  }

  @Test
  fun `RequisitionNotFoundByDataProvider works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          RequisitionNotFoundByDataProviderException(
            EXTERNAL_DATA_PROVIDER_ID,
            EXTERNAL_REQUISITION_ID
          )
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalRequisitionId).isEqualTo(EXTERNAL_REQUISITION_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Requisition not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "REQUISITION_NOT_FOUND",
      mapOf(
        "external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString(),
        "external_requisition_id" to EXTERNAL_REQUISITION_ID.toString(),
      ),
      "NOT_FOUND: Requisition not found. " +
        "external_data_provider_id=$EXTERNAL_DATA_PROVIDER_ID " +
        "external_requisition_id=$EXTERNAL_REQUISITION_ID"
    )
  }

  @Test
  fun `RequisitionStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          RequisitionStateIllegalException(
            EXTERNAL_REQUISITION_ID,
            Requisition.State.PENDING_PARAMS
          )
        assertThat(internalException.externalRequisitionId).isEqualTo(EXTERNAL_REQUISITION_ID)
        assertThat(internalException.state).isEqualTo(Requisition.State.PENDING_PARAMS)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Requisition state illegal. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "REQUISITION_STATE_ILLEGAL",
      mapOf(
        "external_requisition_id" to EXTERNAL_REQUISITION_ID.toString(),
        "requisition_state" to Requisition.State.PENDING_PARAMS.toString()
      ),
      "FAILED_PRECONDITION: Requisition state illegal. " +
        "external_requisition_id=$EXTERNAL_REQUISITION_ID " +
        "requisition_state=${Requisition.State.PENDING_PARAMS}"
    )
  }

  @Test
  fun `AccountNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = AccountNotFoundException(EXTERNAL_ACCOUNT_ID)
        assertThat(internalException.externalAccountId).isEqualTo(EXTERNAL_ACCOUNT_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "Account not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "ACCOUNT_NOT_FOUND",
      mapOf("external_account_id" to EXTERNAL_ACCOUNT_ID.toString()),
      "FAILED_PRECONDITION: Account not found. " + "external_account_id=$EXTERNAL_ACCOUNT_ID"
    )
  }

  @Test
  fun `DuplicateAccountIdentity works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          DuplicateAccountIdentityException(EXTERNAL_ACCOUNT_ID, ISSUER, SUBJECT)
        assertThat(internalException.externalAccountId).isEqualTo(EXTERNAL_ACCOUNT_ID)
        assertThat(internalException.issuer).isEqualTo(ISSUER)
        assertThat(internalException.subject).isEqualTo(SUBJECT)
        internalException.throwStatusRuntimeException(Status.INVALID_ARGUMENT) {
          "Duplicate Account identity. " + internalException.contextToString()
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
      "INVALID_ARGUMENT: Duplicate Account identity. " +
        "external_account_id=$EXTERNAL_ACCOUNT_ID " +
        "issuer=$ISSUER subject=$SUBJECT"
    )
  }

  @Test
  fun `AccountActivationStateIllegal works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          AccountActivationStateIllegalException(
            EXTERNAL_ACCOUNT_ID,
            Account.ActivationState.UNRECOGNIZED
          )
        assertThat(internalException.externalAccountId).isEqualTo(EXTERNAL_ACCOUNT_ID)
        assertThat(internalException.state).isEqualTo(Account.ActivationState.UNRECOGNIZED)
        internalException.throwStatusRuntimeException(Status.PERMISSION_DENIED) {
          "Account activation state illegal. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "ACCOUNT_ACTIVATION_STATE_ILLEGAL",
      mapOf(
        "external_account_id" to EXTERNAL_ACCOUNT_ID.toString(),
        "account_activation_state" to Account.ActivationState.UNRECOGNIZED.toString()
      ),
      "PERMISSION_DENIED: Account activation state illegal. " +
        "external_account_id=$EXTERNAL_ACCOUNT_ID " +
        "account_activation_state=${Account.ActivationState.UNRECOGNIZED}"
    )
  }

  @Test
  fun `PermissionDenied works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = PermissionDeniedException()
        internalException.throwStatusRuntimeException(Status.PERMISSION_DENIED) {
          "Permission Denied."
        }
      }
    verifyErrorInfo(
      exception,
      "PERMISSION_DENIED",
      mapOf(),
      "PERMISSION_DENIED: Permission Denied."
    )
  }

  @Test
  fun `ApiKeyNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = ApiKeyNotFoundException(EXTERNAL_API_KEY_ID)
        assertThat(internalException.externalApiKeyId).isEqualTo(EXTERNAL_API_KEY_ID)
        internalException.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
          "API Key not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "API_KEY_NOT_FOUND",
      mapOf("external_api_key_id" to EXTERNAL_API_KEY_ID.toString()),
      "FAILED_PRECONDITION: API Key not found. " + "external_api_key_id=$EXTERNAL_API_KEY_ID"
    )
  }

  @Test
  fun `EventGroupNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          EventGroupNotFoundException(EXTERNAL_DATA_PROVIDER_ID, EXTERNAL_EVENT_GROUP_ID)
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalEventGroupId).isEqualTo(EXTERNAL_EVENT_GROUP_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Event Group not found. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "EVENT_GROUP_NOT_FOUND",
      mapOf(
        "external_data_provider_id" to EXTERNAL_DATA_PROVIDER_ID.toString(),
        "external_event_group_id" to EXTERNAL_EVENT_GROUP_ID.toString()
      ),
      "NOT_FOUND: Event Group not found. " +
        "external_data_provider_id=$EXTERNAL_DATA_PROVIDER_ID " +
        "external_event_group_id=$EXTERNAL_EVENT_GROUP_ID"
    )
  }

  @Test
  fun `EventGroupInvalidArgs works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          EventGroupInvalidArgsException(EXTERNAL_MEASUREMENT_ID, EXTERNAL_MEASUREMENT_ID_2)
        assertThat(internalException.originalExternalMeasurementId)
          .isEqualTo(EXTERNAL_MEASUREMENT_ID)
        assertThat(internalException.providedExternalMeasurementId)
          .isEqualTo(EXTERNAL_MEASUREMENT_ID_2)
        internalException.throwStatusRuntimeException(Status.INVALID_ARGUMENT) {
          "Event Group has invalid arguments. " + internalException.contextToString()
        }
      }
    verifyErrorInfo(
      exception,
      "EVENT_GROUP_INVALID_ARGS",
      mapOf(
        "original_external_measurement_id" to EXTERNAL_MEASUREMENT_ID.toString(),
        "provided_external_measurement_id" to EXTERNAL_MEASUREMENT_ID_2.toString()
      ),
      "INVALID_ARGUMENT: Event Group has invalid arguments. " +
        "original_external_measurement_id=$EXTERNAL_MEASUREMENT_ID " +
        "provided_external_measurement_id=$EXTERNAL_MEASUREMENT_ID_2"
    )
  }

  @Test
  fun `EventGroupMetadataDescriptorNotFound works as expected`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException =
          EventGroupMetadataDescriptorNotFoundException(
            EXTERNAL_DATA_PROVIDER_ID,
            EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID
          )
        assertThat(internalException.externalDataProviderId).isEqualTo(EXTERNAL_DATA_PROVIDER_ID)
        assertThat(internalException.externalEventGroupMetadataDescriptorId)
          .isEqualTo(EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) {
          "Event Group Metadata Descriptor not found. " + internalException.contextToString()
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
      "NOT_FOUND: Event Group Metadata Descriptor not found. " +
        "external_data_provider_id=$EXTERNAL_DATA_PROVIDER_ID " +
        "external_event_group_metadata_descriptor_id=$EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID"
    )
  }
}
