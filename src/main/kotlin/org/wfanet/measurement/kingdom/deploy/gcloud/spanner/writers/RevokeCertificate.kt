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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.api.gax.rpc.ErrorDetails
import com.google.protobuf.type
import java.lang.IllegalStateException
import java.time.Clock
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.Certificate.RevocationState
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateRevocationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamMeasurements
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamMeasurementsByDataProviderCertificate
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamMeasurementsByDuchyCertificate
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

private val PENDING_MEASUREMENT_STATES =
  listOf(
    Measurement.State.PENDING_COMPUTATION,
    Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
    Measurement.State.PENDING_REQUISITION_FULFILLMENT,
    Measurement.State.PENDING_REQUISITION_PARAMS
  )

/**
 * Revokes a certificate in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [DataProviderCertificateNotFoundException] Certificate not found
 * @throws [MeasurementConsumerCertificateNotFoundException] Certificate not found
 * @throws [DuchyCertificateNotFoundException] Certificate not found
 * @throws [DuchyNotFoundException] Duchy not found
 */
class RevokeCertificate(private val request: RevokeCertificateRequest) :
  SpannerWriter<Certificate, Certificate>() {

  override suspend fun TransactionScope.runTransaction(): Certificate {
    val externalCertificateId = ExternalId(request.externalCertificateId)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    val certificateResult =
      when (request.parentCase) {
        RevokeCertificateRequest.ParentCase.EXTERNAL_DATA_PROVIDER_ID -> {
          val externalDataProviderId = ExternalId(request.externalDataProviderId)
          val reader =
            CertificateReader(CertificateReader.ParentType.DATA_PROVIDER)
              .bindWhereClause(externalDataProviderId, externalCertificateId)
          reader.execute(transactionContext).singleOrNull()
            ?: throw DataProviderCertificateNotFoundException(
              externalDataProviderId,
              externalCertificateId
            ) {
              "Certificate not found."
            }
        }
        RevokeCertificateRequest.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> {
          val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
          val reader =
            CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
              .bindWhereClause(externalMeasurementConsumerId, externalCertificateId)
          reader.execute(transactionContext).singleOrNull()
            ?: throw MeasurementConsumerCertificateNotFoundException(
              externalMeasurementConsumerId,
              externalCertificateId
            ) {
              "Certificate not found."
            }
        }
        RevokeCertificateRequest.ParentCase.EXTERNAL_MODEL_PROVIDER_ID -> {
          val externalModelProviderId = ExternalId(request.externalModelProviderId)
          val reader =
            CertificateReader(CertificateReader.ParentType.MODEL_PROVIDER)
              .bindWhereClause(externalModelProviderId, externalCertificateId)
          reader.execute(transactionContext).singleOrNull()
            ?: throw ModelProviderCertificateNotFoundException(
              externalModelProviderId,
              externalCertificateId
            ) {
              "Certificate not found."
            }
        }
        RevokeCertificateRequest.ParentCase.EXTERNAL_DUCHY_ID -> {
          val duchyId =
            InternalId(
              DuchyIds.getInternalId(request.externalDuchyId)
                ?: throw DuchyNotFoundException(request.externalDuchyId) { " Duchy not found." }
            )
          val reader =
            CertificateReader(CertificateReader.ParentType.DUCHY)
              .bindWhereClause(duchyId, externalCertificateId)
          reader.execute(transactionContext).singleOrNull()
            ?: throw DuchyCertificateNotFoundException(
              request.externalDuchyId,
              externalCertificateId
            ) {
              "Certificate not found."
            }
        }
        RevokeCertificateRequest.ParentCase.PARENT_NOT_SET ->
          throw IllegalStateException("RevokeCertificateRequest is missing parent field.")
      }

    val revocationState = certificateResult.certificate.revocationState
    if (
      request.revocationState == RevocationState.REVOCATION_STATE_UNSPECIFIED ||
        (revocationState == RevocationState.REVOKED &&
          request.revocationState == RevocationState.HOLD)
    ) {
      throw CertificateRevocationStateIllegalException(externalCertificateId, revocationState) {
        "Certificate is in $revocationState state, cannot set to ${request.revocationState}."
      }
    }

    transactionContext.bufferUpdateMutation("Certificates") {
      set("CertificateId" to certificateResult.certificateId.value)
      set("RevocationState" to request.revocationState)
    }

    when (request.parentCase) {
      RevokeCertificateRequest.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> {

        val filter =
          StreamMeasurementsRequestKt.filter {
            externalMeasurementConsumerId = request.externalMeasurementConsumerId
            externalMeasurementConsumerCertificateId = request.externalCertificateId
            states += PENDING_MEASUREMENT_STATES
          }

        StreamMeasurements(Measurement.View.DEFAULT, filter).execute(transactionContext).collect {
          val details =
            it.measurement.details.copy {
              failure =
                MeasurementKt.failure {
                  reason = Measurement.Failure.Reason.CERTIFICATE_REVOKED
                  message = "The associated Measurement Consumer certificate has been revoked."
                }
            }

          failMeasurement(it.measurementConsumerId, it.measurementId, details)
        }
      }
      RevokeCertificateRequest.ParentCase.EXTERNAL_DATA_PROVIDER_ID -> {
        StreamMeasurementsByDataProviderCertificate(
            certificateResult.certificateId,
            PENDING_MEASUREMENT_STATES
          )
          .execute(transactionContext)
          .collect {
            val details =
              it.measurementDetails.copy {
                failure =
                  MeasurementKt.failure {
                    reason = Measurement.Failure.Reason.CERTIFICATE_REVOKED
                    message = "An associated Data Provider certificate has been revoked."
                  }
              }

            failMeasurement(it.measurementConsumerId, it.measurementId, details)
          }
      }
      RevokeCertificateRequest.ParentCase.EXTERNAL_DUCHY_ID -> {
        StreamMeasurementsByDuchyCertificate(
            certificateResult.certificateId,
            PENDING_MEASUREMENT_STATES
          )
          .execute(transactionContext)
          .collect {
            val details =
              it.measurementDetails.copy {
                failure =
                  MeasurementKt.failure {
                    reason = Measurement.Failure.Reason.CERTIFICATE_REVOKED
                    message = "An associated Duchy certificate has been revoked."
                  }
              }

            failMeasurement(it.measurementConsumerId, it.measurementId, details)
          }
      }
      else -> {}
    }

    return certificateResult.certificate.copy { this.revocationState = request.revocationState }
  }

  override fun ResultScope<Certificate>.buildResult(): Certificate {
    return checkNotNull(transactionResult)
  }

  private suspend fun TransactionScope.failMeasurement(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    details: Measurement.Details
  ) {

    val measurementState =
      MeasurementReader.readMeasurementState(
        transactionContext,
        measurementConsumerId,
        measurementId
      )

    val measurementLogEntryDetails =
      MeasurementLogEntryKt.details {
        logMessage = "Measurement failed due to a certificate revoked"
        this.error =
          MeasurementLogEntryKt.errorDetails {
            this.type = MeasurementLogEntry.ErrorDetails.Type.PERMANENT
            // TODO(@marcopremier): plumb in a clock instance dependency not to hardcode the system
            // one
            this.errorTime = Clock.systemUTC().protoTimestamp()
          }
      }

    updateMeasurementState(
      measurementConsumerId = measurementConsumerId,
      measurementId = measurementId,
      nextState = Measurement.State.FAILED,
      previousState = measurementState,
      logDetails = measurementLogEntryDetails,
      details = details
    )
  }
}
