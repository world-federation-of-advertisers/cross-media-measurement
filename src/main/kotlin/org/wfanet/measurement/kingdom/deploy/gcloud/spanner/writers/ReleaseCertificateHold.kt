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

import java.lang.IllegalStateException
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.Certificate.RevocationState
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateRevocationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader

/**
 * Revokes a certificate in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerCertificateNotFoundException] Certificate not found
 * @throws [DataProviderCertificateNotFoundException] Certificate not found
 * @throws [DuchyCertificateNotFoundException] Certificate not found
 * @throws [DuchyNotFoundException] Duchy not found
 * @throws [CertificateRevocationStateIllegalException] CertificateRevocation state is REVOKED or
 *   not specified
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#305) : Consider cancelling all
 *   associated active measurements if a certificate is revoked
 */
class ReleaseCertificateHold(private val request: ReleaseCertificateHoldRequest) :
  SpannerWriter<Certificate, Certificate>() {

  override suspend fun TransactionScope.runTransaction(): Certificate {
    val externalCertificateId = ExternalId(request.externalCertificateId)
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    val certificateResult =
      when (request.parentCase) {
        ReleaseCertificateHoldRequest.ParentCase.EXTERNAL_DATA_PROVIDER_ID -> {
          val externalDataProviderId = ExternalId(request.externalDataProviderId)
          val reader =
            CertificateReader(CertificateReader.ParentType.DATA_PROVIDER)
              .bindWhereClause(externalDataProviderId, externalCertificateId)
          reader.execute(transactionContext).singleOrNull()
            ?: throw DataProviderCertificateNotFoundException(
              externalDataProviderId,
              externalCertificateId,
            )
        }
        ReleaseCertificateHoldRequest.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> {
          val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
          val reader =
            CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
              .bindWhereClause(externalMeasurementConsumerId, externalCertificateId)
          reader.execute(transactionContext).singleOrNull()
            ?: throw MeasurementConsumerCertificateNotFoundException(
              externalMeasurementConsumerId,
              externalCertificateId,
            ) {
              "Certificate not found."
            }
        }
        ReleaseCertificateHoldRequest.ParentCase.EXTERNAL_MODEL_PROVIDER_ID -> {
          val externalModelProviderId = ExternalId(request.externalModelProviderId)
          val reader =
            CertificateReader(CertificateReader.ParentType.MODEL_PROVIDER)
              .bindWhereClause(externalModelProviderId, externalCertificateId)
          reader.execute(transactionContext).singleOrNull()
            ?: throw ModelProviderCertificateNotFoundException(
              externalModelProviderId,
              externalCertificateId,
            ) {
              "Certificate not found."
            }
        }
        ReleaseCertificateHoldRequest.ParentCase.EXTERNAL_DUCHY_ID -> {
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
              externalCertificateId,
            ) {
              "Certificate not found."
            }
        }
        ReleaseCertificateHoldRequest.ParentCase.PARENT_NOT_SET ->
          throw IllegalStateException("ReleaseCertificateHoldRequest is missing parent field.")
      }

    val certificateRevocationState = certificateResult.certificate.revocationState
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (certificateRevocationState) {
      RevocationState.HOLD -> {
        transactionContext.bufferUpdateMutation("Certificates") {
          set("CertificateId" to certificateResult.certificateId.value)
          set("RevocationState").toInt64(RevocationState.REVOCATION_STATE_UNSPECIFIED)
        }
        certificateResult.certificate.copy {
          revocationState = RevocationState.REVOCATION_STATE_UNSPECIFIED
        }
      }
      RevocationState.REVOKED,
      RevocationState.REVOCATION_STATE_UNSPECIFIED ->
        throw CertificateRevocationStateIllegalException(
          ExternalId(externalCertificateId.value),
          certificateRevocationState,
        ) {
          "Certificate is in $certificateRevocationState state, cannot release hold."
        }
      RevocationState.UNRECOGNIZED ->
        throw IllegalStateException("Certificate RevocationState field is unrecognized.")
    }
  }

  override fun ResultScope<Certificate>.buildResult(): Certificate {
    return checkNotNull(transactionResult)
  }
}
