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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.UpdatePublicKeyRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Updates the public key details for a [MeasurementConsumer] or a [DataProvider].
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerCertificateNotFoundException] MeasurementConsumer's Certificate not
 *   found
 * @throws [DataProviderNotFoundException] DataProvider not found
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class UpdatePublicKey(private val request: UpdatePublicKeyRequest) : SimpleSpannerWriter<Unit>() {

  override suspend fun TransactionScope.runTransaction() {
    if (request.externalMeasurementConsumerId != 0L) {
      val measurementConsumerResult =
        MeasurementConsumerReader()
          .readByExternalMeasurementConsumerId(
            transactionContext,
            ExternalId(request.externalMeasurementConsumerId),
          )
          ?: throw MeasurementConsumerNotFoundException(
            ExternalId(request.externalMeasurementConsumerId)
          )

      val certificateId: InternalId =
        CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
          .readMeasurementConsumerCertificateIdByExternalId(
            transactionContext,
            InternalId(measurementConsumerResult.measurementConsumerId),
            ExternalId(request.externalCertificateId),
          )
          ?: throw MeasurementConsumerCertificateNotFoundException(
            ExternalId(request.externalMeasurementConsumerId),
            ExternalId(request.externalCertificateId),
          )

      val measurementConsumerDetails =
        measurementConsumerResult.measurementConsumer.details.copy {
          apiVersion = request.apiVersion
          publicKey = request.publicKey
          publicKeySignature = request.publicKeySignature
          publicKeySignatureAlgorithmOid = request.publicKeySignatureAlgorithmOid
        }

      transactionContext.bufferUpdateMutation("MeasurementConsumers") {
        set("MeasurementConsumerId" to measurementConsumerResult.measurementConsumerId)
        set("PublicKeyCertificateId" to certificateId)
        set("MeasurementConsumerDetails").to(measurementConsumerDetails)
      }
    } else if (request.externalDataProviderId != 0L) {
      val dataProviderResult =
        DataProviderReader()
          .readByExternalDataProviderId(
            transactionContext,
            ExternalId(request.externalDataProviderId),
          ) ?: throw DataProviderNotFoundException(ExternalId(request.externalDataProviderId))

      val certificateId: InternalId =
        CertificateReader(CertificateReader.ParentType.DATA_PROVIDER)
          .readDataProviderCertificateIdByExternalId(
            transactionContext,
            InternalId(dataProviderResult.dataProviderId),
            ExternalId(request.externalCertificateId),
          )
          ?: throw DataProviderCertificateNotFoundException(
            ExternalId(request.externalDataProviderId),
            ExternalId(request.externalCertificateId),
          )

      val dataProviderDetails =
        dataProviderResult.dataProvider.details.copy {
          apiVersion = request.apiVersion
          publicKey = request.publicKey
          publicKeySignature = request.publicKeySignature
          publicKeySignatureAlgorithmOid = request.publicKeySignatureAlgorithmOid
        }

      transactionContext.bufferUpdateMutation("DataProviders") {
        set("DataProviderId" to dataProviderResult.dataProviderId)
        set("PublicKeyCertificateId" to certificateId)
        set("DataProviderDetails").to(dataProviderDetails)
      }
    }
  }
}
