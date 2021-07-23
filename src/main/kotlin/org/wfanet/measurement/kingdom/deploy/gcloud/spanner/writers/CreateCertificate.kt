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

import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader.OwnerType
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

class CreateCertificate(private val certificate: Certificate, val ownerType: OwnerType) :
  SpannerWriter<Certificate, Certificate>() {

  override suspend fun TransactionScope.runTransaction(): Certificate {
    val certificateId = idGenerator.generateInternalId()
    val externalMapId = idGenerator.generateExternalId()
    certificate.toInsertMutation(certificateId).bufferTo(transactionContext)
    createCertificateMapTableMutation(
      getOwnerNameAndId(transactionContext),
      certificateId,
      externalMapId
    )
      .bufferTo(transactionContext)
    return certificate.toBuilder().setExternalCertificateId(externalMapId.value).build()
  }

  override fun ResultScope<Certificate>.buildResult(): Certificate {
    return checkNotNull(transactionResult)
  }
  private suspend fun getOwnerNameAndId(
    transactionContext: AsyncDatabaseClient.TransactionContext
  ): InternalId {
    return when (ownerType) {
      OwnerType.MEASUREMENT_CONSUMER -> {
        val measurementConsumerId =
          MeasurementConsumerReader()
            .readExternalIdOrNull(
              transactionContext,
              ExternalId(certificate.externalMeasurementConsumerId)
            )
            ?.measurementConsumerId
            ?: throw KingdomInternalException(
              KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
            )
        InternalId(measurementConsumerId)
      }
      OwnerType.DATA_PROVIDER -> {
        val dataProviderId =
          DataProviderReader()
            .readExternalIdOrNull(
              transactionContext,
              ExternalId(certificate.externalDataProviderId)
            )
            ?.dataProviderId
            ?: throw KingdomInternalException(KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND)
        InternalId(dataProviderId)
      }
      OwnerType.DUCHY -> TODO("uakyol implement duchy support after duchy config is implemented")
    }
  }

  private fun createCertificateMapTableMutation(
    internalOwnerId: InternalId,
    internalCertificateId: InternalId,
    externalMapId: ExternalId
  ): Mutation {
    val tableName = "${ownerType.tableName}Certificates"
    val internalIdField = "${ownerType.tableName}Id"
    val externalIdField = "External${ownerType.tableName}CertificateId"
    return insertMutation(tableName) {
      set(internalIdField to internalOwnerId.value)
      set("CertificateId" to internalCertificateId.value)
      set(externalIdField to externalMapId.value)
    }
  }
}

fun Certificate.toInsertMutation(internalId: InternalId): Mutation {
  return insertMutation("Certificates") {
    set("CertificateId" to internalId.value)
    set("SubjectKeyIdentifier" to subjectKeyIdentifier.toGcloudByteArray())
    set("NotValidBefore" to notValidBefore.toGcloudTimestamp())
    set("NotValidAfter" to notValidAfter.toGcloudTimestamp())
    set("RevocationState" to revocationState)
    set("CertificateDetails" to details)
    setJson("CertificateDetailsJson" to details)
  }
}
