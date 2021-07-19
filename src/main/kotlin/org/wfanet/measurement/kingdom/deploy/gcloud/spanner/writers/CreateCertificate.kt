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
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader.Owner

private const val DATA_PROVIDER = "DataProvider"
private const val MEASUREMENT_CONSUMER = "MeasurementConsumer"
private const val DUCHY = "Duchy"

class CreateCertificate(private val certificate: Certificate) :
  SpannerWriter<Certificate, Certificate>() {
  data class InternalResource(val ownerTableName: String, val id: InternalId)

  override suspend fun TransactionScope.runTransaction(): Certificate {
    val internalResource = getInternalResourceNameAndId(transactionContext)
    val certificateId = idGenerator.generateInternalId()
    val externalMapId = idGenerator.generateExternalId()
    certificate.toInsertMutation(certificateId).bufferTo(transactionContext)
    createCertificateMapTableMutation(
      internalResource.ownerTableName,
      internalResource.id,
      certificateId,
      externalMapId
    )
      .bufferTo(transactionContext)
    return certificate.toBuilder().setExternalCertificateId(externalMapId.value).build()
  }

  override fun ResultScope<Certificate>.buildResult(): Certificate {
    return checkNotNull(transactionResult)
  }
  private suspend fun getInternalResourceNameAndId(
    transactionContext: AsyncDatabaseClient.TransactionContext
  ): InternalResource {
    if (certificate.hasExternalMeasurementConsumerId()) {
      val measuerementConsumerId =
        MeasurementConsumerReader()
          .readExternalId(transactionContext, ExternalId(certificate.externalMeasurementConsumerId))
          .measurementConsumerId
      return InternalResource(
        Owner.MEASUREMENT_CONSUMER.tableName,
        InternalId(measuerementConsumerId)
      )
    }
    if (certificate.hasExternalDataProviderId()) {
      val dataProviderId =
        DataProviderReader()
          .readExternalId(transactionContext, ExternalId(certificate.externalDataProviderId))
          .dataProviderId
      return InternalResource(Owner.DATA_PROVIDER.tableName, InternalId(dataProviderId))
    }
    return TODO("uakyol implement duchy support after duchy config is implemented")
  }

  private fun createCertificateMapTableMutation(
    ownerTableName: String,
    internalId: InternalId,
    internalCertificateId: InternalId,
    externalMapId: ExternalId
  ): Mutation {
    val tableName = "${ownerTableName}Certificates"
    val internalIdField = "${ownerTableName}Id"
    val externalIdField = "External${ownerTableName}CertificateId"
    return insertMutation(tableName) {
      set(internalIdField to internalId.value)
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
