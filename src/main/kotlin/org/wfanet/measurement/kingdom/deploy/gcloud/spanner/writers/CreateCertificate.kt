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

class CreateCertificate(private val certificate: Certificate) :
  SpannerWriter<Certificate, Certificate>() {
  data class InternalResource(val name: String, val id: InternalId)

  // https://github.com/protocolbuffers/protobuf/releases/tag/v3.15.0
  override suspend fun TransactionScope.runTransaction(): Certificate {
    val internalResource = getInternalResourceNameAndId(transactionContext)
    val certificateId = idGenerator.generateInternalId()
    val externalMapId = idGenerator.generateExternalId()
    certificate.toInsertMutation(certificateId).bufferTo(transactionContext)
    createCertificateMapTableMutation(
      internalResource.name,
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
    if (certificate.externalMeasurementConsumerId != 0L) {
      val measuerementConsumerId =
        MeasurementConsumerReader()
          .readExternalId(transactionContext, ExternalId(certificate.externalMeasurementConsumerId))
          .measurementConsumerId
      return InternalResource("MeasurementConsumer", InternalId(measuerementConsumerId))
    } else if (certificate.externalDataProviderId != 0L) {
      val dataProviderId =
        DataProviderReader()
          .readExternalId(transactionContext, ExternalId(certificate.externalDataProviderId))
          .dataProviderId
      return InternalResource("DataProvider", InternalId(dataProviderId))
    }
    return InternalResource("Duchy", InternalId(certificate.externalDuchyId.toLong()))
  }

  private fun createCertificateMapTableMutation(
    resourceName: String,
    internalId: InternalId,
    internalCertificateId: InternalId,
    externalMapId: ExternalId
  ): Mutation {
    val tableName = "${resourceName}Certificates"
    val internalIdField = "${resourceName}Id"
    val externalIdField = "External${resourceName}CertificateId"
    return insertMutation(tableName) {
      set(internalIdField to internalId.value)
      set(externalIdField to internalCertificateId.value)
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
