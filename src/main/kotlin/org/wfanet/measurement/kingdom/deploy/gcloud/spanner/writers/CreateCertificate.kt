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

import com.google.cloud.spanner.ErrorCode as SpannerErrorCode
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
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
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertSubjectKeyIdAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelProviderReader

/**
 * Creates a certificate in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [CertSubjectKeyIdAlreadyExistsException] subjectKeyIdentifier of [Certificate] collides
 *   with a certificate already in the database
 * @throws [DataProviderNotFoundException] DataProvider not found
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * @throws [DuchyNotFoundException] Duchy not found
 */
class CreateCertificate(private val certificate: Certificate) :
  SpannerWriter<Certificate, Certificate>() {

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  private val ownerTableName: String =
    when (certificate.parentCase) {
      Certificate.ParentCase.EXTERNAL_DATA_PROVIDER_ID -> "DataProvider"
      Certificate.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> "MeasurementConsumer"
      Certificate.ParentCase.EXTERNAL_DUCHY_ID -> "Duchy"
      Certificate.ParentCase.EXTERNAL_MODEL_PROVIDER_ID -> "ModelProvider"
      Certificate.ParentCase.PARENT_NOT_SET ->
        throw IllegalArgumentException("Parent field of Certificate is not set")
    }

  override suspend fun TransactionScope.runTransaction(): Certificate {
    val certificateId = idGenerator.generateInternalId()
    val externalMapId = idGenerator.generateExternalId()
    certificate.toInsertMutation(certificateId).bufferTo(transactionContext)
    createCertificateMapTableMutation(
        getOwnerInternalId(transactionContext),
        certificateId.value,
        externalMapId.value
      )
      .bufferTo(transactionContext)
    return certificate.toBuilder().setExternalCertificateId(externalMapId.value).build()
  }

  override fun ResultScope<Certificate>.buildResult(): Certificate {
    return checkNotNull(transactionResult)
  }

  private suspend fun getOwnerInternalId(
    transactionContext: AsyncDatabaseClient.TransactionContext
  ): Long {
    // TODO: change all of the reads below to use index lookups or simple queries.
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (certificate.parentCase) {
      Certificate.ParentCase.EXTERNAL_DATA_PROVIDER_ID -> {
        val externalDataProviderId = ExternalId(certificate.externalDataProviderId)
        DataProviderReader()
          .readByExternalDataProviderId(transactionContext, externalDataProviderId)
          ?.dataProviderId
          ?: throw DataProviderNotFoundException(externalDataProviderId)
      }
      Certificate.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> {
        val externalMeasurementConsumerId = ExternalId(certificate.externalMeasurementConsumerId)
        MeasurementConsumerReader()
          .readByExternalMeasurementConsumerId(transactionContext, externalMeasurementConsumerId)
          ?.measurementConsumerId
          ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)
      }
      Certificate.ParentCase.EXTERNAL_DUCHY_ID ->
        DuchyIds.getInternalId(certificate.externalDuchyId)
          ?: throw DuchyNotFoundException(certificate.externalDuchyId)
      Certificate.ParentCase.EXTERNAL_MODEL_PROVIDER_ID -> {
        val externalModelProviderId = ExternalId(certificate.externalModelProviderId)
        ModelProviderReader()
          .readByExternalModelProviderId(transactionContext, externalModelProviderId)
          ?.modelProviderId
          ?: throw ModelProviderNotFoundException(externalModelProviderId)
      }
      Certificate.ParentCase.PARENT_NOT_SET ->
        throw IllegalArgumentException("Parent field of Certificate is not set")
    }
  }

  private fun createCertificateMapTableMutation(
    internalOwnerId: Long,
    internalCertificateId: Long,
    externalMapId: Long
  ): Mutation {
    val tableName = "${ownerTableName}Certificates"
    val internalIdField = "${ownerTableName}Id"
    val externalIdField = "External${ownerTableName}CertificateId"
    return insertMutation(tableName) {
      set(internalIdField to internalOwnerId)
      set("CertificateId" to internalCertificateId)
      set(externalIdField to externalMapId)
    }
  }

  override suspend fun handleSpannerException(e: SpannerException): Certificate? {
    when (e.errorCode) {
      SpannerErrorCode.ALREADY_EXISTS -> throw CertSubjectKeyIdAlreadyExistsException()
      else -> throw e
    }
  }
}

fun Certificate.toInsertMutation(internalId: InternalId): Mutation {
  return insertMutation("Certificates") {
    set("CertificateId" to internalId)
    set("SubjectKeyIdentifier" to subjectKeyIdentifier.toGcloudByteArray())
    set("NotValidBefore" to notValidBefore.toGcloudTimestamp())
    set("NotValidAfter" to notValidAfter.toGcloudTimestamp())
    set("RevocationState" to revocationState)
    set("CertificateDetails" to details)
    setJson("CertificateDetailsJson" to details)
  }
}
