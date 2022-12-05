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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteString
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds

class CertificateReader(private val parentType: ParentType) :
  BaseSpannerReader<CertificateReader.Result>() {
  data class Result(
    val certificate: Certificate,
    val certificateId: InternalId,
    val isNotYetActive: Boolean,
    val isExpired: Boolean,
    val isValid: Boolean,
  )

  enum class ParentType(private val prefix: String) {
    DATA_PROVIDER("DataProvider"),
    MEASUREMENT_CONSUMER("MeasurementConsumer"),
    DUCHY("Duchy"),
    MODEL_PROVIDER("ModelProvider");

    val idColumnName: String = "${prefix}Id"
    val externalCertificateIdColumnName: String = "External${prefix}CertificateId"
    val certificatesTableName: String = "${prefix}Certificates"

    val externalIdColumnName: String?
      get() =
        when (this) {
          DATA_PROVIDER,
          MEASUREMENT_CONSUMER,
          MODEL_PROVIDER -> "External${prefix}Id"
          DUCHY -> null
        }

    val tableName: String?
      get() =
        when (this) {
          DATA_PROVIDER,
          MEASUREMENT_CONSUMER,
          MODEL_PROVIDER -> "${prefix}s"
          DUCHY -> null
        }
  }

  override val builder: Statement.Builder = Statement.newBuilder(buildBaseSql(parentType))

  /** Fills [builder], returning this [CertificateReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): CertificateReader {
    builder.fill()
    return this
  }

  fun bindWhereClause(parentId: InternalId, externalCertificateId: ExternalId): CertificateReader {
    return fillStatementBuilder {
      appendClause(
        """
        WHERE
          ${parentType.idColumnName} = @parentId
          AND ${parentType.externalCertificateIdColumnName} = @externalCertificateId
        """
          .trimIndent()
      )
      bind("parentId" to parentId)
      bind("externalCertificateId" to externalCertificateId)
    }
  }

  fun bindWhereClause(
    externalParentId: ExternalId,
    externalCertificateId: ExternalId
  ): CertificateReader {
    return fillStatementBuilder {
      appendClause(
        """
        WHERE
          ${parentType.externalIdColumnName} = @externalParentId
          AND ${parentType.externalCertificateIdColumnName} = @externalCertificateId
        """
          .trimIndent()
      )
      bind("externalParentId" to externalParentId)
      bind("externalCertificateId" to externalCertificateId)
    }
  }

  override suspend fun translate(struct: Struct): Result {
    val certificateId = struct.getInternalId("CertificateId")
    val isNotYetActive = struct.getBoolean("IsNotYetActive")
    val isExpired = struct.getBoolean("IsExpired")
    val isValid = struct.getBoolean("IsValid")
    return when (parentType) {
      ParentType.DATA_PROVIDER ->
        Result(
          buildDataProviderCertificate(struct),
          certificateId,
          isNotYetActive,
          isExpired,
          isValid
        )
      ParentType.MEASUREMENT_CONSUMER ->
        Result(
          buildMeasurementConsumerCertificate(struct),
          certificateId,
          isNotYetActive,
          isExpired,
          isValid
        )
      ParentType.DUCHY -> {
        val duchyId = struct.getLong("DuchyId")
        val externalDuchyId =
          checkNotNull(DuchyIds.getExternalId(duchyId)) {
            "Duchy with internal ID $duchyId not found"
          }
        Result(
          buildDuchyCertificate(externalDuchyId, struct),
          certificateId,
          isNotYetActive,
          isExpired,
          isValid
        )
      }
      ParentType.MODEL_PROVIDER ->
        Result(
          buildModelProviderCertificate(struct),
          certificateId,
          isNotYetActive,
          isExpired,
          isValid
        )
    }
  }

  suspend fun readDataProviderCertificateIdByExternalId(
    readContext: AsyncDatabaseClient.ReadContext,
    dataProviderId: InternalId,
    externalCertificateId: ExternalId
  ): InternalId? {
    val idColumn = "CertificateId"
    return readContext
      .readRowUsingIndex(
        "DataProviderCertificates",
        "DataProviderCertificatesByExternalId",
        Key.of(dataProviderId.value, externalCertificateId.value),
        idColumn
      )
      ?.let { struct -> InternalId(struct.getLong(idColumn)) }
  }

  suspend fun readMeasurementConsumerCertificateIdByExternalId(
    readContext: AsyncDatabaseClient.ReadContext,
    measurementConsumerId: InternalId,
    externalCertificateId: ExternalId
  ): InternalId? {
    val idColumn = "CertificateId"
    return readContext
      .readRowUsingIndex(
        "MeasurementConsumerCertificates",
        "MeasurementConsumerCertificatesByExternalId",
        Key.of(measurementConsumerId.value, externalCertificateId.value),
        idColumn
      )
      ?.let { struct -> InternalId(struct.getLong(idColumn)) }
  }

  companion object {
    private fun buildBaseSql(parentType: ParentType): String {
      return when (parentType) {
        ParentType.DATA_PROVIDER,
        ParentType.MEASUREMENT_CONSUMER,
        ParentType.MODEL_PROVIDER -> buildExternalIdSql(parentType)
        ParentType.DUCHY -> buildInternalIdSql(parentType)
      }
    }

    /** Builds base SQL when only external ID of parent is known. */
    private fun buildExternalIdSql(parentType: ParentType): String =
      """
      SELECT
        CertificateId,
        SubjectKeyIdentifier,
        NotValidBefore,
        NotValidAfter,
        RevocationState,
        CertificateDetails,
        ${parentType.externalCertificateIdColumnName},
        ${parentType.externalIdColumnName!!},
        CURRENT_TIMESTAMP() < NotValidBefore AS IsNotYetActive,
        CURRENT_TIMESTAMP() > NotValidAfter AS IsExpired,
        RevocationState = ${Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED.number} AND CURRENT_TIMESTAMP() >= NotValidBefore AND CURRENT_TIMESTAMP() <= NotValidAfter AS IsValid,
      FROM
        ${parentType.certificatesTableName}
        JOIN ${parentType.tableName!!} USING (${parentType.idColumnName})
        JOIN Certificates USING (CertificateId)
      """
        .trimIndent()

    /** Builds base SQL when internal ID of parent is known. */
    private fun buildInternalIdSql(parentType: ParentType): String =
      """
      SELECT
        CertificateId,
        SubjectKeyIdentifier,
        NotValidBefore,
        NotValidAfter,
        RevocationState,
        CertificateDetails,
        ${parentType.externalCertificateIdColumnName},
        ${parentType.idColumnName},
        CURRENT_TIMESTAMP() < NotValidBefore AS IsNotYetActive,
        CURRENT_TIMESTAMP() > NotValidAfter AS IsExpired,
        RevocationState = ${Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED.number} AND CURRENT_TIMESTAMP() >= NotValidBefore AND CURRENT_TIMESTAMP() <= NotValidAfter AS IsValid,
      FROM
        ${parentType.certificatesTableName}
        JOIN Certificates USING (CertificateId)
      """
        .trimIndent()

    fun buildDataProviderCertificate(struct: Struct) = certificate {
      fillCommon(struct)

      val parentType = ParentType.DATA_PROVIDER
      externalDataProviderId = struct.getLong(parentType.externalIdColumnName)
      externalCertificateId = struct.getLong(parentType.externalCertificateIdColumnName)
    }

    private fun buildModelProviderCertificate(struct: Struct) = certificate {
      fillCommon(struct)

      val parentType = ParentType.MODEL_PROVIDER
      externalModelProviderId = struct.getLong(parentType.externalIdColumnName)
      externalCertificateId = struct.getLong(parentType.externalCertificateIdColumnName)
    }

    private fun buildMeasurementConsumerCertificate(struct: Struct) = certificate {
      fillCommon(struct)

      val parentType = ParentType.MEASUREMENT_CONSUMER
      externalMeasurementConsumerId = struct.getLong(parentType.externalIdColumnName)
      externalCertificateId = struct.getLong(parentType.externalCertificateIdColumnName)
    }

    fun buildDuchyCertificate(externalDuchyId: String, struct: Struct) = certificate {
      fillCommon(struct)
      this.externalDuchyId = externalDuchyId
      externalCertificateId = struct.getLong("ExternalDuchyCertificateId")
    }

    private fun CertificateKt.Dsl.fillCommon(struct: Struct) {
      subjectKeyIdentifier = struct.getBytesAsByteString("SubjectKeyIdentifier")
      notValidBefore = struct.getTimestamp("NotValidBefore").toProto()
      notValidAfter = struct.getTimestamp("NotValidAfter").toProto()
      revocationState =
        struct.getProtoEnum("RevocationState", Certificate.RevocationState::forNumber)
      details = struct.getProtoMessage("CertificateDetails", Certificate.Details.parser())
    }

    /** Returns the internal Certificate ID for a Duchy Certificate, or `null` if not found. */
    suspend fun getDuchyCertificateId(
      txn: AsyncDatabaseClient.TransactionContext,
      duchyId: InternalId,
      externalDuchyCertificateId: ExternalId
    ): InternalId? {
      val struct =
        txn.readRowUsingIndex(
          "DuchyCertificates",
          "DuchyCertificatesByExternalId",
          Key.of(duchyId.value, externalDuchyCertificateId.value),
          "CertificateId"
        )
          ?: return null
      return InternalId(struct.getLong("CertificateId"))
    }
  }
}
