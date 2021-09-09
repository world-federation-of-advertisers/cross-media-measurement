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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteString
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds

class CertificateReader(private val parentType: ParentType) : BaseSpannerReader<Certificate>() {
  enum class ParentType(private val prefix: String) {
    DATA_PROVIDER("DataProvider"),
    MEASUREMENT_CONSUMER("MeasurementConsumer"),
    DUCHY("Duchy");

    val idColumnName: String = "${prefix}Id"
    val externalCertificateIdColumnName: String = "External${prefix}CertificateId"
    val certificatesTableName: String = "${prefix}Certificates"

    val externalIdColumnName: String?
      get() =
        when (this) {
          DATA_PROVIDER, MEASUREMENT_CONSUMER -> "External${prefix}Id"
          DUCHY -> null
        }

    val tableName: String?
      get() =
        when (this) {
          DATA_PROVIDER, MEASUREMENT_CONSUMER -> "${prefix}s"
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
        """.trimIndent()
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
        """.trimIndent()
      )
      bind("externalParentId" to externalParentId)
      bind("externalCertificateId" to externalCertificateId)
    }
  }

  override suspend fun translate(struct: Struct): Certificate {
    return when (parentType) {
      ParentType.DATA_PROVIDER -> buildDataProviderCertificate(struct)
      ParentType.MEASUREMENT_CONSUMER -> buildMeasurementConsumerCertificate(struct)
      ParentType.DUCHY -> {
        val duchyId = struct.getLong("DuchyId")
        val externalDuchyId =
          checkNotNull(DuchyIds.getExternalId(duchyId)) {
            "Duchy with internal ID $duchyId not found"
          }
        buildDuchyCertificate(externalDuchyId, struct)
      }
    }
  }

  companion object {
    private fun buildBaseSql(parentType: ParentType): String {
      return when (parentType) {
        ParentType.DATA_PROVIDER, ParentType.MEASUREMENT_CONSUMER -> buildExternalIdSql(parentType)
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
        ${parentType.externalIdColumnName!!}
      FROM
        ${parentType.certificatesTableName}
        JOIN ${parentType.tableName!!} USING (${parentType.idColumnName})
        JOIN Certificates USING (CertificateId)
      """.trimIndent()

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
        ${parentType.idColumnName}
      FROM
        ${parentType.certificatesTableName}
        JOIN Certificates USING (CertificateId)
      """.trimIndent()

    fun buildDataProviderCertificate(struct: Struct) = certificate {
      fillCommon(struct)

      val parentType = ParentType.DATA_PROVIDER
      externalDataProviderId = struct.getLong(parentType.externalIdColumnName)
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
  }
}
