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

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteString
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Certificate

class CertificateReader(val owner: Owner) : SpannerReader<CertificateReader.Result>() {
  data class Result(val certificate: Certificate, val certificateId: Long)

  enum class Owner(val tableName: String) {
    DATA_PROVIDER("DataProvider"),
    MEASUREMENT_CONSUMER("MeasurementConsumer"),
    DUCHY("Duchy"),
  }

  override val baseSql: String =
    """
    SELECT
      ${owner.tableName}Certificates.CertificateId,
      Certificates.SubjectKeyIdentifier,
      Certificates.NotValidBefore,
      Certificates.NotValidAfter,
      Certificates.RevocationState,
      Certificates.CertificateDetails,
      ${owner.tableName}Certificates.External${owner.tableName}CertificateId,
      ${owner.tableName}Certificates.${owner.tableName}Id,
      ${owner.tableName}s.External${owner.tableName}Id
    FROM ${owner.tableName}Certificates
    JOIN ${owner.tableName}s USING (${owner.tableName}Id)
    JOIN Certificates USING (CertificateId)
    """.trimIndent()

  override val externalIdColumn: String =
    "${owner.tableName}Certificates.External${owner.tableName}CertificateId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildCertificate(struct), struct.getLong("CertificateId"))

  private fun populateExternalId(
    certificateBuilder: Certificate.Builder,
    struct: Struct
  ): Certificate {
    val externalResourceIdColumn = "External${owner.tableName}Id"
    if (owner == Owner.MEASUREMENT_CONSUMER) {
      return certificateBuilder
        .setExternalMeasurementConsumerId(struct.getLong(externalResourceIdColumn))
        .build()
    }
    if (owner == Owner.DATA_PROVIDER) {
      return certificateBuilder
        .setExternalDataProviderId(struct.getLong(externalResourceIdColumn))
        .build()
    }

    return TODO("uakyol implement duchy support after duchy config is implemented")
  }

  private fun buildCertificate(struct: Struct): Certificate {
    val certificateBuilder =
      Certificate.newBuilder().apply {
        externalCertificateId = struct.getLong("External${owner.tableName}CertificateId")
        subjectKeyIdentifier = struct.getBytesAsByteString("SubjectKeyIdentifier")
        notValidBefore = struct.getTimestamp("NotValidBefore").toProto()
        notValidAfter = struct.getTimestamp("NotValidAfter").toProto()
        revocationState =
          struct.getProtoEnum("RevocationState", Certificate.RevocationState::forNumber)
        details = struct.getProtoMessage("CertificateDetails", Certificate.Details.parser())
      }
    return populateExternalId(certificateBuilder, struct)
  }
}
