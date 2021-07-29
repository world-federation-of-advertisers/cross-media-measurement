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
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest

class CertificateReader(val request: GetCertificateRequest) :
  SpannerReader<CertificateReader.Result>() {
  data class Result(val certificate: Certificate, val certificateId: Long)
  private val tableName: String
  init {
    tableName =
      when (request.parentCase) {
        GetCertificateRequest.ParentCase.EXTERNAL_DATA_PROVIDER_ID -> "DataProvider"
        GetCertificateRequest.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> "MeasurementConsumer"
        GetCertificateRequest.ParentCase.EXTERNAL_DUCHY_ID -> "Duchy"
        GetCertificateRequest.ParentCase.PARENT_NOT_SET ->
          throw IllegalArgumentException("Parent field of GetCertificateRequest is not set")
      }
  }

  override val baseSql: String =
    """
    SELECT
      ${tableName}Certificates.CertificateId,
      Certificates.SubjectKeyIdentifier,
      Certificates.NotValidBefore,
      Certificates.NotValidAfter,
      Certificates.RevocationState,
      Certificates.CertificateDetails,
      ${tableName}Certificates.External${tableName}CertificateId,
      ${tableName}Certificates.${tableName}Id,
      ${tableName}s.External${tableName}Id
    FROM ${tableName}Certificates
    JOIN ${tableName}s USING (${tableName}Id)
    JOIN Certificates USING (CertificateId)
    """.trimIndent()

  override val externalIdColumn: String =
    "${tableName}Certificates.External${tableName}CertificateId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildCertificate(struct), struct.getLong("CertificateId"))

  private fun populateExternalId(
    certificateBuilder: Certificate.Builder,
    struct: Struct
  ): Certificate {
    val externalResourceIdColumn = "External${tableName}Id"
    return certificateBuilder
      .apply {
        when (request.parentCase) {
          GetCertificateRequest.ParentCase.EXTERNAL_DATA_PROVIDER_ID ->
            externalDataProviderId = struct.getLong(externalResourceIdColumn)
          GetCertificateRequest.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID ->
            externalMeasurementConsumerId = struct.getLong(externalResourceIdColumn)
          GetCertificateRequest.ParentCase.EXTERNAL_DUCHY_ID ->
            TODO("uakyol implement duchy support after duchy config is implemented")
          GetCertificateRequest.ParentCase.PARENT_NOT_SET ->
            throw IllegalArgumentException("Parent field of GetCertificateRequest is not set")
        }
      }
      .build()
  }

  private fun buildCertificate(struct: Struct): Certificate {
    val certificateBuilder =
      Certificate.newBuilder().apply {
        externalCertificateId = struct.getLong("External${tableName}CertificateId")
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
