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
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerDetails

class MeasurementConsumerReader : SpannerReader<MeasurementConsumerReader.Result>() {
  data class Result(
    val measurementConsumer: MeasurementConsumer,
    val measurementConsumerId: Long,
    val externalMeasurementConsumerId: Long,
  )

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumers.MeasurementConsumerId,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      MeasurementConsumers.MeasurementConsumerDetails,
      MeasurementConsumerCertificates.ExternalMeasurementConsumerCertificateId,
      Certificates.SubjectKeyIdentifier,
      Certificates.NotValidBefore,
      Certificates.NotValidAfter,
      Certificates.RevocationState,
      Certificates.CertificateDetails
    FROM MeasurementConsumers
    JOIN MeasurementConsumerCertificates ON (
      MeasurementConsumerCertificates.MeasurementConsumerId = MeasurementConsumers.MeasurementConsumerId
      AND MeasurementConsumerCertificates.CertificateId = MeasurementConsumers.PublicKeyCertificateId
    )
    JOIN Certificates USING (CertificateId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildMeasurementConsumer(struct),
      struct.getLong("MeasurementConsumerId"),
      struct.getLong("ExternalMeasurementConsumerId"),
    )

  private fun buildMeasurementConsumer(struct: Struct): MeasurementConsumer =
    MeasurementConsumer.newBuilder()
      .apply {
        externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
        details =
          struct.getProtoMessage(
            "MeasurementConsumerDetails",
            MeasurementConsumerDetails.getDefaultInstance(),
          )
        certificate = CertificateReader.buildMeasurementConsumerCertificate(struct)
      }
      .build()

  suspend fun readByExternalMeasurementConsumerId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE ExternalMeasurementConsumerId = @externalMeasurementConsumerId")
        bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
      }
      .execute(readContext)
      .singleOrNullIfEmpty()
  }

  companion object {
    /** Reads the [InternalId] for a MeasurementConsumer given its [ExternalId]. */
    suspend fun readMeasurementConsumerId(
      readContext: AsyncDatabaseClient.ReadContext,
      externalMeasurementConsumerId: ExternalId,
    ): InternalId? {
      val column = "MeasurementConsumerId"
      val row: Struct =
        readContext.readRowUsingIndex(
          "MeasurementConsumers",
          "MeasurementConsumersByExternalId",
          Key.of(externalMeasurementConsumerId.value),
          column,
        ) ?: return null
      return row.getInternalId(column)
    }

    /** Reads [InternalId]s for MeasurementConsumers given their [ExternalId]s. * */
    suspend fun readInternalIdsByExternalIds(
      readContext: AsyncDatabaseClient.ReadContext,
      externalMeasurementConsumerIds: Collection<ExternalId>,
    ): Map<ExternalId, InternalId> {
      if (externalMeasurementConsumerIds.isEmpty()) {
        return emptyMap()
      }

      val keySet =
        KeySet.newBuilder()
          .apply {
            for (externalId in externalMeasurementConsumerIds) {
              addKey(Key.of(externalId.value))
            }
          }
          .build()

      return buildMap {
        readContext
          .readUsingIndex(
            "MeasurementConsumers",
            "MeasurementConsumersByExternalId",
            keySet,
            listOf("ExternalMeasurementConsumerId", "MeasurementConsumerId"),
          )
          .collect { struct ->
            put(
              ExternalId(struct.getLong("ExternalMeasurementConsumerId")),
              InternalId(struct.getLong("MeasurementConsumerId")),
            )
          }
      }
    }
  }
}
