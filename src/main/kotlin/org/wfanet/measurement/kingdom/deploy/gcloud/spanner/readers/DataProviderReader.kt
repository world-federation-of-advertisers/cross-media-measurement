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
import com.google.cloud.spanner.Struct
import com.google.type.interval
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderDetails
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.modelLineKey
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException

class DataProviderReader : SpannerReader<DataProviderReader.Result>() {
  data class Result(
    val dataProvider: DataProvider,
    val dataProviderId: Long,
    val certificateId: Long,
    val certificateValid: Boolean,
  )

  override val baseSql: String =
    """
    SELECT
      DataProviders.DataProviderId,
      DataProviders.ExternalDataProviderId,
      DataProviders.DataProviderDetails,
      DataProviderCertificates.ExternalDataProviderCertificateId,
      Certificates.CertificateId,
      Certificates.SubjectKeyIdentifier,
      Certificates.NotValidBefore,
      Certificates.NotValidAfter,
      Certificates.RevocationState,
      Certificates.CertificateDetails,
      RevocationState = ${Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED.number} AND CURRENT_TIMESTAMP() >= NotValidBefore AND CURRENT_TIMESTAMP() <= NotValidAfter AS IsValid,
      ARRAY(
        SELECT AS STRUCT
          DataProviderRequiredDuchies.DuchyId
        FROM
          DataProviderRequiredDuchies
        WHERE
          DataProviderRequiredDuchies.DataProviderId = DataProviders.DataProviderId
      ) AS DataProviderRequiredDuchies,
      ARRAY(
        SELECT AS STRUCT
          ExternalModelProviderId,
          ExternalModelSuiteId,
          ExternalModelLineId,
          StartTime,
          EndTime,
        FROM
          DataProviderAvailabilityIntervals
          JOIN ModelLines USING (ModelProviderId, ModelSuiteId, ModelLineId)
          JOIN ModelSuites USING (ModelProviderId, ModelSuiteId)
          JOIN ModelProviders USING (ModelProviderId)
        WHERE
          DataProviderAvailabilityIntervals.DataProviderId = DataProviders.DataProviderId
      ) AS DataAvailabilityIntervals,
    FROM DataProviders
    JOIN DataProviderCertificates ON (
      DataProviderCertificates.DataProviderId = DataProviders.DataProviderId
      AND DataProviderCertificates.CertificateId = DataProviders.PublicKeyCertificateId
    )
    JOIN Certificates USING (CertificateId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildDataProvider(struct),
      struct.getLong("DataProviderId"),
      struct.getLong("CertificateId"),
      struct.getBoolean("IsValid"),
    )

  suspend fun readByExternalDataProviderId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE ExternalDataProviderId = @externalDataProviderId")
        bind("externalDataProviderId").to(externalDataProviderId.value)
      }
      .execute(readContext)
      .singleOrNullIfEmpty()
  }

  /**
   * Reads the [DataProvider]s by [externalDataProviderIds].
   *
   * @return list of [Result] in the same iteration order as [externalDataProviderIds]
   * @throws DataProviderNotFoundException if no [DataProvider] is found for a specified external ID
   */
  suspend fun readByExternalDataProviderIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderIds: Iterable<ExternalId>,
  ): List<Result> {
    val resultsByExternalId: Map<ExternalId, Result> =
      fillStatementBuilder {
          appendClause("WHERE ExternalDataProviderId IN UNNEST(@externalDataProviderIds)")
          bind("externalDataProviderIds")
            .toInt64Array(externalDataProviderIds.map(ExternalId::value))
        }
        .execute(readContext)
        .toList()
        .associateBy { ExternalId(it.dataProvider.externalDataProviderId) }

    return externalDataProviderIds.map {
      resultsByExternalId[it] ?: throw DataProviderNotFoundException(it)
    }
  }

  private fun buildDataProvider(struct: Struct) = dataProvider {
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
    details =
      struct.getProtoMessage("DataProviderDetails", DataProviderDetails.getDefaultInstance())
    certificate = CertificateReader.buildDataProviderCertificate(struct)
    requiredExternalDuchyIds += buildExternalDuchyIdList(struct)
    dataAvailabilityIntervals +=
      struct.getStructList("DataAvailabilityIntervals").map {
        DataProviderKt.dataAvailabilityMapEntry {
          key = modelLineKey {
            externalModelProviderId = it.getLong("ExternalModelProviderId")
            externalModelSuiteId = it.getLong("ExternalModelSuiteId")
            externalModelLineId = it.getLong("ExternalModelLineId")
          }
          value = interval {
            startTime = it.getTimestamp("StartTime").toProto()
            endTime = it.getTimestamp("EndTime").toProto()
          }
        }
      }
  }

  private fun buildExternalDuchyIdList(struct: Struct): List<String> {
    return struct.getStructList("DataProviderRequiredDuchies").map {
      checkNotNull(DuchyIds.getExternalId(it.getLong("DuchyId"))) {
        "Duchy with internal ID ${it.getLong("DuchyId")} not found"
      }
    }
  }

  companion object {
    /** Reads the [InternalId] for a DataProvider given its [ExternalId]. */
    suspend fun readDataProviderId(
      readContext: AsyncDatabaseClient.ReadContext,
      externalDataProviderId: ExternalId,
    ): InternalId? {
      val column = "DataProviderId"
      val row: Struct =
        readContext.readRowUsingIndex(
          "DataProviders",
          "DataProvidersByExternalId",
          Key.of(externalDataProviderId.value),
          column,
        ) ?: return null

      return row.getInternalId(column)
    }
  }
}
