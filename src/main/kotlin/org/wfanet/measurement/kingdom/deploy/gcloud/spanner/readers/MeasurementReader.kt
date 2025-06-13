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
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteString
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementDetails
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundException

class MeasurementReader(private val view: Measurement.View, measurementsIndex: Index = Index.NONE) :
  SpannerReader<MeasurementReader.Result>() {

  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val createRequestId: String?,
    val measurement: Measurement,
  )

  enum class Index(internal val sql: String) {
    NONE(""),
    CREATE_REQUEST_ID("@{FORCE_INDEX=MeasurementsByCreateRequestId}"),
    CONTINUATION_TOKEN("@{FORCE_INDEX=MeasurementsByContinuationToken}"),
  }

  override val baseSql: String =
    """
    @{spanner_emulator.disable_query_null_filtered_index_check=true}
    WITH FilteredMeasurements AS (
      SELECT *
      FROM
        Measurements${measurementsIndex.sql}
        JOIN MeasurementConsumers USING (MeasurementConsumerId)
    """
      .trimIndent()

  private var filled = false

  /** Optional ORDER BY clause that is appended at the end of the overall query. */
  var orderByClause: String? = null
    set(value) {
      check(!filled) { "Statement builder already filled" }
      field = value
    }

  /**
   * Fills the statement builder for the query.
   *
   * @param block a function for filling the statement builder for the Measurements table subquery.
   */
  override fun fillStatementBuilder(block: Statement.Builder.() -> Unit): SpannerReader<Result> {
    check(!filled) { "Statement builder already filled" }
    filled = true

    return super.fillStatementBuilder {
      block()
      append(")\n")

      val sql =
        when (view) {
          Measurement.View.DEFAULT -> DEFAULT_VIEW_SQL
          Measurement.View.COMPUTATION -> COMPUTATION_VIEW_SQL
          Measurement.View.COMPUTATION_STATS -> COMPUTATION_STATS_VIEW_SQL
          Measurement.View.UNRECOGNIZED -> error("Invalid view $view")
        }
      appendClause(sql)

      val orderByClause = orderByClause
      if (orderByClause != null) {
        appendClause(orderByClause)
      }
    }
  }

  override suspend fun translate(struct: Struct): Result =
    Result(
      struct.getInternalId("MeasurementConsumerId"),
      struct.getInternalId("MeasurementId"),
      if (struct.isNull("CreateRequestId")) null else struct.getString("CreateRequestId"),
      buildMeasurement(struct),
    )

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: ExternalId,
    externalMeasurementId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE ExternalMeasurementConsumerId = @externalMeasurementConsumerId
            AND ExternalMeasurementId = @externalMeasurementId
          """
            .trimIndent()
        )
        bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
        bind("externalMeasurementId").to(externalMeasurementId.value)
      }
      .execute(readContext)
      .singleOrNullIfEmpty()
  }

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: ExternalId,
    externalMeasurementIds: List<ExternalId>,
  ): List<Result> {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE ExternalMeasurementConsumerId = @externalMeasurementConsumerId
            AND ExternalMeasurementId IN UNNEST(@ids)
          """
            .trimIndent()
        )
        bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
        bind("ids").toInt64Array(externalMeasurementIds.map { it.value })
      }
      .execute(readContext)
      .toList()
  }

  suspend fun readByExternalComputationId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalComputationId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE ExternalComputationId = @externalComputationId")
        bind("externalComputationId").to(externalComputationId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  private fun buildMeasurement(struct: Struct): Measurement {
    return measurement {
      when (view) {
        Measurement.View.DEFAULT -> fillDefaultView(struct)
        Measurement.View.COMPUTATION -> fillComputationView(struct)
        Measurement.View.COMPUTATION_STATS -> fillComputationStatsView(struct)
        Measurement.View.UNRECOGNIZED ->
          throw IllegalArgumentException("View field of GetMeasurementRequest is not set")
      }
    }
  }

  companion object {
    suspend fun readMeasurementState(
      readContext: AsyncDatabaseClient.ReadContext,
      measurementConsumerId: InternalId,
      measurementId: InternalId,
    ): Measurement.State {
      val column = "State"
      return readContext
        .readRow(
          "Measurements",
          Key.of(measurementConsumerId.value, measurementId.value),
          listOf(column),
        )
        ?.getProtoEnum(column, Measurement.State::forNumber)
        ?: throw MeasurementNotFoundException { "Measurement not found $measurementId" }
    }

    /**
     * Returns a [Key] for the specified external Measurement ID and external Measurement consumer
     * ID pair.
     */
    suspend fun readKeyByExternalIds(
      readContext: AsyncDatabaseClient.ReadContext,
      externalMeasurementConsumerId: ExternalId,
      externalMeasurementId: ExternalId,
    ): Key? {
      val sql =
        """
        SELECT
          Measurements.MeasurementId AS measurementId,
          Measurements.MeasurementConsumerId AS measurementConsumerId
        FROM
          Measurements
          JOIN MeasurementConsumers USING (MeasurementConsumerId)
        """
          .trimIndent()
      val statement =
        statement(sql) {
          appendClause(
            """
            WHERE
              ExternalMeasurementConsumerId = @externalMeasurementConsumerId
              AND ExternalMeasurementId = @externalMeasurementId
            """
              .trimIndent()
          )
          bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
          bind("externalMeasurementId").to(externalMeasurementId.value)
          appendClause("LIMIT 1")
        }

      val row: Struct =
        readContext
          .executeQuery(
            statement,
            Options.tag("reader=MeasurementReader,action=readMeasurementKeyByExternalIds"),
          )
          .singleOrNull() ?: return null

      return Key.of(
        row.getInternalId("measurementConsumerId").value,
        row.getInternalId("measurementId").value,
      )
    }

    /**
     * Returns an Etag [String] for the update time of a [Measurement] specified by Measurement Key
     *
     * @throws [MeasurementNotFoundException] when the Measurement is not found
     */
    suspend fun readEtag(
      readContext: AsyncDatabaseClient.ReadContext,
      measurementKey: Key,
    ): String {
      val column = "UpdateTime"
      val updateTime =
        readContext.readRow("Measurements", measurementKey, listOf(column))?.getTimestamp(column)
          ?: throw MeasurementNotFoundException { "Measurement not found for $measurementKey" }
      return ETags.computeETag(updateTime)
    }

    private val DEFAULT_VIEW_SQL =
      """
      SELECT
        Measurements.MeasurementId,
        Measurements.MeasurementConsumerId,
        Measurements.ExternalMeasurementId,
        Measurements.ExternalComputationId,
        Measurements.ProvidedMeasurementId,
        Measurements.CreateRequestId,
        Measurements.MeasurementDetails,
        Measurements.CreateTime,
        Measurements.UpdateTime,
        Measurements.State AS MeasurementState,
        ExternalMeasurementConsumerId,
        ExternalMeasurementConsumerCertificateId,
        ARRAY(
          SELECT AS STRUCT
            ExternalDataProviderId,
            ExternalDataProviderCertificateId,
            RequisitionDetails
          FROM
            Requisitions
            JOIN DataProviders USING (DataProviderId)
            JOIN DataProviderCertificates ON (
              DataProviderCertificates.DataProviderId = Requisitions.DataProviderId
              AND DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId
            )
          WHERE
            Requisitions.MeasurementConsumerId = Measurements.MeasurementConsumerId
            AND Requisitions.MeasurementId = Measurements.MeasurementId
        ) AS Requisitions,
        ARRAY(
          SELECT AS STRUCT
            DuchyMeasurementResults.DuchyId,
            ExternalDuchyCertificateId,
            EncryptedResult,
            PublicApiVersion
          FROM
            DuchyMeasurementResults
            JOIN DuchyCertificates USING (DuchyId, CertificateId)
          WHERE
            DuchyMeasurementResults.MeasurementConsumerId = Measurements.MeasurementConsumerId
            AND DuchyMeasurementResults.MeasurementId = Measurements.MeasurementId
        ) AS DuchyResults
      FROM
        FilteredMeasurements AS Measurements
        JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
      """
        .trimIndent()

    private val COMPUTATION_VIEW_SQL =
      """
      SELECT
        ExternalMeasurementConsumerId,
        ExternalMeasurementConsumerCertificateId,
        Measurements.MeasurementId,
        Measurements.MeasurementConsumerId,
        Measurements.ExternalMeasurementId,
        Measurements.ExternalComputationId,
        Measurements.ProvidedMeasurementId,
        Measurements.CreateRequestId,
        Measurements.MeasurementDetails,
        Measurements.CreateTime,
        Measurements.UpdateTime,
        Measurements.State AS MeasurementState,
        ARRAY(
          SELECT AS STRUCT
            ExternalDataProviderId,
            Requisitions.UpdateTime,
            Requisitions.ExternalRequisitionId,
            Requisitions.State AS RequisitionState,
            Requisitions.FulfillingDuchyId,
            Requisitions.RequisitionDetails,
            ExternalDataProviderCertificateId,
            SubjectKeyIdentifier,
            NotValidBefore,
            NotValidAfter,
            RevocationState,
            CertificateDetails,
          FROM
            Requisitions
            JOIN DataProviders USING (DataProviderId)
            JOIN DataProviderCertificates ON (
              DataProviderCertificates.DataProviderId = Requisitions.DataProviderId
              AND DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId
            )
            JOIN Certificates USING (CertificateId)
          WHERE
            Requisitions.MeasurementConsumerId = Measurements.MeasurementConsumerId
            AND Requisitions.MeasurementId = Measurements.MeasurementId
        ) AS Requisitions,
        ARRAY(
          SELECT AS STRUCT
            ExternalDuchyCertificateId,
            ComputationParticipants.DuchyId,
            ComputationParticipants.UpdateTime,
            ComputationParticipants.State,
            ComputationParticipants.ParticipantDetails,
            Certificates.SubjectKeyIdentifier,
            Certificates.NotValidBefore,
            Certificates.NotValidAfter,
            Certificates.RevocationState,
            Certificates.CertificateDetails,
            ARRAY(
              SELECT AS STRUCT
                DuchyMeasurementLogEntries.CreateTime,
                DuchyMeasurementLogEntries.ExternalComputationLogEntryId,
                DuchyMeasurementLogEntries.DuchyMeasurementLogDetails,
                MeasurementLogEntries.MeasurementLogDetails
              FROM
                DuchyMeasurementLogEntries
                JOIN MeasurementLogEntries USING (MeasurementConsumerId, MeasurementId, CreateTime)
              WHERE
                DuchyMeasurementLogEntries.DuchyId = ComputationParticipants.DuchyId
                AND DuchyMeasurementLogEntries.MeasurementConsumerId = ComputationParticipants.MeasurementConsumerId
                AND DuchyMeasurementLogEntries.MeasurementId = ComputationParticipants.MeasurementId
                AND MeasurementLogEntries.MeasurementLogDetails.has_error
              ORDER BY MeasurementLogEntries.CreateTime DESC
            ) AS DuchyMeasurementLogEntries
          FROM
            ComputationParticipants
            LEFT JOIN (DuchyCertificates JOIN Certificates USING (CertificateId))
              USING (DuchyId, CertificateId)
          WHERE
            ComputationParticipants.MeasurementConsumerId = Measurements.MeasurementConsumerId
            AND ComputationParticipants.MeasurementId = Measurements.MeasurementId
        ) AS ComputationParticipants,
        ARRAY(
          SELECT AS STRUCT
            DuchyMeasurementResults.DuchyId,
            ExternalDuchyCertificateId,
            EncryptedResult,
            PublicApiVersion
          FROM
            DuchyMeasurementResults
            JOIN DuchyCertificates USING (DuchyId, CertificateId)
          WHERE
            Measurements.MeasurementConsumerId = DuchyMeasurementResults.MeasurementConsumerId
            AND Measurements.MeasurementId = DuchyMeasurementResults.MeasurementId
        ) AS DuchyResults
      FROM
        FilteredMeasurements AS Measurements
        JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
      """
        .trimIndent()

    private val COMPUTATION_STATS_VIEW_SQL =
      """
      SELECT
        ExternalMeasurementConsumerId,
        ExternalMeasurementConsumerCertificateId,
        Measurements.MeasurementId,
        Measurements.MeasurementConsumerId,
        Measurements.ExternalMeasurementId,
        Measurements.ExternalComputationId,
        Measurements.ProvidedMeasurementId,
        Measurements.CreateRequestId,
        Measurements.MeasurementDetails,
        Measurements.CreateTime,
        Measurements.UpdateTime,
        Measurements.State AS MeasurementState,
        ARRAY(
          SELECT AS STRUCT
            ExternalDuchyCertificateId,
            ComputationParticipants.DuchyId,
            ComputationParticipants.UpdateTime,
            ComputationParticipants.State,
            ComputationParticipants.ParticipantDetails,
            Certificates.SubjectKeyIdentifier,
            Certificates.NotValidBefore,
            Certificates.NotValidAfter,
            Certificates.RevocationState,
            Certificates.CertificateDetails,
            ARRAY(
              SELECT AS STRUCT
                DuchyMeasurementLogEntries.CreateTime,
                DuchyMeasurementLogEntries.ExternalComputationLogEntryId,
                DuchyMeasurementLogEntries.DuchyMeasurementLogDetails,
                MeasurementLogEntries.MeasurementLogDetails
              FROM
                DuchyMeasurementLogEntries
                JOIN MeasurementLogEntries USING (MeasurementConsumerId, MeasurementId, CreateTime)
              WHERE
                DuchyMeasurementLogEntries.DuchyId = ComputationParticipants.DuchyId
                AND DuchyMeasurementLogEntries.MeasurementConsumerId = ComputationParticipants.MeasurementConsumerId
                AND DuchyMeasurementLogEntries.MeasurementId = ComputationParticipants.MeasurementId
              ORDER BY MeasurementLogEntries.CreateTime DESC
            ) AS DuchyMeasurementLogEntries
          FROM
            ComputationParticipants
            LEFT JOIN (DuchyCertificates JOIN Certificates USING (CertificateId))
              USING (DuchyId, CertificateId)
          WHERE
            ComputationParticipants.MeasurementConsumerId = Measurements.MeasurementConsumerId
            AND ComputationParticipants.MeasurementId = Measurements.MeasurementId
        ) AS ComputationParticipants,
        ARRAY(
          SELECT AS STRUCT
            DuchyMeasurementResults.DuchyId,
            ExternalDuchyCertificateId,
            EncryptedResult,
            PublicApiVersion
          FROM
            DuchyMeasurementResults
            JOIN DuchyCertificates USING (DuchyId, CertificateId)
          WHERE
            Measurements.MeasurementConsumerId = DuchyMeasurementResults.MeasurementConsumerId
            AND Measurements.MeasurementId = DuchyMeasurementResults.MeasurementId
        ) AS DuchyResults
      FROM
        FilteredMeasurements AS Measurements
        JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
      """
        .trimIndent()
  }
}

private fun MeasurementKt.Dsl.fillMeasurementCommon(struct: Struct) {
  externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
  externalMeasurementId = struct.getLong("ExternalMeasurementId")
  if (!struct.isNull("ExternalComputationId")) {
    externalComputationId = struct.getLong("ExternalComputationId")
  }
  if (!struct.isNull("ProvidedMeasurementId")) {
    providedMeasurementId = struct.getString("ProvidedMeasurementId")
  }
  externalMeasurementConsumerCertificateId =
    struct.getLong("ExternalMeasurementConsumerCertificateId")
  createTime = struct.getTimestamp("CreateTime").toProto()
  updateTime = struct.getTimestamp("UpdateTime").toProto()
  state = struct.getProtoEnum("MeasurementState", Measurement.State::forNumber)
  details = struct.getProtoMessage("MeasurementDetails", MeasurementDetails.getDefaultInstance())
  if (state == Measurement.State.SUCCEEDED) {
    for (duchyResultStruct in struct.getStructList("DuchyResults")) {
      results += resultInfo {
        val duchyId = duchyResultStruct.getLong("DuchyId")
        externalAggregatorDuchyId =
          checkNotNull(DuchyIds.getExternalId(duchyId)) {
            "Duchy with internal ID $duchyId not found"
          }
        externalCertificateId = duchyResultStruct.getLong("ExternalDuchyCertificateId")
        encryptedResult = duchyResultStruct.getBytesAsByteString("EncryptedResult")
        apiVersion = duchyResultStruct.getString("PublicApiVersion")
      }
    }
  }
  etag = ETags.computeETag(struct.getTimestamp("UpdateTime"))
}

private fun MeasurementKt.Dsl.fillDefaultView(struct: Struct) {
  fillMeasurementCommon(struct)

  val measurementSucceeded = state == Measurement.State.SUCCEEDED
  for (requisitionStruct in struct.getStructList("Requisitions")) {
    val requisitionDetails =
      requisitionStruct.getProtoMessage(
        "RequisitionDetails",
        RequisitionDetails.getDefaultInstance(),
      )
    val externalDataProviderId = requisitionStruct.getLong("ExternalDataProviderId")
    val externalDataProviderCertificateId =
      requisitionStruct.getLong("ExternalDataProviderCertificateId")
    dataProviders[externalDataProviderId] = dataProviderValue {
      this.externalDataProviderCertificateId = externalDataProviderCertificateId
      dataProviderPublicKey = requisitionDetails.dataProviderPublicKey
      encryptedRequisitionSpec = requisitionDetails.encryptedRequisitionSpec
      nonceHash = requisitionDetails.nonceHash

      // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting these
      // fields.
      dataProviderPublicKeySignature = requisitionDetails.dataProviderPublicKeySignature
      dataProviderPublicKeySignatureAlgorithmOid =
        requisitionDetails.dataProviderPublicKeySignatureAlgorithmOid
    }

    if (measurementSucceeded && !requisitionDetails.encryptedData.isEmpty) {
      results += resultInfo {
        this.externalDataProviderId = externalDataProviderId
        externalCertificateId =
          if (requisitionDetails.externalCertificateId != 0L) {
            requisitionDetails.externalCertificateId
          } else {
            externalDataProviderCertificateId
          }
        encryptedResult = requisitionDetails.encryptedData
        apiVersion = requisitionDetails.encryptedDataApiVersion.ifEmpty { Version.V2_ALPHA.string }
      }
    }
  }
}

private fun MeasurementKt.Dsl.fillComputationView(struct: Struct) {
  fillMeasurementCommon(struct)
  val requisitionsStructs = struct.getStructList("Requisitions")
  val dataProvidersCount = requisitionsStructs.size

  val externalMeasurementId = ExternalId(struct.getLong("ExternalMeasurementId"))
  val externalMeasurementConsumerId = ExternalId(struct.getLong("ExternalMeasurementConsumerId"))
  val externalComputationId = ExternalId(struct.getLong("ExternalComputationId"))

  // Map of external Duchy ID to ComputationParticipant struct.
  val participantStructs: Map<String, Struct> =
    struct.getStructList("ComputationParticipants").associateBy {
      val duchyId = it.getLong("DuchyId")
      checkNotNull(DuchyIds.getExternalId(duchyId)) { "Duchy with internal ID $duchyId not found" }
    }

  for ((externalDuchyId, participantStruct) in participantStructs) {
    computationParticipants +=
      ComputationParticipantReader.buildComputationParticipant(
        externalMeasurementConsumerId = externalMeasurementConsumerId,
        externalMeasurementId = externalMeasurementId,
        externalDuchyId = externalDuchyId,
        externalComputationId = externalComputationId,
        measurementDetails = details,
        struct = participantStruct,
      )
  }

  for (requisitionStruct in requisitionsStructs) {
    requisitions +=
      RequisitionReader.buildRequisition(
        struct,
        requisitionStruct,
        participantStructs,
        dataProvidersCount,
      )
  }
}

private fun MeasurementKt.Dsl.fillComputationStatsView(struct: Struct) {
  fillMeasurementCommon(struct)

  val externalMeasurementId = ExternalId(struct.getLong("ExternalMeasurementId"))
  val externalMeasurementConsumerId = ExternalId(struct.getLong("ExternalMeasurementConsumerId"))
  val externalComputationId = ExternalId(struct.getLong("ExternalComputationId"))

  // Map of external Duchy ID to ComputationParticipant struct.
  val participantStructs: Map<String, Struct> =
    struct.getStructList("ComputationParticipants").associateBy {
      val duchyId = it.getLong("DuchyId")
      checkNotNull(DuchyIds.getExternalId(duchyId)) { "Duchy with internal ID $duchyId not found" }
    }

  for ((externalDuchyId, participantStruct) in participantStructs) {
    computationParticipants +=
      ComputationParticipantReader.buildComputationParticipant(
        externalMeasurementConsumerId = externalMeasurementConsumerId,
        externalMeasurementId = externalMeasurementId,
        externalDuchyId = externalDuchyId,
        externalComputationId = externalComputationId,
        measurementDetails = details,
        struct = participantStruct,
      )

    logEntries +=
      buildLogEntries(
        externalMeasurementConsumerId = externalMeasurementConsumerId,
        externalMeasurementId = externalMeasurementId,
        externalDuchyId = externalDuchyId,
        struct = participantStruct,
      )
  }
}

private fun buildLogEntries(
  externalMeasurementConsumerId: ExternalId,
  externalMeasurementId: ExternalId,
  externalDuchyId: String,
  struct: Struct,
): Collection<DuchyMeasurementLogEntry> {
  val logEntryStructs = struct.getStructList("DuchyMeasurementLogEntries")
  return buildList {
    for (logEntryStruct in logEntryStructs) {
      val measurementLogEntryDetails =
        logEntryStruct.getProtoMessage(
          "MeasurementLogDetails",
          MeasurementLogEntryDetails.getDefaultInstance(),
        )
      val duchyMeasurementLogEntryDetails =
        logEntryStruct.getProtoMessage(
          "DuchyMeasurementLogDetails",
          DuchyMeasurementLogEntryDetails.getDefaultInstance(),
        )

      add(
        duchyMeasurementLogEntry {
          logEntry = measurementLogEntry {
            this.externalMeasurementConsumerId = externalMeasurementConsumerId.value
            this.externalMeasurementId = externalMeasurementId.value
            createTime = logEntryStruct.getTimestamp("CreateTime").toProto()
            details = measurementLogEntryDetails
          }
          this.externalDuchyId = externalDuchyId
          externalComputationLogEntryId = logEntryStruct.getLong("ExternalComputationLogEntryId")
          details = duchyMeasurementLogEntryDetails
        }
      )
    }
  }
}
