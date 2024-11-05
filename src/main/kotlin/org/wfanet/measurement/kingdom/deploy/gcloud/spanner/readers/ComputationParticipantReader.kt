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
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantDetails
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementDetails
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundByMeasurementException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags

private val BASE_SQL =
  """
  SELECT
    ComputationParticipants.MeasurementConsumerId,
    ComputationParticipants.MeasurementId,
    ComputationParticipants.DuchyId,
    ComputationParticipants.CertificateId,
    ComputationParticipants.State,
    ComputationParticipants.ParticipantDetails,
    ComputationParticipants.UpdateTime,
    Measurements.ExternalMeasurementId,
    Measurements.ExternalComputationId,
    Measurements.MeasurementDetails,
    Measurements.State as MeasurementState,
    MeasurementConsumers.ExternalMeasurementConsumerId,
    DuchyCertificates.ExternalDuchyCertificateId,
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
        MeasurementLogEntries
        JOIN DuchyMeasurementLogEntries USING (MeasurementConsumerId, MeasurementId, CreateTime)
      WHERE
        DuchyMeasurementLogEntries.DuchyId = ComputationParticipants.DuchyId
        AND DuchyMeasurementLogEntries.MeasurementConsumerId = ComputationParticipants.MeasurementConsumerId
        AND DuchyMeasurementLogEntries.MeasurementId = ComputationParticipants.MeasurementId
      ORDER BY MeasurementLogEntries.CreateTime DESC
    ) AS DuchyMeasurementLogEntries
  FROM
    Measurements
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN ComputationParticipants USING (MeasurementConsumerId, MeasurementId)
    LEFT JOIN (
      DuchyCertificates
      JOIN Certificates ON DuchyCertificates.CertificateId = Certificates.CertificateId
    ) ON
      ComputationParticipants.DuchyId = DuchyCertificates.DuchyId
      AND ComputationParticipants.CertificateId = DuchyCertificates.CertificateId
  """
    .trimIndent()

class ComputationParticipantReader : BaseSpannerReader<ComputationParticipantReader.Result>() {
  data class Result(
    val computationParticipant: ComputationParticipant,
    val measurementId: InternalId,
    val measurementConsumerId: InternalId,
    val measurementState: Measurement.State,
    val measurementDetails: MeasurementDetails,
  )

  override val builder: Statement.Builder = Statement.newBuilder(BASE_SQL)

  /** Fills [builder], returning this [ComputationParticipantReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): ComputationParticipantReader {
    builder.fill()
    return this
  }

  suspend fun readByExternalComputationId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalComputationId: ExternalId,
    duchyId: InternalId,
  ): Result? {
    fillStatementBuilder {
      appendClause(
        """
        WHERE
          ExternalComputationId = @externalComputationId
          AND ComputationParticipants.DuchyId = @duchyId
        """
          .trimIndent()
      )
      bind("externalComputationId" to externalComputationId)
      bind("duchyId" to duchyId)
      appendClause("LIMIT 1")
    }
    return execute(readContext).singleOrNull()
  }

  suspend fun readByExternalComputationId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalComputationId: ExternalId,
    externalDuchyId: String,
  ): Result? {
    val duchyId =
      DuchyIds.getInternalId(externalDuchyId) ?: throw DuchyNotFoundException(externalDuchyId)
    return readByExternalComputationId(readContext, externalComputationId, InternalId(duchyId))
  }

  override suspend fun translate(struct: Struct) =
    Result(
      buildComputationParticipant(struct),
      struct.getInternalId("MeasurementId"),
      struct.getInternalId("MeasurementConsumerId"),
      struct.getProtoEnum("MeasurementState", Measurement.State::forNumber),
      struct.getProtoMessage("MeasurementDetails", MeasurementDetails.getDefaultInstance()),
    )

  private fun buildComputationParticipant(struct: Struct): ComputationParticipant {
    val externalMeasurementConsumerId = ExternalId(struct.getLong("ExternalMeasurementConsumerId"))
    val externalMeasurementId = ExternalId(struct.getLong("ExternalMeasurementId"))
    val externalComputationId = ExternalId(struct.getLong("ExternalComputationId"))
    val measurementDetails =
      struct.getProtoMessage("MeasurementDetails", MeasurementDetails.getDefaultInstance())

    val duchyId = struct.getLong("DuchyId")
    val externalDuchyId =
      checkNotNull(DuchyIds.getExternalId(duchyId)) { "Duchy with internal ID $duchyId not found" }

    return buildComputationParticipant(
      externalMeasurementConsumerId = externalMeasurementConsumerId,
      externalMeasurementId = externalMeasurementId,
      externalDuchyId = externalDuchyId,
      externalComputationId = externalComputationId,
      measurementDetails = measurementDetails,
      struct = struct,
    )
  }

  companion object {
    fun buildComputationParticipant(
      externalMeasurementConsumerId: ExternalId,
      externalMeasurementId: ExternalId,
      externalDuchyId: String,
      externalComputationId: ExternalId,
      measurementDetails: MeasurementDetails,
      struct: Struct,
    ): ComputationParticipant {
      val failureLogEntry: DuchyMeasurementLogEntry? =
        buildFailureLogEntry(
          externalMeasurementConsumerId,
          externalMeasurementId,
          externalDuchyId,
          struct.getStructList("DuchyMeasurementLogEntries"),
        )
      val updateTime = struct.getTimestamp("UpdateTime")
      val etag = ETags.computeETag(updateTime)
      return computationParticipant {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId.value
        this.externalMeasurementId = externalMeasurementId.value
        this.externalDuchyId = externalDuchyId
        this.externalComputationId = externalComputationId.value
        if (!struct.isNull("ExternalDuchyCertificateId")) {
          duchyCertificate = CertificateReader.buildDuchyCertificate(externalDuchyId, struct)
        }
        this.updateTime = updateTime.toProto()
        this.etag = etag
        state = struct.getProtoEnum("State", ComputationParticipant.State::forNumber)
        details =
          struct.getProtoMessage(
            "ParticipantDetails",
            ComputationParticipantDetails.getDefaultInstance(),
          )
        apiVersion = measurementDetails.apiVersion

        if (failureLogEntry != null) {
          this.failureLogEntry = failureLogEntry
        }
      }
    }

    private fun buildFailureLogEntry(
      externalMeasurementConsumerId: ExternalId,
      externalMeasurementId: ExternalId,
      externalDuchyId: String,
      logEntryStructs: Iterable<Struct>,
    ): DuchyMeasurementLogEntry? {
      return logEntryStructs
        .asSequence()
        .map {
          it to
            it.getProtoMessage(
              "MeasurementLogDetails",
              MeasurementLogEntryDetails.getDefaultInstance(),
            )
        }
        .find { (_, logEntryDetails) -> logEntryDetails.hasError() }
        ?.let { (struct, logEntryDetails) ->
          duchyMeasurementLogEntry {
            logEntry = measurementLogEntry {
              this.externalMeasurementConsumerId = externalMeasurementConsumerId.value
              this.externalMeasurementId = externalMeasurementId.value
              createTime = struct.getTimestamp("CreateTime").toProto()
              details = logEntryDetails
            }
            this.externalDuchyId = externalDuchyId
            externalComputationLogEntryId = struct.getLong("ExternalComputationLogEntryId")
            details =
              struct.getProtoMessage(
                "DuchyMeasurementLogDetails",
                DuchyMeasurementLogEntryDetails.getDefaultInstance(),
              )
          }
        }
    }
  }
}

suspend fun readComputationParticipantState(
  readContext: AsyncDatabaseClient.ReadContext,
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  duchyId: InternalId,
): ComputationParticipant.State {
  val column = "State"
  return readContext
    .readRow(
      "ComputationParticipants",
      Key.of(measurementConsumerId.value, measurementId.value, duchyId.value),
      listOf(column),
    )
    ?.getProtoEnum(column, ComputationParticipant.State::forNumber)
    ?: throw ComputationParticipantNotFoundByMeasurementException(
      measurementConsumerId,
      measurementConsumerId,
      duchyId,
    ) {
      "ComputationParticipant not found $duchyId"
    }
}

suspend fun computationParticipantsInState(
  readContext: AsyncDatabaseClient.ReadContext,
  duchyIds: List<InternalId>,
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  state: ComputationParticipant.State,
): Boolean {
  val wrongState =
    duchyIds
      .asFlow()
      .map {
        readComputationParticipantState(readContext, measurementConsumerId, measurementId, it)
      }
      .firstOrNull { it != state }

  return wrongState == null
}
