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
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds

class MeasurementReader(private val view: Measurement.View) :
  SpannerReader<MeasurementReader.Result>() {

  data class Result(val measurement: Measurement, val measurementId: Long)

  private fun constructBaseSql(view: Measurement.View): String {
    return when (view) {
      Measurement.View.DEFAULT -> defaultViewBaseSql
      Measurement.View.COMPUTATION -> computationViewBaseSql
      Measurement.View.UNRECOGNIZED ->
        throw IllegalArgumentException("View field of GetMeasurementRequest is not set")
    }
  }
  override val baseSql: String = constructBaseSql(view)

  override val externalIdColumn: String = "Measurements.ExternalComputationId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildMeasurement(struct), struct.getLong("MeasurementId"))

  private fun buildMeasurement(struct: Struct): Measurement {
    return measurement {
      when (view) {
        Measurement.View.DEFAULT -> fillDefaultView(struct)
        Measurement.View.COMPUTATION -> fillComputationView(struct)
        Measurement.View.UNRECOGNIZED ->
          throw IllegalArgumentException("View field of GetMeasurementRequest is not set")
      }
    }
  }

  companion object {
    private val defaultViewBaseSql =
      """
    SELECT
      Measurements.MeasurementId,
      Measurements.MeasurementConsumerId,
      Measurements.ExternalMeasurementId,
      Measurements.ExternalComputationId,
      Measurements.ProvidedMeasurementId,
      Measurements.MeasurementDetails,
      Measurements.CreateTime,
      Measurements.UpdateTime,
      Measurements.State AS MeasurementState,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      MeasurementConsumerCertificates.ExternalMeasurementConsumerCertificateId
    FROM Measurements
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN MeasurementConsumerCertificates USING(MeasurementConsumerId, CertificateId)
    """.trimIndent()

    private val computationViewBaseSql =
      """
    SELECT
      ExternalMeasurementConsumerId,
      ExternalMeasurementConsumerCertificateId,
      Measurements.MeasurementId,
      Measurements.ExternalMeasurementId,
      Measurements.ExternalComputationId,
      Measurements.ProvidedMeasurementId,
      Measurements.MeasurementDetails,
      Measurements.CreateTime,
      Measurements.UpdateTime,
      Measurements.State AS MeasurementState,
      ARRAY(
        SELECT AS STRUCT
          ExternalDataProviderId,
          ExternalDataProviderCertificateId,
          Requisitions.UpdateTime,
          Requisitions.ExternalRequisitionId,
          Requisitions.State AS RequisitionState,
          Requisitions.FulfillingDuchyId,
          Requisitions.RequisitionDetails
        FROM
          Requisitions
          JOIN DataProviders USING (DataProviderId)
          JOIN DataProviderCertificates
            ON (DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId)
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
          ARRAY(
            SELECT AS STRUCT
              DuchyMeasurementLogEntries.CreateTime,
              DuchyMeasurementLogEntries.ExternalComputationLogEntryId,
              DuchyMeasurementLogEntries.DuchyMeasurementLogDetails,
              MeasurementLogEntries.MeasurementLogDetails
            FROM
              DuchyMeasurementLogEntries
              JOIN MeasurementLogEntries USING (MeasurementConsumerId, MeasurementId, CreateTime)
            WHERE DuchyMeasurementLogEntries.DuchyId = ComputationParticipants.DuchyId
            ORDER BY MeasurementLogEntries.CreateTime DESC
          ) AS DuchyMeasurementLogEntries
        FROM
          ComputationParticipants
          LEFT JOIN DuchyCertificates USING (DuchyId, CertificateId)
        WHERE
          ComputationParticipants.MeasurementConsumerId = Measurements.MeasurementConsumerId
          AND ComputationParticipants.MeasurementId = Measurements.MeasurementId
      ) AS ComputationParticipants
    FROM
      Measurements
      JOIN MeasurementConsumers USING (MeasurementConsumerId)
      JOIN MeasurementConsumerCertificates USING(MeasurementConsumerId, CertificateId)
    """.trimIndent()
  }
}

private fun MeasurementKt.Dsl.fillMeasurementCommon(struct: Struct) {
  externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
  externalMeasurementId = struct.getLong("ExternalMeasurementId")
  externalComputationId = struct.getLong("ExternalComputationId")
  providedMeasurementId = struct.getString("ProvidedMeasurementId")
  externalMeasurementConsumerCertificateId =
    struct.getLong("ExternalMeasurementConsumerCertificateId")
  createTime = struct.getTimestamp("CreateTime").toProto()
  updateTime = struct.getTimestamp("UpdateTime").toProto()
  state = struct.getProtoEnum("MeasurementState", Measurement.State::forNumber)
  details = struct.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
  // TODO(@wangyaopw): Map external protocol config ID from ProtocolConfigs after it is
  // implemented.
}

private fun MeasurementKt.Dsl.fillDefaultView(struct: Struct) {
  fillMeasurementCommon(struct)

  // TODO(@SanjayVas): Fill data providers.
}

private fun MeasurementKt.Dsl.fillComputationView(struct: Struct) {
  fillMeasurementCommon(struct)

  val externalMeasurementId = struct.getLong("ExternalMeasurementId")
  val externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
  val externalComputationId = struct.getLong("ExternalComputationId")
  val apiVersion = details.apiVersion

  // Map of external Duchy ID to ComputationParticipant struct.
  val participantStructs: Map<String, Struct> =
    struct.getStructList("ComputationParticipants").associateBy {
      val duchyId = it.getLong("DuchyId")
      checkNotNull(DuchyIds.getExternalId(duchyId)) { "Duchy with internal ID $duchyId not found" }
    }

  for ((externalDuchyId, participantStruct) in participantStructs) {
    // TODO(@SanjayVas): Share this logic with ComputationParticipantReader once it exists.
    computationParticipants +=
      computationParticipant {
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        this.externalMeasurementId = externalMeasurementId
        this.externalDuchyId = externalDuchyId
        this.externalComputationId = externalComputationId
        if (!participantStruct.isNull("ExternalDuchyCertificateId")) {
          externalDuchyCertificateId = participantStruct.getLong("ExternalDuchyCertificateId")
          // TODO(@SanjayVas): Include denormalized Certificate.
        }
        updateTime = participantStruct.getTimestamp("UpdateTime").toProto()
        state = participantStruct.getProtoEnum("State", ComputationParticipant.State::forNumber)
        details =
          participantStruct.getProtoMessage(
            "ParticipantDetails",
            ComputationParticipant.Details.parser()
          )
        this.apiVersion = apiVersion

        buildFailureLogEntry(
          externalMeasurementConsumerId,
          externalMeasurementId,
          externalDuchyId,
          participantStruct.getStructList("DuchyMeasurementLogEntries")
        )
          ?.let { failureLogEntry = it }
      }
  }

  for (requisitionStruct in struct.getStructList("Requisitions")) {
    requisitions +=
      RequisitionReader.buildRequisition(struct, requisitionStruct, participantStructs)
  }
}

private fun buildFailureLogEntry(
  externalMeasurementConsumerId: Long,
  externalMeasurementId: Long,
  externalDuchyId: String,
  logEntryStructs: Iterable<Struct>
): DuchyMeasurementLogEntry? {
  return logEntryStructs
    .asSequence()
    .map { it to it.getProtoMessage("MeasurementLogDetails", MeasurementLogEntry.Details.parser()) }
    .find { (_, logEntryDetails) -> logEntryDetails.hasError() }
    ?.let { (struct, logEntryDetails) ->
      duchyMeasurementLogEntry {
        logEntry =
          measurementLogEntry {
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            this.externalMeasurementId = externalMeasurementId
            createTime = struct.getTimestamp("CreateTime").toProto()
            details = logEntryDetails
          }
        this.externalDuchyId = externalDuchyId
        externalComputationLogEntryId = struct.getLong("ExternalComputationLogEntryId")
        details =
          struct.getProtoMessage(
            "DuchyMeasurementLogDetails",
            DuchyMeasurementLogEntry.Details.parser()
          )
      }
    }
}
