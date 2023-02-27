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
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteString
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundException

class MeasurementReader(private val view: Measurement.View) :
  SpannerReader<MeasurementReader.Result>() {

  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val measurement: Measurement
  )

  private fun constructBaseSql(view: Measurement.View): String {
    return when (view) {
      Measurement.View.DEFAULT -> defaultViewBaseSql
      Measurement.View.COMPUTATION -> computationViewBaseSql
      Measurement.View.UNRECOGNIZED ->
        throw IllegalArgumentException("View field of GetMeasurementRequest is not set")
    }
  }

  override val baseSql: String = constructBaseSql(view)

  override suspend fun translate(struct: Struct): Result =
    Result(
      struct.getInternalId("MeasurementConsumerId"),
      struct.getInternalId("MeasurementId"),
      buildMeasurement(struct)
    )

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: ExternalId,
    externalMeasurementId: ExternalId
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE ExternalMeasurementConsumerId = @externalMeasurementConsumerId
            AND ExternalMeasurementId = @externalMeasurementId
          """
        )
        bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
        bind("externalMeasurementId").to(externalMeasurementId.value)

        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
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
        Measurement.View.UNRECOGNIZED ->
          throw IllegalArgumentException("View field of GetMeasurementRequest is not set")
      }
    }
  }

  companion object {

    suspend fun readMeasurementState(
      readContext: AsyncDatabaseClient.ReadContext,
      measurementConsumerId: InternalId,
      measurementId: InternalId
    ): Measurement.State {
      val column = "State"
      return readContext
        .readRow(
          "Measurements",
          Key.of(measurementConsumerId.value, measurementId.value),
          listOf(column)
        )
        ?.getProtoEnum(column, Measurement.State::forNumber)
        ?: throw MeasurementNotFoundException() { "Measurement not found $measurementId" }
    }

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
      MeasurementConsumerCertificates.ExternalMeasurementConsumerCertificateId,
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
          EncryptedResult
        FROM
          DuchyMeasurementResults
          JOIN DuchyCertificates USING (DuchyId, CertificateId)
        WHERE
          DuchyMeasurementResults.MeasurementConsumerId = Measurements.MeasurementConsumerId
          AND DuchyMeasurementResults.MeasurementId = Measurements.MeasurementId
      ) AS DuchyResults
    FROM
      Measurements
      JOIN MeasurementConsumers USING (MeasurementConsumerId)
      JOIN MeasurementConsumerCertificates USING(MeasurementConsumerId, CertificateId)
    """
        .trimIndent()

    private val computationViewBaseSql =
      """
    SELECT
      ExternalMeasurementConsumerId,
      ExternalMeasurementConsumerCertificateId,
      Measurements.MeasurementId,
      Measurements.MeasurementConsumerId,
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
          EncryptedResult
        FROM
          DuchyMeasurementResults
          JOIN DuchyCertificates USING (CertificateId)
        WHERE
          Measurements.MeasurementConsumerId = DuchyMeasurementResults.MeasurementConsumerId
          AND Measurements.MeasurementId = DuchyMeasurementResults.MeasurementId
      ) AS DuchyResults
    FROM
      Measurements@{FORCE_INDEX=MeasurementsByContinuationToken}
      JOIN MeasurementConsumers USING (MeasurementConsumerId)
      JOIN MeasurementConsumerCertificates USING(MeasurementConsumerId, CertificateId)
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
  details = struct.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
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
      }
    }
  }
}

private fun MeasurementKt.Dsl.fillDefaultView(struct: Struct) {
  fillMeasurementCommon(struct)

  val measurementSucceeded = state == Measurement.State.SUCCEEDED
  for (requisitionStruct in struct.getStructList("Requisitions")) {
    val requisitionDetails =
      requisitionStruct.getProtoMessage("RequisitionDetails", Requisition.Details.parser())
    val externalDataProviderId = requisitionStruct.getLong("ExternalDataProviderId")
    val externalDataProviderCertificateId =
      requisitionStruct.getLong("ExternalDataProviderCertificateId")
    dataProviders[externalDataProviderId] = dataProviderValue {
      this.externalDataProviderCertificateId = externalDataProviderCertificateId
      dataProviderPublicKey = requisitionDetails.dataProviderPublicKey
      dataProviderPublicKeySignature = requisitionDetails.dataProviderPublicKeySignature
      encryptedRequisitionSpec = requisitionDetails.encryptedRequisitionSpec
    }

    if (measurementSucceeded && !requisitionDetails.encryptedData.isEmpty) {
      results += resultInfo {
        this.externalDataProviderId = externalDataProviderId
        externalCertificateId = externalDataProviderCertificateId
        encryptedResult = requisitionDetails.encryptedData
      }
    }
  }
}

private fun MeasurementKt.Dsl.fillComputationView(struct: Struct) {
  fillMeasurementCommon(struct)
  val requisitionsStructs = struct.getStructList("Requisitions")
  val dataProvidersCount = requisitionsStructs.size

  if (struct.isNull("ExternalComputationId")) {
    for (requisitionStruct in requisitionsStructs) {
      requisitions +=
        RequisitionReader.buildRequisition(struct, requisitionStruct, mapOf(), dataProvidersCount)
    }
    return
  }

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
        struct = participantStruct
      )
  }

  for (requisitionStruct in requisitionsStructs) {
    requisitions +=
      RequisitionReader.buildRequisition(
        struct,
        requisitionStruct,
        participantStructs,
        dataProvidersCount
      )
  }
}
