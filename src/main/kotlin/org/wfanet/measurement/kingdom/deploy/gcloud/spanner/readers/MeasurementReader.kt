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
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds

class MeasurementReader(private val view: Measurement.View) :
  SpannerReader<MeasurementReader.Result>() {

  data class Result(
    val measurement: Measurement,
    val measurementId: Long,
    val measurementConsumerId: Long
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

  override val externalIdColumn: String = "Measurements.ExternalComputationId"

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildMeasurement(struct),
      struct.getLong("MeasurementId"),
      struct.getLong("MeasurementConsumerId")
    )

  private fun buildMeasurement(struct: Struct): Measurement {
    return measurement {
      externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
      externalMeasurementId = struct.getLong("ExternalMeasurementId")
      externalComputationId = struct.getLong("ExternalComputationId")
      providedMeasurementId = struct.getString("ProvidedMeasurementId")
      externalMeasurementConsumerCertificateId =
        struct.getLong("ExternalMeasurementConsumerCertificateId")
      createTime = struct.getTimestamp("CreateTime").toProto()
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      state = struct.getProtoEnum("State", Measurement.State::forNumber)
      details = struct.getProtoMessage("MeasurementDetails", Measurement.Details.parser())

      // TODO(@wangyaopw): Map external protocol config ID from ProtocolConfigs after it is
      // implemented.

      when (view) {
        Measurement.View.DEFAULT -> {}
        Measurement.View.COMPUTATION -> {
          // TODO(@SanjayVas): populate all the relevant fields for a requisition.
          for (requisitionStruct in struct.getStructList("Requisitions")) {
            requisitions +=
              requisition {
                externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
                externalMeasurementId = struct.getLong("ExternalMeasurementId")
                externalRequisitionId = requisitionStruct.getLong("ExternalRequisitionId")
              }
          }
          // TODO(@uakyol) : populate all the relevant fields for a computationParticipant.
          for (participantStruct in struct.getStructList("ComputationParticipants")) {
            val duchyId = participantStruct.getLong("DuchyId")
            val externalDuchyId =
              checkNotNull(DuchyIds.getExternalId(duchyId)) {
                "Duchy with internal ID $duchyId not found"
              }
            computationParticipants +=
              computationParticipant {
                this.externalDuchyId = externalDuchyId
                externalMeasurementId = struct.getLong("ExternalMeasurementId")
                externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
                externalComputationId = struct.getLong("ExternalComputationId")
                state =
                  participantStruct.getProtoEnum("State", ComputationParticipant.State::forNumber)
              }
          }
        }
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
      Measurements.State,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      MeasurementConsumerCertificates.ExternalMeasurementConsumerCertificateId
    FROM Measurements
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN MeasurementConsumerCertificates USING(MeasurementConsumerId, CertificateId)
    """.trimIndent()

    private val computationViewBaseSql =
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
      Measurements.State,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      MeasurementConsumerCertificates.ExternalMeasurementConsumerCertificateId,
      ARRAY(
         SELECT AS STRUCT
           r.ExternalRequisitionId
         FROM Requisitions AS r
         WHERE Measurements.MeasurementId = r.MeasurementId
         AND Measurements.MeasurementConsumerId = r.MeasurementConsumerId
       ) AS Requisitions,
      ARRAY(
         SELECT AS STRUCT
           c.DuchyId,
           c.State
         FROM ComputationParticipants AS c
         WHERE Measurements.MeasurementId = c.MeasurementId
         AND Measurements.MeasurementConsumerId = c.MeasurementConsumerId
       ) AS ComputationParticipants
    FROM Measurements
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN MeasurementConsumerCertificates USING(MeasurementConsumerId, CertificateId)
    """.trimIndent()
  }
}
