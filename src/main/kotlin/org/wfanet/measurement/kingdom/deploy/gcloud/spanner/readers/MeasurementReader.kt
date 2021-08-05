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
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition
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
    // TODO(@google.com uakyol): populate all the relevant fields for a measurement.
    val measurementBuilder =
      Measurement.newBuilder().apply {
        externalMeasurementId = struct.getLong("ExternalMeasurementId")
        externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
        externalComputationId = struct.getLong("ExternalComputationId")
        providedMeasurementId = struct.getString("ProvidedMeasurementId")
        details = struct.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
        createTime = struct.getTimestamp("CreateTime").toProto()
      }

    return when (view) {
      // TODO(@google.com uakyol): populate all the relevant fields for a measurement.
      Measurement.View.DEFAULT -> measurementBuilder.build()
      Measurement.View.COMPUTATION -> {
        // TODO(@uakyol): populate all the relevant fields for a requisition.
        val requisitions =
          struct
            .getStructList("Requisitions")
            .map {
              Requisition.newBuilder()
                .apply {
                  externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
                  externalMeasurementId = struct.getLong("ExternalMeasurementId")
                  externalRequisitionId = it.getLong("ExternalRequisitionId")
                }
                .build()
            }
            .toList()
        // TODO(@uakyol) : populate all the relevant fields for a computationParticipant.
        val computationParticipants =
          struct
            .getStructList("ComputationParticipants")
            .map {
              ComputationParticipant.newBuilder()
                .apply {
                  externalDuchyId = DuchyIds.getExternalId(it.getLong("DuchyId"))
                  externalMeasurementId = struct.getLong("ExternalMeasurementId")
                  externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
                  externalComputationId = struct.getLong("ExternalComputationId")
                  state = it.getProtoEnum("State", ComputationParticipant.State::forNumber)
                }
                .build()
            }
            .toList()

        measurementBuilder
          .also {
            it.addAllRequisitions(requisitions)
            it.addAllComputationParticipants(computationParticipants)
          }
          .build()
      }
      Measurement.View.UNRECOGNIZED ->
        throw IllegalArgumentException("View field of GetMeasurementRequest is not set")
    }
  }

  suspend fun readExternalIdWithGroupByOrNull(
    readContext: AsyncDatabaseClient.ReadContext,
    externalId: ExternalId
  ): Result? {
    return withBuilder {
        appendClause("WHERE $externalIdColumn = @external_id")
        appendClause("GROUP BY 1, 2, 3, 4, 5, 6, 7, 8")
        bind("external_id").to(externalId.value)

        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
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
      MeasurementConsumers.ExternalMeasurementConsumerId
    FROM Measurements
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
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
      MeasurementConsumers.ExternalMeasurementConsumerId,
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
    """.trimIndent()
  }
}
