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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt.duchyValue
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds

private val BASE_SQL =
  """
  SELECT
    Requisitions.MeasurementConsumerId,
    Requisitions.MeasurementId,
    Requisitions.RequisitionId,
    Requisitions.DataProviderId,
    Requisitions.UpdateTime,
    Requisitions.ExternalRequisitionId,
    Requisitions.DataProviderCertificateId,
    Requisitions.State AS RequisitionState,
    Requisitions.FulfillingDuchyId,
    Requisitions.RequisitionDetails,
    ExternalMeasurementId,
    ExternalMeasurementConsumerId,
    ExternalMeasurementConsumerCertificateId,
    ExternalComputationId,
    ExternalDataProviderId,
    ExternalDataProviderCertificateId,
    Measurements.State AS MeasurementState,
    MeasurementDetails,
    ARRAY(
      SELECT AS STRUCT
        ComputationParticipants.*,
        ExternalDuchyCertificateId
      FROM
        ComputationParticipants
        JOIN DuchyCertificates USING (DuchyId, CertificateId)
      WHERE
        ComputationParticipants.MeasurementConsumerId = Requisitions.MeasurementConsumerId
        AND ComputationParticipants.MeasurementId = Requisitions.MeasurementId
    ) AS Duchies
  FROM
    Requisitions
    JOIN Measurements USING (MeasurementConsumerId, MeasurementId)
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
    JOIN DataProviders USING (DataProviderId)
    JOIN DataProviderCertificates
      ON (DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId)
  """.trimIndent()

class RequisitionReader : BaseSpannerReader<Requisition>() {
  override val builder = Statement.newBuilder(BASE_SQL)

  override suspend fun translate(struct: Struct): Requisition = buildRequisition(struct)

  /** Fills [builder], returning this [RequisitionReader] for chaining. */
  fun fillStatementBuilder(fill: Statement.Builder.() -> Unit): RequisitionReader {
    builder.fill()
    return this
  }

  suspend fun readByExternalId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: Long,
    externalMeasurementId: Long,
    externalRequisitionId: Long,
  ): Requisition? {
    val externalRequisitionIdParam = "externalRequisitionId"
    val externalMeasurementIdParam = "externalMeasurementId"
    val externalMeasurementConsumerIdParam = "externalMeasurementConsumerId"

    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalRequisitionId = @$externalRequisitionIdParam
            AND ExternalMeasurementId = @$externalMeasurementIdParam
            AND ExternalMeasurementConsumerId = @$externalMeasurementConsumerIdParam
          """.trimIndent()
        )
        bind(externalMeasurementConsumerIdParam to externalMeasurementConsumerId)
        bind(externalMeasurementIdParam to externalMeasurementId)
        bind(externalRequisitionIdParam to externalRequisitionId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByExternalDataProviderId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: Long,
    externalRequisitionId: Long,
  ): Requisition? {
    val externalRequisitionIdParam = "externalRequisitionId"
    val externalDataProviderIdParam = "externalDataProviderId"

    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalRequisitionId = @$externalRequisitionIdParam
            AND ExternalDataProviderId = @$externalDataProviderIdParam
          """.trimIndent()
        )
        bind(externalDataProviderIdParam to externalDataProviderId)
        bind(externalRequisitionIdParam to externalRequisitionId)
      }
      .execute(readContext)
      .singleOrNull()
  }

  companion object {
    /** Builds a [Requisition] from [struct]. */
    fun buildRequisition(struct: Struct): Requisition = requisition {
      externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
      externalMeasurementId = struct.getLong("ExternalMeasurementId")
      externalRequisitionId = struct.getLong("ExternalRequisitionId")
      externalComputationId = struct.getLong("ExternalComputationId")
      externalDataProviderId = struct.getLong("ExternalDataProviderId")
      externalDataProviderCertificateId = struct.getLong("ExternalDataProviderCertificateId")
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      state = struct.getProtoEnum("RequisitionState", Requisition.State::forNumber)
      if (state == Requisition.State.FULFILLED) {
        val fulfillingDuchyId = struct.getLong("FulfillingDuchyId")
        externalFulfillingDuchyId =
          checkNotNull(DuchyIds.getExternalId(fulfillingDuchyId)) {
            "External ID not found for fulfilling Duchy $fulfillingDuchyId"
          }
      }
      details = struct.getProtoMessage("RequisitionDetails", Requisition.Details.parser())
      for (duchyStruct in struct.getStructList("Duchies")) {
        val duchyId = duchyStruct.getLong("DuchyId")
        val externalDuchyId =
          checkNotNull(DuchyIds.getExternalId(duchyId)) {
            "External ID not found for Duchy $duchyId"
          }
        duchies.put(externalDuchyId, buildDuchyValue(duchyStruct))
      }
      parentMeasurement = buildParentMeasurement(struct)
    }
  }
}

private fun buildParentMeasurement(struct: Struct) = parentMeasurement {
  val measurementDetails =
    struct.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
  apiVersion = measurementDetails.apiVersion
  externalMeasurementConsumerCertificateId =
    struct.getLong("ExternalMeasurementConsumerCertificateId")
  measurementSpec = measurementDetails.measurementSpec
  measurementSpecSignature = measurementDetails.measurementSpecSignature
  state = struct.getProtoEnum("MeasurementState", Measurement.State::forNumber)
  // TODO(@uakyol): Fill external protocol config ID once we have a config mapping.
}

private fun buildDuchyValue(struct: Struct): Requisition.DuchyValue = duchyValue {
  externalDuchyCertificateId = struct.getLong("ExternalDuchyCertificateId")

  val participantDetails =
    struct.getProtoMessage("ParticipantDetails", ComputationParticipant.Details.parser())
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  when (participantDetails.protocolCase) {
    ComputationParticipant.Details.ProtocolCase.LIQUID_LEGIONS_V2 -> {
      liquidLegionsV2 = participantDetails.liquidLegionsV2
    }
    // Protocol may only be set after computation participant sets requisition params.
    ComputationParticipant.Details.ProtocolCase.PROTOCOL_NOT_SET -> Unit
  }
}
