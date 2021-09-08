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
import org.wfanet.measurement.common.identity.InternalId
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
    Requisitions.UpdateTime,
    Requisitions.ExternalRequisitionId,
    Requisitions.State AS RequisitionState,
    Requisitions.FulfillingDuchyId,
    Requisitions.RequisitionDetails,
    ExternalMeasurementId,
    ExternalMeasurementConsumerId,
    ExternalMeasurementConsumerCertificateId,
    ExternalComputationId,
    ExternalDataProviderId,
    ExternalDataProviderCertificateId,
    SubjectKeyIdentifier,
    NotValidBefore,
    NotValidAfter,
    RevocationState,
    CertificateDetails,
    Measurements.State AS MeasurementState,
    MeasurementDetails,
    ARRAY(
      SELECT AS STRUCT
        ComputationParticipants.DuchyId,
        ComputationParticipants.ParticipantDetails,
        ExternalDuchyCertificateId
      FROM
        ComputationParticipants
        LEFT JOIN DuchyCertificates USING (DuchyId, CertificateId)
      WHERE
        ComputationParticipants.MeasurementConsumerId = Requisitions.MeasurementConsumerId
        AND ComputationParticipants.MeasurementId = Requisitions.MeasurementId
    ) AS ComputationParticipants
  FROM
    Requisitions
    JOIN Measurements USING (MeasurementConsumerId, MeasurementId)
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
    JOIN DataProviders USING (DataProviderId)
    JOIN DataProviderCertificates
      ON (DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId)
    JOIN Certificates ON (Certificates.CertificateId = DataProviderCertificates.CertificateId)
  """.trimIndent()

private object Params {
  const val EXTERNAL_MEASUREMENT_CONSUMER_ID = "externalMeasurementConsumerId"
  const val EXTERNAL_MEASUREMENT_ID = "externalMeasurementId"
  const val EXTERNAL_COMPUTATION_ID = "externalComputationId"
  const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
  const val EXTERNAL_REQUISITION_ID = "externalRequisitionId"
}

class RequisitionReader : BaseSpannerReader<RequisitionReader.Result>() {
  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val requisitionId: InternalId,
    val requisition: Requisition
  )

  override val builder: Statement.Builder = Statement.newBuilder(BASE_SQL)

  override suspend fun translate(struct: Struct): Result {
    return Result(
      InternalId(struct.getLong("MeasurementConsumerId")),
      InternalId(struct.getLong("MeasurementId")),
      InternalId(struct.getLong("RequisitionId")),
      buildRequisition(struct)
    )
  }

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
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalRequisitionId = @${Params.EXTERNAL_REQUISITION_ID}
            AND ExternalMeasurementId = @${Params.EXTERNAL_MEASUREMENT_ID}
            AND ExternalMeasurementConsumerId = @${Params.EXTERNAL_MEASUREMENT_CONSUMER_ID}
          """.trimIndent()
        )
        bind(Params.EXTERNAL_MEASUREMENT_CONSUMER_ID to externalMeasurementConsumerId)
        bind(Params.EXTERNAL_MEASUREMENT_ID to externalMeasurementId)
        bind(Params.EXTERNAL_REQUISITION_ID to externalRequisitionId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByExternalDataProviderId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: Long,
    externalRequisitionId: Long,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalRequisitionId = @${Params.EXTERNAL_REQUISITION_ID}
            AND ExternalDataProviderId = @${Params.EXTERNAL_DATA_PROVIDER_ID}
          """.trimIndent()
        )
        bind(Params.EXTERNAL_DATA_PROVIDER_ID to externalDataProviderId)
        bind(Params.EXTERNAL_REQUISITION_ID to externalRequisitionId)
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByExternalComputationId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalComputationId: Long,
    externalRequisitionId: Long
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalComputationId = @${Params.EXTERNAL_COMPUTATION_ID}
            AND ExternalRequisitionId = @${Params.EXTERNAL_REQUISITION_ID}
          """.trimIndent()
        )
        bind(Params.EXTERNAL_COMPUTATION_ID to externalComputationId)
        bind(Params.EXTERNAL_REQUISITION_ID to externalRequisitionId)
      }
      .execute(readContext)
      .singleOrNull()
  }

  companion object {
    /** Builds a [Requisition] from [struct]. */
    private fun buildRequisition(struct: Struct): Requisition {
      // Map of external Duchy ID to ComputationParticipant struct.
      val participantStructs =
        struct.getStructList("ComputationParticipants").associateBy {
          val duchyId = it.getLong("DuchyId")
          checkNotNull(DuchyIds.getExternalId(duchyId)) {
            "Duchy with internal ID $duchyId not found"
          }
        }
      return buildRequisition(struct, struct, participantStructs)
    }

    fun buildRequisition(
      measurementStruct: Struct,
      requisitionStruct: Struct,
      participantStructs: Map<String, Struct>
    ) = requisition {
      externalMeasurementConsumerId = measurementStruct.getLong("ExternalMeasurementConsumerId")
      externalMeasurementId = measurementStruct.getLong("ExternalMeasurementId")
      externalRequisitionId = requisitionStruct.getLong("ExternalRequisitionId")
      externalComputationId = measurementStruct.getLong("ExternalComputationId")
      externalDataProviderId = requisitionStruct.getLong("ExternalDataProviderId")
      updateTime = requisitionStruct.getTimestamp("UpdateTime").toProto()
      state = requisitionStruct.getProtoEnum("RequisitionState", Requisition.State::forNumber)
      if (state == Requisition.State.FULFILLED) {
        val fulfillingDuchyId = requisitionStruct.getLong("FulfillingDuchyId")
        externalFulfillingDuchyId =
          checkNotNull(DuchyIds.getExternalId(fulfillingDuchyId)) {
            "External ID not found for fulfilling Duchy $fulfillingDuchyId"
          }
      }
      details =
        requisitionStruct.getProtoMessage("RequisitionDetails", Requisition.Details.parser())
      for ((externalDuchyId, participantStruct) in participantStructs) {
        duchies[externalDuchyId] = buildDuchyValue(participantStruct)
      }
      dataProviderCertificate = CertificateReader.buildDataProviderCertificate(requisitionStruct)
      parentMeasurement = buildParentMeasurement(measurementStruct)
    }

    /**
     * Builds a [Requisition.DuchyValue] from a [Struct].
     *
     * @param struct a [Struct] representing a single ComputationParticipant
     */
    private fun buildDuchyValue(struct: Struct): Requisition.DuchyValue = duchyValue {
      if (!struct.isNull("ExternalDuchyCertificateId")) {
        externalDuchyCertificateId = struct.getLong("ExternalDuchyCertificateId")
      }

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

    private fun buildParentMeasurement(struct: Struct) = parentMeasurement {
      val measurementDetails =
        struct.getProtoMessage("MeasurementDetails", Measurement.Details.parser())
      apiVersion = measurementDetails.apiVersion
      externalMeasurementConsumerCertificateId =
        struct.getLong("ExternalMeasurementConsumerCertificateId")
      measurementSpec = measurementDetails.measurementSpec
      measurementSpecSignature = measurementDetails.measurementSpecSignature
      protocolConfig = measurementDetails.protocolConfig
      state = struct.getProtoEnum("MeasurementState", Measurement.State::forNumber)
    }
  }
}
