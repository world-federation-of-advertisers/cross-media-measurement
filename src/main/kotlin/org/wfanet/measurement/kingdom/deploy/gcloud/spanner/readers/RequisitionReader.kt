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
import org.wfanet.measurement.internal.kingdom.ComputationParticipantDetails
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementDetails
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionKt.duchyValue
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds

class RequisitionReader : SpannerReader<RequisitionReader.Result>() {
  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val requisitionId: InternalId,
    val requisition: Requisition,
    val measurementDetails: MeasurementDetails,
  )

  override val baseSql: String
    get() = BASE_SQL

  private var filled = false

  /** Optional ORDER BY clause that is appended at the end of the overall query. */
  var orderByClause: String? = null
    set(value) {
      check(!filled) { "Statement builder already filled" }
      field = value
    }

  override suspend fun translate(struct: Struct): Result {
    return Result(
      InternalId(struct.getLong("MeasurementConsumerId")),
      InternalId(struct.getLong("MeasurementId")),
      InternalId(struct.getLong("RequisitionId")),
      buildRequisition(struct),
      struct.getProtoMessage("MeasurementDetails", MeasurementDetails.getDefaultInstance()),
    )
  }

  /**
   * Fills the statement builder for the query.
   *
   * @param block a function for filling the statement builder for the Requisitions table subquery.
   */
  override fun fillStatementBuilder(block: Statement.Builder.() -> Unit): SpannerReader<Result> {
    check(!filled) { "Statement builder already filled" }
    filled = true

    return super.fillStatementBuilder {
      block()
      append(")\n")

      appendClause(DEFAULT_SQL)

      val orderByClause = orderByClause
      if (orderByClause != null) {
        appendClause(orderByClause)
      }
    }
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
            ExternalRequisitionId = @${EXTERNAL_REQUISITION_ID}
            AND ExternalDataProviderId = @${EXTERNAL_DATA_PROVIDER_ID}
          """
            .trimIndent()
        )
        bind(EXTERNAL_DATA_PROVIDER_ID to externalDataProviderId)
        bind(EXTERNAL_REQUISITION_ID to externalRequisitionId)
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByExternalComputationId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalComputationId: Long,
    externalRequisitionId: Long,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE
            ExternalComputationId = @${EXTERNAL_COMPUTATION_ID}
            AND ExternalRequisitionId = @${EXTERNAL_REQUISITION_ID}
          """
            .trimIndent()
        )
        bind(EXTERNAL_COMPUTATION_ID to externalComputationId)
        bind(EXTERNAL_REQUISITION_ID to externalRequisitionId)
      }
      .execute(readContext)
      .singleOrNull()
  }

  companion object {
    private const val EXTERNAL_COMPUTATION_ID = "externalComputationId"
    private const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    private const val EXTERNAL_REQUISITION_ID = "externalRequisitionId"

    private val BASE_SQL =
      """
      @{spanner_emulator.disable_query_null_filtered_index_check=true}
      WITH FilteredRequisitions AS (
        SELECT
          Requisitions.MeasurementConsumerId,
          Requisitions.MeasurementId,
          Requisitions.RequisitionId,
          Requisitions.UpdateTime,
          Requisitions.ExternalRequisitionId,
          Requisitions.State,
          Requisitions.FulfillingDuchyId,
          Requisitions.RequisitionDetails,
          Requisitions.DataProviderCertificateId,
          ExternalMeasurementConsumerId,
          ExternalMeasurementId,
          ExternalDataProviderId,
          Measurements.State AS MeasurementState,
          ExternalComputationId,
          CertificateId,
          MeasurementDetails
        FROM
          MeasurementConsumers JOIN Measurements USING (MeasurementConsumerId)
          JOIN Requisitions USING (MeasurementConsumerId, MeasurementId)
          JOIN DataProviders USING (DataProviderId)
      """
        .trimIndent()

    private val DEFAULT_SQL =
      """
  SELECT
    MeasurementConsumerId,
    MeasurementId,
    RequisitionId,
    FilteredRequisitions.UpdateTime,
    ExternalRequisitionId,
    FilteredRequisitions.State AS RequisitionState,
    FulfillingDuchyId,
    RequisitionDetails,
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
    MeasurementState,
    MeasurementDetails,
    (
      SELECT
        count(ExternalDataProviderId),
      FROM
        Requisitions
      WHERE
        Requisitions.MeasurementConsumerId = FilteredRequisitions.MeasurementConsumerId
        AND Requisitions.MeasurementId = FilteredRequisitions.MeasurementId
    ) AS MeasurementRequisitionCount,
    ARRAY(
      SELECT AS STRUCT
        ComputationParticipants.DuchyId,
        ComputationParticipants.ParticipantDetails,
        ExternalDuchyCertificateId
      FROM
        ComputationParticipants
        LEFT JOIN DuchyCertificates USING (DuchyId, CertificateId)
      WHERE
        ComputationParticipants.MeasurementConsumerId = FilteredRequisitions.MeasurementConsumerId
        AND ComputationParticipants.MeasurementId = FilteredRequisitions.MeasurementId
    ) AS ComputationParticipants
  FROM
    FilteredRequisitions
    JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
    JOIN DataProviderCertificates
      ON (DataProviderCertificates.CertificateId = FilteredRequisitions.DataProviderCertificateId)
    JOIN Certificates ON (Certificates.CertificateId = DataProviderCertificates.CertificateId)
  """
        .trimIndent()

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
      val dataProvidersCount = struct.getLong("MeasurementRequisitionCount")

      return buildRequisition(struct, struct, participantStructs, dataProvidersCount.toInt())
    }

    fun buildRequisition(
      measurementStruct: Struct,
      requisitionStruct: Struct,
      participantStructs: Map<String, Struct>,
      dataProviderCount: Int,
    ) = requisition {
      externalMeasurementConsumerId = measurementStruct.getLong("ExternalMeasurementConsumerId")
      externalMeasurementId = measurementStruct.getLong("ExternalMeasurementId")
      externalRequisitionId = requisitionStruct.getLong("ExternalRequisitionId")
      externalDataProviderId = requisitionStruct.getLong("ExternalDataProviderId")
      updateTime = requisitionStruct.getTimestamp("UpdateTime").toProto()
      state = requisitionStruct.getProtoEnum("RequisitionState", Requisition.State::forNumber)
      if (!measurementStruct.isNull("ExternalComputationId")) {
        externalComputationId = measurementStruct.getLong("ExternalComputationId")
      }
      if (!requisitionStruct.isNull("FulfillingDuchyId")) {
        val fulfillingDuchyId = requisitionStruct.getLong("FulfillingDuchyId")
        externalFulfillingDuchyId =
          checkNotNull(DuchyIds.getExternalId(fulfillingDuchyId)) {
            "External ID not found for fulfilling Duchy $fulfillingDuchyId"
          }
      }
      for ((externalDuchyId, participantStruct) in participantStructs) {
        duchies[externalDuchyId] = buildDuchyValue(participantStruct)
      }
      details =
        requisitionStruct.getProtoMessage(
          "RequisitionDetails",
          RequisitionDetails.getDefaultInstance(),
        )
      dataProviderCertificate = CertificateReader.buildDataProviderCertificate(requisitionStruct)

      parentMeasurement = buildParentMeasurement(measurementStruct, dataProviderCount)
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
        struct.getProtoMessage(
          "ParticipantDetails",
          ComputationParticipantDetails.getDefaultInstance(),
        )
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (participantDetails.protocolCase) {
        ComputationParticipantDetails.ProtocolCase.LIQUID_LEGIONS_V2 -> {
          liquidLegionsV2 = participantDetails.liquidLegionsV2
        }
        ComputationParticipantDetails.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
          reachOnlyLiquidLegionsV2 = participantDetails.reachOnlyLiquidLegionsV2
        }
        ComputationParticipantDetails.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
          honestMajorityShareShuffle = participantDetails.honestMajorityShareShuffle
        }
        // Protocol may only be set after computation participant sets requisition params.
        ComputationParticipantDetails.ProtocolCase.PROTOCOL_NOT_SET -> Unit
      }
    }

    private fun buildParentMeasurement(struct: Struct, dataProviderCount: Int) = parentMeasurement {
      val measurementDetails =
        struct.getProtoMessage("MeasurementDetails", MeasurementDetails.getDefaultInstance())
      apiVersion = measurementDetails.apiVersion
      externalMeasurementConsumerCertificateId =
        struct.getLong("ExternalMeasurementConsumerCertificateId")
      measurementSpec = measurementDetails.measurementSpec
      measurementSpecSignature = measurementDetails.measurementSpecSignature
      measurementSpecSignatureAlgorithmOid = measurementDetails.measurementSpecSignatureAlgorithmOid
      protocolConfig = measurementDetails.protocolConfig
      state = struct.getProtoEnum("MeasurementState", Measurement.State::forNumber)
      dataProvidersCount = dataProviderCount
    }
  }
}
