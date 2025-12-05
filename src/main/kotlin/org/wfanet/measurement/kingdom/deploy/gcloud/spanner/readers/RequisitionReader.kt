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
import com.google.cloud.spanner.ValueBinder
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.FillableTemplate
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ComputationParticipantDetails
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementDetails
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionKt.duchyValue
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.TrusTeeParams
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags

class RequisitionReader private constructor(override val builder: Statement.Builder) :
  BaseSpannerReader<RequisitionReader.Result>() {
  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val requisitionId: InternalId,
    val requisition: Requisition,
    val measurementDetails: MeasurementDetails,
  )

  enum class Parent(val sqlTemplate: FillableTemplate) {
    MEASUREMENT(
      FillableTemplate(
        """
        @{spanner_emulator.disable_query_null_filtered_index_check=TRUE}
        SELECT
          $COMMON_COLUMNS
          $COMPUTATION_PARTICIPANTS_COLUMN
          $MEASUREMENT_REQUISITIONS_COUNT_COLUMN
        FROM
          Measurements
          JOIN MeasurementConsumers USING (MeasurementConsumerId)
          JOIN Requisitions USING (MeasurementConsumerId, MeasurementId)
          JOIN DataProviders USING (DataProviderId)
          JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
          JOIN DataProviderCertificates ON (
            DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId
          )
          JOIN Certificates ON (Certificates.CertificateId = DataProviderCertificates.CertificateId)
        {{whereClause}}
        {{orderByClause}}
        {{limitClause}}
        """
          .trimIndent()
      )
    ),
    DATA_PROVIDER(
      FillableTemplate(
        """
        @{spanner_emulator.disable_query_null_filtered_index_check=TRUE}
        WITH FilteredRequisitions AS (
          SELECT
            Requisitions.*,
            ExternalDataProviderId,
            ExternalMeasurementConsumerId,
            $COMPUTATION_PARTICIPANTS_COLUMN
          FROM
            DataProviders
            JOIN Requisitions USING (DataProviderId)
            JOIN MeasurementConsumers USING (MeasurementConsumerId)
          {{whereClause}}
          {{orderByClause}}
          {{limitClause}}
        )
        SELECT
          $COMMON_COLUMNS
          ComputationParticipants,
          $MEASUREMENT_REQUISITIONS_COUNT_COLUMN
        FROM
          FilteredRequisitions AS Requisitions
          JOIN Measurements USING (MeasurementConsumerId, MeasurementId)
          JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
          JOIN DataProviderCertificates ON (
            DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId
          )
          JOIN Certificates ON (Certificates.CertificateId = DataProviderCertificates.CertificateId)
        {{orderByClause}}
        """
          .trimIndent()
      )
    ),
    NONE(
      FillableTemplate(
        """
        @{spanner_emulator.disable_query_null_filtered_index_check=TRUE}
        SELECT
          $COMMON_COLUMNS
          $COMPUTATION_PARTICIPANTS_COLUMN
          $MEASUREMENT_REQUISITIONS_COUNT_COLUMN
        FROM
          Requisitions
          JOIN DataProviders USING (DataProviderId)
          JOIN MeasurementConsumers USING (MeasurementConsumerId)
          JOIN Measurements USING (MeasurementConsumerId, MeasurementId)
          JOIN MeasurementConsumerCertificates USING (MeasurementConsumerId, CertificateId)
          JOIN DataProviderCertificates ON (
            DataProviderCertificates.CertificateId = Requisitions.DataProviderCertificateId
          )
          JOIN Certificates ON (Certificates.CertificateId = DataProviderCertificates.CertificateId)
        {{whereClause}}
        {{orderByClause}}
        {{limitClause}}
        """
          .trimIndent()
      )
    ),
  }

  class Builder(parent: Parent) {
    private val sqlTemplate: FillableTemplate = parent.sqlTemplate
    private val statementBuilder = Statement.newBuilder("")

    var whereClause: String = ""
    var orderByClause: String = ""
    var limitClause: String = ""

    fun bind(parameter: String): ValueBinder<Statement.Builder> = statementBuilder.bind(parameter)

    fun build(): RequisitionReader {
      statementBuilder.append(
        sqlTemplate.fill(
          mapOf(
            "whereClause" to whereClause,
            "orderByClause" to orderByClause,
            "limitClause" to limitClause,
          )
        )
      )
      return RequisitionReader(statementBuilder)
    }
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

  companion object {
    private object Params {
      const val EXTERNAL_COMPUTATION_ID = "externalComputationId"
      const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
      const val EXTERNAL_REQUISITION_ID = "externalRequisitionId"
    }

    private val COMMON_COLUMNS =
      """
      MeasurementConsumerId,
      MeasurementId,
      RequisitionId,
      Requisitions.UpdateTime,
      ExternalRequisitionId,
      Requisitions.State AS RequisitionState,
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
      Measurements.State AS MeasurementState,
      MeasurementDetails,
      Measurements.CreateTime,
      """
        .trimIndent()

    private val COMPUTATION_PARTICIPANTS_COLUMN =
      """
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
      ) AS ComputationParticipants,
      """
        .trimIndent()

    private val MEASUREMENT_REQUISITIONS_COUNT_COLUMN =
      """
      (
        SELECT
          COUNT(DataProviderId),
        FROM
          Requisitions
        WHERE
          Requisitions.MeasurementConsumerId = Measurements.MeasurementConsumerId
          AND Requisitions.MeasurementId = Measurements.MeasurementId
      ) AS MeasurementRequisitionCount,
      """
        .trimIndent()

    fun build(parent: Parent, fillBuilder: Builder.() -> Unit): RequisitionReader {
      return Builder(parent).apply(fillBuilder).build()
    }

    suspend fun readByExternalDataProviderId(
      readContext: AsyncDatabaseClient.ReadContext,
      externalDataProviderId: ExternalId,
      externalRequisitionId: ExternalId,
    ): Result? {
      val whereClause =
        """
        WHERE
          ExternalRequisitionId = @${Params.EXTERNAL_REQUISITION_ID}
          AND ExternalDataProviderId = @${Params.EXTERNAL_DATA_PROVIDER_ID}
        """
          .trimIndent()
      return build(Parent.DATA_PROVIDER) {
          this.whereClause = whereClause
          bind(Params.EXTERNAL_DATA_PROVIDER_ID).to(externalDataProviderId)
          bind(Params.EXTERNAL_REQUISITION_ID).to(externalRequisitionId)
        }
        .execute(readContext)
        .singleOrNull()
    }

    suspend fun readByExternalComputationId(
      readContext: AsyncDatabaseClient.ReadContext,
      externalComputationId: ExternalId,
      externalRequisitionId: ExternalId,
    ): Result? {
      val whereClause =
        """
        WHERE
          ExternalComputationId = @${Params.EXTERNAL_COMPUTATION_ID}
          AND ExternalRequisitionId = @${Params.EXTERNAL_REQUISITION_ID}
        """
          .trimIndent()
      return build(Parent.MEASUREMENT) {
          this.whereClause = whereClause
          bind(Params.EXTERNAL_COMPUTATION_ID).to(externalComputationId)
          bind(Params.EXTERNAL_REQUISITION_ID).to(externalRequisitionId)
        }
        .execute(readContext)
        .singleOrNull()
    }

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
      etag = ETags.computeETag(requisitionStruct.getTimestamp("UpdateTime"))
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
        ComputationParticipantDetails.ProtocolCase.TRUS_TEE -> {
          trusTee = TrusTeeParams.getDefaultInstance()
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
      createTime = struct.getTimestamp("CreateTime").toProto()
    }
  }
}
