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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SpannerWriter.TransactionScope

/**
 * Creates a measurement in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND]
 * * [ErrorCode.DATA_PROVIDER_NOT_FOUND]
 * * [ErrorCode.CERTIFICATE_NOT_FOUND]
 * * [ErrorCode.CERTIFICATE_IS_INVALID]
 */
class CreateMeasurement(private val measurement: Measurement) :
  SpannerWriter<Measurement, Measurement>() {

  override suspend fun TransactionScope.runTransaction(): Measurement {
    val measurementConsumerId: InternalId =
      readMeasurementConsumerId(ExternalId(measurement.externalMeasurementConsumerId))

    if (measurement.providedMeasurementId.isNotBlank()) {
      val existingMeasurement = findExistingMeasurement(measurementConsumerId)
      if (existingMeasurement != null) {
        return existingMeasurement
      }
    }

    // protocol has to be set for the measurement to require computation
    return if (measurement.details.protocolConfig.protocolCase !=
        ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET
    ) {
      createComputationMeasurement(measurement, measurementConsumerId)
    } else {
      createDirectMeasurement(measurement, measurementConsumerId)
    }
  }

  private suspend fun TransactionScope.createComputationMeasurement(
    measurement: Measurement,
    measurementConsumerId: InternalId
  ): Measurement {
    val initialMeasurementState = Measurement.State.PENDING_REQUISITION_PARAMS

    val measurementId: InternalId = idGenerator.generateInternalId()
    val externalMeasurementId: ExternalId = idGenerator.generateExternalId()
    val externalComputationId: ExternalId = idGenerator.generateExternalId()
    insertMeasurement(
      measurementConsumerId,
      measurementId,
      externalMeasurementId,
      externalComputationId,
      initialMeasurementState
    )

    // Insert into Requisitions for each EDP
    insertRequisitions(
      measurementConsumerId,
      measurementId,
      measurement.dataProvidersMap,
      Requisition.State.PENDING_PARAMS
    )

    DuchyIds.entries.forEach { entry ->
      insertComputationParticipant(
        measurementConsumerId,
        measurementId,
        InternalId(entry.internalDuchyId),
      )
    }

    return measurement.copy {
      this.externalMeasurementId = externalMeasurementId.value
      this.externalComputationId = externalComputationId.value
      state = initialMeasurementState
    }
  }

  private suspend fun TransactionScope.createDirectMeasurement(
    measurement: Measurement,
    measurementConsumerId: InternalId
  ): Measurement {
    val initialMeasurementState = Measurement.State.PENDING_REQUISITION_FULFILLMENT

    val measurementId: InternalId = idGenerator.generateInternalId()
    val externalMeasurementId: ExternalId = idGenerator.generateExternalId()
    insertMeasurement(
      measurementConsumerId,
      measurementId,
      externalMeasurementId,
      null,
      initialMeasurementState
    )

    // Insert into Requisitions for each EDP
    insertRequisitions(
      measurementConsumerId,
      measurementId,
      measurement.dataProvidersMap,
      Requisition.State.UNFULFILLED
    )

    return measurement.copy {
      this.externalMeasurementId = externalMeasurementId.value
      state = initialMeasurementState
    }
  }

  private suspend fun TransactionScope.insertMeasurement(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    externalMeasurementId: ExternalId,
    externalComputationId: ExternalId?,
    initialMeasurementState: Measurement.State
  ) {
    val reader =
      CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
        .bindWhereClause(
          measurementConsumerId,
          ExternalId(measurement.externalMeasurementConsumerCertificateId)
        )
    val measurementConsumerCertificateId =
      reader.execute(transactionContext).singleOrNull()?.let { validateCertificate(it) }
        ?: throw KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND)

    transactionContext.bufferInsertMutation("Measurements") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("ExternalMeasurementId" to externalMeasurementId)
      if (externalComputationId != null) {
        set("ExternalComputationId" to externalComputationId)
      }
      if (measurement.providedMeasurementId.isNotBlank()) {
        set("ProvidedMeasurementId" to measurement.providedMeasurementId)
      }
      set("CertificateId" to measurementConsumerCertificateId)
      set("State" to initialMeasurementState)
      set("MeasurementDetails" to measurement.details)
      setJson("MeasurementDetailsJson" to measurement.details)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }
  }

  private fun TransactionScope.insertComputationParticipant(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    duchyId: InternalId
  ) {
    val participantDetails = ComputationParticipant.Details.getDefaultInstance()
    transactionContext.bufferInsertMutation("ComputationParticipants") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to duchyId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State" to ComputationParticipant.State.CREATED)
      set("ParticipantDetails" to participantDetails)
      setJson("ParticipantDetailsJson" to participantDetails)
    }
  }

  private suspend fun TransactionScope.insertRequisitions(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    dataProvidersMap: Map<Long, Measurement.DataProviderValue>,
    initialRequisitionState: Requisition.State,
  ) {
    for ((externalDataProviderId, dataProviderValue) in dataProvidersMap) {
      val dataProviderId = readDataProviderId(ExternalId(externalDataProviderId))
      insertRequisition(
        measurementConsumerId,
        measurementId,
        dataProviderId,
        dataProviderValue,
        initialRequisitionState
      )
    }
  }

  private suspend fun TransactionScope.insertRequisition(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    dataProviderId: InternalId,
    dataProviderValue: Measurement.DataProviderValue,
    initialRequisitionState: Requisition.State
  ) {
    val reader =
      CertificateReader(CertificateReader.ParentType.DATA_PROVIDER)
        .bindWhereClause(
          dataProviderId,
          ExternalId(dataProviderValue.externalDataProviderCertificateId)
        )

    val dataProviderCertificateId =
      reader.execute(transactionContext).singleOrNull()?.let { validateCertificate(it) }
        ?: throw KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND)

    val requisitionId = idGenerator.generateInternalId()
    val externalRequisitionId = idGenerator.generateExternalId()
    val details: Requisition.Details =
      RequisitionKt.details {
        dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
        dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
        encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
        nonceHash = dataProviderValue.nonceHash
      }

    transactionContext.bufferInsertMutation("Requisitions") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("RequisitionId" to requisitionId)
      set("DataProviderId" to dataProviderId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("ExternalRequisitionId" to externalRequisitionId)
      set("DataProviderCertificateId" to dataProviderCertificateId)
      set("State" to initialRequisitionState)
      set("RequisitionDetails" to details)
      setJson("RequisitionDetailsJson" to details)
    }
  }

  private suspend fun TransactionScope.findExistingMeasurement(
    measurementConsumerId: InternalId
  ): Measurement? {
    val params =
      object {
        val MEASUREMENT_CONSUMER_ID = "measurementConsumerId"
        val PROVIDED_MEASUREMENT_ID = "providedMeasurementId"
      }
    val whereClause =
      """
      WHERE MeasurementConsumerId = @${params.MEASUREMENT_CONSUMER_ID}
        AND ProvidedMeasurementId = @${params.PROVIDED_MEASUREMENT_ID}
      """.trimIndent()

    return MeasurementReader(Measurement.View.DEFAULT)
      .fillStatementBuilder {
        appendClause(whereClause)
        bind(params.MEASUREMENT_CONSUMER_ID to measurementConsumerId)
        bind(params.PROVIDED_MEASUREMENT_ID to measurement.providedMeasurementId)
      }
      .execute(transactionContext)
      .map { it.measurement }
      .singleOrNull()
  }

  override fun ResultScope<Measurement>.buildResult(): Measurement {
    val measurement = checkNotNull(transactionResult)
    if (measurement.hasCreateTime()) {
      // Existing measurement was returned instead of inserting new one, so keep its timestamps.
      return measurement
    }
    return measurement.copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }
}

private suspend fun TransactionScope.readMeasurementConsumerId(
  externalMeasurementConsumerId: ExternalId
): InternalId {
  val column = "MeasurementConsumerId"
  return transactionContext.readRowUsingIndex(
      "MeasurementConsumers",
      "MeasurementConsumersByExternalId",
      Key.of(externalMeasurementConsumerId.value),
      column
    )
    ?.let { struct -> InternalId(struct.getLong(column)) }
    ?: throw KingdomInternalException(ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND) {
      "MeasurementConsumer with external ID $externalMeasurementConsumerId not found"
    }
}

private suspend fun TransactionScope.readDataProviderId(
  externalDataProviderId: ExternalId
): InternalId {
  val column = "DataProviderId"
  return transactionContext.readRowUsingIndex(
      "DataProviders",
      "DataProvidersByExternalId",
      Key.of(externalDataProviderId.value),
      column
    )
    ?.let { struct -> InternalId(struct.getLong(column)) }
    ?: throw KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND) {
      "DataProvider with external ID $externalDataProviderId not found"
    }
}

/**
 * Returns the internal Certificate Id if the revocation state has not been set and the current time
 * is inside the valid time period.
 *
 * Throws a [ErrorCode.CERTIFICATE_IS_INVALID] otherwise.
 */
private fun validateCertificate(
  certificateResult: CertificateReader.Result,
): InternalId {
  if (!certificateResult.isValid) {
    throw KingdomInternalException(ErrorCode.CERTIFICATE_IS_INVALID)
  }

  return certificateResult.certificateId
}
