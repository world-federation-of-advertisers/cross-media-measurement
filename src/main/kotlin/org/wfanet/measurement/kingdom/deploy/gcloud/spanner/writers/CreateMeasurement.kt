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
import java.time.Clock
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toSet
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
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotActiveException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamDataProviders
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.readDataProviderId
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SpannerWriter.TransactionScope

/**
 * Creates a measurement in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * @throws [DataProviderNotFoundException] DataProvider not found
 * @throws [MeasurementConsumerCertificateNotFoundException] MeasurementConsumer's Certificate not
 *   found
 * @throws [CertificateIsInvalidException] Certificate is invalid
 * @throws [DuchyNotActiveException] One or more required duchies were inactive at measurement
 *   creation time
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

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
    return when (measurement.details.protocolConfig.protocolCase) {
      ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 ->
        createComputedMeasurement(measurement, measurementConsumerId)
      ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET ->
        createDirectMeasurement(measurement, measurementConsumerId)
    }
  }

  private suspend fun TransactionScope.createComputedMeasurement(
    measurement: Measurement,
    measurementConsumerId: InternalId
  ): Measurement {
    val initialMeasurementState = Measurement.State.PENDING_REQUISITION_PARAMS

    val requiredDuchyIds: Set<String> =
      Llv2ProtocolConfig.requiredExternalDuchyIds +
        readDataProviderRequiredDuchies(
          measurement.dataProvidersMap.keys.map { ExternalId(it) }.toSet()
        )

    val now = Clock.systemUTC().instant()
    val requiredDuchyEntries = DuchyIds.entries.filter { it.externalDuchyId in requiredDuchyIds }
    requiredDuchyEntries.forEach {
      if (!it.isActive(now)) {
        throw DuchyNotActiveException(it.externalDuchyId)
      }
    }

    val includedDuchyEntries =
      if (requiredDuchyEntries.size < Llv2ProtocolConfig.minimumNumberOfRequiredDuchies) {
        val additionalActiveDuchies =
          DuchyIds.entries.filter { it !in requiredDuchyEntries && it.isActive(now) }
        requiredDuchyEntries +
          additionalActiveDuchies.take(
            Llv2ProtocolConfig.minimumNumberOfRequiredDuchies - requiredDuchyEntries.size
          )
      } else {
        requiredDuchyEntries
      }

    if (includedDuchyEntries.size < Llv2ProtocolConfig.minimumNumberOfRequiredDuchies) {
      throw IllegalStateException("Not enough active duchies to run the computation")
    }

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

    includedDuchyEntries.forEach { entry ->
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
    val externalMeasurementConsumerId =
      ExternalId(measurement.externalMeasurementConsumerCertificateId)
    val reader =
      CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
        .bindWhereClause(measurementConsumerId, externalMeasurementConsumerId)
    val measurementConsumerCertificateId =
      reader.execute(transactionContext).singleOrNull()?.let { validateCertificate(it) }
        ?: throw MeasurementConsumerCertificateNotFoundException(
          externalMeasurementConsumerId,
          externalMeasurementId
        )

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
      insertRequisition(
        measurementConsumerId,
        measurementId,
        ExternalId(externalDataProviderId),
        dataProviderValue,
        initialRequisitionState
      )
    }
  }

  private suspend fun TransactionScope.insertRequisition(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    externalDataProviderId: ExternalId,
    dataProviderValue: Measurement.DataProviderValue,
    initialRequisitionState: Requisition.State
  ) {
    val dataProviderId = transactionContext.readDataProviderId(externalDataProviderId)
    val reader =
      CertificateReader(CertificateReader.ParentType.DATA_PROVIDER)
        .bindWhereClause(
          dataProviderId,
          ExternalId(dataProviderValue.externalDataProviderCertificateId)
        )

    val dataProviderCertificateId =
      reader.execute(transactionContext).singleOrNull()?.let { validateCertificate(it) }
        ?: throw DataProviderCertificateNotFoundException(
          externalDataProviderId,
          ExternalId(dataProviderValue.externalDataProviderCertificateId)
        )

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
      """
        .trimIndent()

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
  return transactionContext
    .readRowUsingIndex(
      "MeasurementConsumers",
      "MeasurementConsumersByExternalId",
      Key.of(externalMeasurementConsumerId.value),
      column
    )
    ?.let { struct -> InternalId(struct.getLong(column)) }
    ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId) {
      "MeasurementConsumer with external ID $externalMeasurementConsumerId not found"
    }
}

private suspend fun TransactionScope.readDataProviderRequiredDuchies(
  externalDataProviderIds: Set<ExternalId>
): Set<String> {
  return StreamDataProviders(externalDataProviderIds)
    .execute(transactionContext)
    .map { it.dataProvider.requiredExternalDuchyIdsList.asFlow() }
    .flattenConcat()
    .toSet()
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
    throw CertificateIsInvalidException()
  }

  return certificateResult.certificateId
}
