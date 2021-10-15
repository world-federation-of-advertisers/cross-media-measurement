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
import java.time.Instant
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SpannerWriter.TransactionScope

private val INITIAL_MEASUREMENT_STATE = Measurement.State.PENDING_REQUISITION_PARAMS

/**
 * Creates a measurement in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND]
 * * [KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND]
 * * [KingdomInternalException.Code.CERTIFICATE_NOT_FOUND]
 */
class CreateMeasurement(private val clock: Clock, private val measurement: Measurement) :
  SpannerWriter<Measurement, Measurement>() {

  override suspend fun TransactionScope.runTransaction(): Measurement {
    val now = clock.instant()
    val measurementConsumerId: InternalId =
      readMeasurementConsumerId(ExternalId(measurement.externalMeasurementConsumerId))

    if (measurement.providedMeasurementId.isNotBlank()) {
      val existingMeasurement = findExistingMeasurement(measurementConsumerId)
      if (existingMeasurement != null) {
        return existingMeasurement
      }
    }

    val measurementId: InternalId = idGenerator.generateInternalId()
    val externalMeasurementId: ExternalId = idGenerator.generateExternalId()
    val externalComputationId: ExternalId = idGenerator.generateExternalId()
    insertMeasurement(
      now,
      measurementConsumerId,
      measurementId,
      externalMeasurementId,
      externalComputationId
    )

    // Insert into Requisitions for each EDP
    for ((externalDataProviderId, dataProviderValue) in measurement.dataProvidersMap) {
      val dataProviderId = readDataProviderId(ExternalId(externalDataProviderId))
      insertRequisition(
        now,
        measurementConsumerId,
        measurementId,
        dataProviderId,
        dataProviderValue
      )
    }

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
      state = INITIAL_MEASUREMENT_STATE
    }
  }

  private suspend fun TransactionScope.insertMeasurement(
    now: Instant,
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    externalMeasurementId: ExternalId,
    externalComputationId: ExternalId
  ) {
    val measurementConsumerCertificateId =
      readMeasurementConsumerCertificateId(
        measurementConsumerId,
        ExternalId(measurement.externalMeasurementConsumerCertificateId)
      )

    validateCertificate(
      CertificateReader.ParentType.MEASUREMENT_CONSUMER,
      measurementConsumerId,
      ExternalId(measurement.externalMeasurementConsumerCertificateId),
      now
    )

    transactionContext.bufferInsertMutation("Measurements") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("ExternalMeasurementId" to externalMeasurementId)
      set("ExternalComputationId" to externalComputationId)
      if (measurement.providedMeasurementId.isNotBlank()) {
        set("ProvidedMeasurementId" to measurement.providedMeasurementId)
      }
      set("CertificateId" to measurementConsumerCertificateId)
      set("State" to INITIAL_MEASUREMENT_STATE)
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

  private suspend fun TransactionScope.insertRequisition(
    now: Instant,
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    dataProviderId: InternalId,
    dataProviderValue: Measurement.DataProviderValue
  ) {
    val dataProviderCertificateId =
      readDataProviderCertificateId(
        dataProviderId,
        ExternalId(dataProviderValue.externalDataProviderCertificateId)
      )

    validateCertificate(
      CertificateReader.ParentType.DATA_PROVIDER,
      dataProviderId,
      ExternalId(dataProviderValue.externalDataProviderCertificateId),
      now
    )

    val requisitionId = idGenerator.generateInternalId()
    val externalRequisitionId = idGenerator.generateExternalId()
    val details: Requisition.Details =
      RequisitionKt.details {
        dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
        dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
        encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
      }

    transactionContext.bufferInsertMutation("Requisitions") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("RequisitionId" to requisitionId)
      set("DataProviderId" to dataProviderId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("ExternalRequisitionId" to externalRequisitionId)
      set("DataProviderCertificateId" to dataProviderCertificateId)
      set("State" to Requisition.State.UNFULFILLED)
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
    ?: throw KingdomInternalException(
      KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
    ) { "MeasurementConsumer with external ID $externalMeasurementConsumerId not found" }
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
    ?: throw KingdomInternalException(KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND) {
      "DataProvider with external ID $externalDataProviderId not found"
    }
}

private suspend fun TransactionScope.readDataProviderCertificateId(
  dataProviderId: InternalId,
  externalCertificateId: ExternalId
): InternalId {
  val column = "CertificateId"
  return transactionContext.readRowUsingIndex(
      "DataProviderCertificates",
      "DataProviderCertificatesByExternalId",
      Key.of(dataProviderId.value, externalCertificateId.value),
      column
    )
    ?.let { struct -> InternalId(struct.getLong(column)) }
    ?: throw KingdomInternalException(KingdomInternalException.Code.CERTIFICATE_NOT_FOUND) {
      "Certificate for DataProvider ${dataProviderId.value} with external ID " +
        "$externalCertificateId not found"
    }
}

private suspend fun TransactionScope.readMeasurementConsumerCertificateId(
  measurementConsumerId: InternalId,
  externalCertificateId: ExternalId
): InternalId {
  val column = "CertificateId"
  return transactionContext.readRowUsingIndex(
      "MeasurementConsumerCertificates",
      "MeasurementConsumerCertificatesByExternalId",
      Key.of(measurementConsumerId.value, externalCertificateId.value),
      column
    )
    ?.let { struct -> InternalId(struct.getLong(column)) }
    ?: throw KingdomInternalException(KingdomInternalException.Code.CERTIFICATE_NOT_FOUND) {
      "Certificate for MeasurementConsumer ${measurementConsumerId.value} with external ID " +
        "$externalCertificateId not found"
    }
}

private suspend fun TransactionScope.validateCertificate(
  parentType: CertificateReader.ParentType,
  parentId: InternalId,
  externalCertificateId: ExternalId,
  now: Instant
) {
  val reader = CertificateReader(parentType).bindWhereClause(parentId, externalCertificateId)

  reader.execute(transactionContext).singleOrNull()?.let {
    if (it.certificate.revocationState !=
        Certificate.RevocationState.REVOCATION_STATE_UNSPECIFIED ||
        now.isBefore(it.certificate.notValidBefore.toInstant()) ||
        now.isAfter(it.certificate.notValidAfter.toInstant())
    ) {
      throw KingdomInternalException(KingdomInternalException.Code.CERTIFICATE_IS_INVALID)
    }
  }
    ?: throw KingdomInternalException(KingdomInternalException.Code.CERTIFICATE_NOT_FOUND)
}
