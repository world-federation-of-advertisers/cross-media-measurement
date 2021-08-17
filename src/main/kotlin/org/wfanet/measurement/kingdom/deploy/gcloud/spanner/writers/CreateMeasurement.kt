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

import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader

// TODO(@uakyol) : Read this from protocol config when it is implemented.
private const val FAKE_PROTOCOL_CONFIG_ID = 0L

/** Creates a measurement in the database. */
class CreateMeasurement(private val measurement: Measurement) :
  SpannerWriter<Measurement, Measurement>() {

  override suspend fun TransactionScope.runTransaction(): Measurement {
    val measurementConsumerId =
      MeasurementConsumerReader()
        .readExternalIdOrNull(
          transactionContext,
          ExternalId(measurement.externalMeasurementConsumerId)
        )
        ?.measurementConsumerId
        ?: throw KingdomInternalException(
          KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
        )

    val existingMeasurement = findExistingMeasurement(measurementConsumerId)
    if (existingMeasurement != null) {
      return existingMeasurement
    }

    // Insert this measurement into Measurements
    val measurementId = idGenerator.generateInternalId().value

    val measurementConsumerCertificateId =
      getMeasurementConsumerCertificateId(
        measurement.externalMeasurementConsumerId,
        measurement.externalMeasurementConsumerCertificateId
      )

    val measurement =
      createNewMeasurement(measurementId, measurementConsumerId, measurementConsumerCertificateId)

    // Insert into Requisitions for each EDP
    for ((externalDataProviderIdValue, dataProviderValue) in measurement.dataProvidersMap) {
      val externalDataProviderId = ExternalId(externalDataProviderIdValue)
      val dataProviderId =
        DataProviderReader()
          .readExternalIdOrNull(transactionContext, externalDataProviderId)
          ?.dataProviderId
          ?: throw KingdomInternalException(KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND)

      val dataProviderCertificateId =
        getDataProviderCertificateId(
          externalDataProviderId.value,
          dataProviderValue.externalDataProviderCertificateId
        )
      createRequisition(
        externalDataProviderId.value,
        measurementConsumerId,
        measurementId,
        dataProviderId,
        dataProviderCertificateId
      )
    }

    // Insert into ComputationParticipants for each Duchy
    DuchyIds.entries.forEach { entry ->
      createComputationParticipant(measurementConsumerId, measurementId, entry.internalDuchyId)
    }
    return measurement
  }

  private suspend fun TransactionScope.createNewMeasurement(
    measurementId: Long,
    measurementConsumerId: Long,
    measurementConsumerCertificateId: Long
  ): Measurement {
    val externalMeasurementId = idGenerator.generateExternalId()
    val externalComputationId = idGenerator.generateExternalId()

    insertMutation("Measurements") {
        set("MeasurementId" to measurementId)
        set("MeasurementConsumerId" to measurementConsumerId)
        set("ExternalMeasurementId" to externalMeasurementId.value)
        set("ExternalComputationId" to externalComputationId.value)
        set("ProvidedMeasurementId" to measurement.providedMeasurementId)
        set("CertificateId" to measurementConsumerCertificateId)
        set("ProtocolConfigId" to FAKE_PROTOCOL_CONFIG_ID)
        set("State" to Measurement.State.PENDING_REQUISITION_PARAMS)
        set("MeasurementDetails" to measurement.details)
        setJson("MeasurementDetailsJson" to measurement.details)
        set("CreateTime" to Value.COMMIT_TIMESTAMP)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      }
      .bufferTo(transactionContext)

    return measurement
      .toBuilder()
      .also {
        it.externalMeasurementId = externalMeasurementId.value
        it.externalComputationId = externalComputationId.value
      }
      .build()
  }

  private suspend fun TransactionScope.createComputationParticipant(
    measurementConsumerId: Long,
    measurementId: Long,
    duchyId: Long
  ) {
    // TODO(@uakyol): populate all the relevant fields for a computationParticipants.
    insertMutation("ComputationParticipants") {
        set("MeasurementConsumerId" to measurementConsumerId)
        set("MeasurementId" to measurementId)
        set("DuchyId" to duchyId)
        set("State" to ComputationParticipant.State.CREATED)
      }
      .bufferTo(transactionContext)
  }

  private suspend fun TransactionScope.createRequisition(
    externalDataProviderId: Long,
    measurementConsumerId: Long,
    measurementId: Long,
    dataProviderId: Long,
    dataProviderCertificateId: Long
  ) {
    val internalRequisitionId = idGenerator.generateInternalId()
    val externalRequisitionId = idGenerator.generateExternalId()

    insertMutation("Requisitions") {
        set("MeasurementConsumerId" to measurementConsumerId)
        set("MeasurementId" to measurementId)
        set("RequisitionId" to internalRequisitionId.value)
        set("DataProviderId" to dataProviderId)
        set("CreateTime" to Value.COMMIT_TIMESTAMP)
        set("ExternalRequisitionId" to externalRequisitionId.value)
        set("DataProviderCertificateId" to dataProviderCertificateId)
        set("State" to Requisition.State.UNFULFILLED)
      }
      .bufferTo(transactionContext)
  }

  private suspend fun TransactionScope.getMeasurementConsumerCertificateId(
    externalMeasurementConsumerId: Long,
    externalMeasurementConsumerCertificateId: Long
  ): Long {
    val measurementConsumerGetCertificateRequest =
      GetCertificateRequest.newBuilder()
        .also {
          it.externalMeasurementConsumerId = externalMeasurementConsumerId
          it.externalCertificateId = measurement.externalMeasurementConsumerCertificateId
        }
        .build()

    return CertificateReader(measurementConsumerGetCertificateRequest)
      .readExternalIdOrNull(
        transactionContext,
        ExternalId(measurementConsumerGetCertificateRequest.externalCertificateId)
      )
      ?.certificateId
      ?: throw KingdomInternalException(
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
      )
  }

  private suspend fun TransactionScope.getDataProviderCertificateId(
    externalDataProviderId: Long,
    externalDataProviderCertificateId: Long
  ): Long {
    val dataProviderGetCertificateRequest =
      GetCertificateRequest.newBuilder()
        .also {
          it.externalDataProviderId = externalDataProviderId
          it.externalCertificateId = externalDataProviderCertificateId
        }
        .build()
    return CertificateReader(dataProviderGetCertificateRequest)
      .readExternalIdOrNull(
        transactionContext,
        ExternalId(dataProviderGetCertificateRequest.externalCertificateId)
      )
      ?.certificateId
      ?: throw KingdomInternalException(KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND)
  }

  private suspend fun TransactionScope.findExistingMeasurement(
    measurementConsumerId: Long
  ): Measurement? {
    val whereClause =
      """
      WHERE Measurements.MeasurementConsumerId = @measurement_consumer_id
        AND Measurements.ProvidedMeasurementId = @provided_measurement_id
      """.trimIndent()

    return MeasurementReader(Measurement.View.DEFAULT)
      .withBuilder {
        appendClause(whereClause)
        bind("measurement_consumer_id").to(measurementConsumerId)
        bind("provided_measurement_id").to(measurement.providedMeasurementId)
      }
      .execute(transactionContext)
      .map { it.measurement }
      .singleOrNull()
  }

  override fun ResultScope<Measurement>.buildResult(): Measurement {
    val measurement = checkNotNull(transactionResult)
    return if (measurement.hasCreateTime()) {
      measurement
    } else {
      measurement.toBuilder().apply { createTime = commitTimestamp.toProto() }.build()
    }
  }
}
