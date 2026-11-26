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
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantDetails
import org.wfanet.measurement.internal.kingdom.CreateMeasurementRequest
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.ModelLineKey
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.populationKey
import org.wfanet.measurement.internal.kingdom.requisitionDetails
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.TrusTeeProtocolConfig
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotActiveException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ETags
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelRollouts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelReleaseReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SpannerWriter.TransactionScope

/**
 * Create measurements in the database.
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
class CreateMeasurements(private val requests: List<CreateMeasurementRequest>) :
  SpannerWriter<List<Measurement>, List<Measurement>>() {

  override suspend fun TransactionScope.runTransaction(): List<Measurement> {
    val measurementConsumerId: InternalId =
      readMeasurementConsumerId(
        ExternalId(requests.first().measurement.externalMeasurementConsumerId)
      )

    val existingMeasurementsMap = findExistingMeasurements(requests, measurementConsumerId)

    val externalMeasurementConsumerCertificateIds: Set<ExternalId> = buildSet {
      requests.forEach { add(ExternalId(it.measurement.externalMeasurementConsumerCertificateId)) }
    }

    val reader =
      CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
        .bindWhereClause(measurementConsumerId, externalMeasurementConsumerCertificateIds)

    val measurementConsumerCertificateIdsMap: Map<Long, InternalId> = buildMap {
      reader.execute(transactionContext).collect {
        validateCertificate(it)
        put(it.certificate.externalCertificateId, it.certificateId)
      }
    }

    val externalDataProviderIdsSet = buildSet {
      requests.map { request ->
        if (existingMeasurementsMap[request.requestId] == null) {
          addAll(request.measurement.dataProvidersMap.keys.map { ExternalId(it) })
          addAll(
            request.measurement.multimeasurementVectorsList.map {
              ExternalId(it.externalDataProviderId)
            }
          )
        }
      }
    }

    // Map of external data provider ID to DataProviderReader.Result
    val dataProvideReaderResultsMap =
      DataProviderReader()
        .readByExternalDataProviderIds(transactionContext, externalDataProviderIdsSet)
        .associateBy {
          if (!it.certificateValid) {
            throw CertificateIsInvalidException()
          }
          it.dataProvider.externalDataProviderId
        }

    externalDataProviderIdsSet.forEach {
      if (dataProvideReaderResultsMap[it.value] == null) {
        throw DataProviderNotFoundException(it)
      }
    }

    return requests.map {
      if (existingMeasurementsMap.containsKey(it.requestId)) {
        existingMeasurementsMap.getValue(it.requestId)
      } else {
        val certificateId =
          measurementConsumerCertificateIdsMap[
            it.measurement.externalMeasurementConsumerCertificateId]
            ?: throw MeasurementConsumerCertificateNotFoundException(
              ExternalId(it.measurement.externalMeasurementConsumerId),
              ExternalId(it.measurement.externalMeasurementConsumerCertificateId),
            )

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
        when (it.measurement.details.protocolConfig.protocolCase) {
          ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2,
          ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2,
          ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
          ProtocolConfig.ProtocolCase.TRUS_TEE,
          ProtocolConfig.ProtocolCase.TRUS_TEE_V2 -> {
            createComputedMeasurement(
              createMeasurementRequest =
                if (it.measurement.details.protocolConfig.hasTrusTeeV2()) {
                  withPinnedPopulation(it)
                } else {
                  it
                },
              measurementConsumerId = measurementConsumerId,
              measurementConsumerCertificateId = certificateId,
              dataProvideReaderResultsMap = dataProvideReaderResultsMap,
            )
          }
          ProtocolConfig.ProtocolCase.DIRECT ->
            createDirectMeasurement(
              createMeasurementRequest = it,
              measurementConsumerId = measurementConsumerId,
              measurementConsumerCertificateId = certificateId,
              dataProvideReaderResultsMap = dataProvideReaderResultsMap,
            )
          ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Protocol is not set.")
        }
      }
    }
  }

  private fun TransactionScope.createComputedMeasurement(
    createMeasurementRequest: CreateMeasurementRequest,
    measurementConsumerId: InternalId,
    measurementConsumerCertificateId: InternalId,
    dataProvideReaderResultsMap: Map<Long, DataProviderReader.Result>,
  ): Measurement {
    val initialMeasurementState = Measurement.State.PENDING_REQUISITION_PARAMS

    // Data provider required duchies only apply to LLv2 protocols.
    val dataProviderRequiredDuchyIds: Set<String> =
      createMeasurementRequest.measurement.dataProvidersMap.keys
        .flatMap {
          dataProvideReaderResultsMap.getValue(it).dataProvider.requiredExternalDuchyIdsList
        }
        .toSet()
    val requiredDuchyIds: Set<String> =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
      when (createMeasurementRequest.measurement.details.protocolConfig.protocolCase) {
        ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 ->
          Llv2ProtocolConfig.requiredExternalDuchyIds + dataProviderRequiredDuchyIds
        ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
          RoLlv2ProtocolConfig.requiredExternalDuchyIds + dataProviderRequiredDuchyIds
        ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
          setOf(
            HmssProtocolConfig.firstNonAggregatorDuchyId,
            HmssProtocolConfig.secondNonAggregatorDuchyId,
            HmssProtocolConfig.aggregatorDuchyId,
          )
        ProtocolConfig.ProtocolCase.TRUS_TEE,
        ProtocolConfig.ProtocolCase.TRUS_TEE_V2 -> setOf(TrusTeeProtocolConfig.duchyId)
        ProtocolConfig.ProtocolCase.DIRECT,
        ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Invalid protocol.")
      }
    val now = Clock.systemUTC().instant()
    val requiredDuchyEntries = DuchyIds.entries.filter { it.externalDuchyId in requiredDuchyIds }
    requiredDuchyEntries.forEach {
      if (!it.isActive(now)) {
        throw DuchyNotActiveException(it.externalDuchyId)
      }
    }
    val minimumNumberOfRequiredDuchies =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
      when (createMeasurementRequest.measurement.details.protocolConfig.protocolCase) {
        ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 ->
          Llv2ProtocolConfig.minimumNumberOfRequiredDuchies
        ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
          RoLlv2ProtocolConfig.minimumNumberOfRequiredDuchies
        ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> HmssProtocolConfig.DUCHY_COUNT
        ProtocolConfig.ProtocolCase.TRUS_TEE,
        ProtocolConfig.ProtocolCase.TRUS_TEE_V2 -> 1
        ProtocolConfig.ProtocolCase.DIRECT,
        ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Invalid protocol.")
      }

    val includedDuchyEntries =
      if (requiredDuchyEntries.size < minimumNumberOfRequiredDuchies) {
        val additionalActiveDuchies =
          DuchyIds.entries.filter { it !in requiredDuchyEntries && it.isActive(now) }
        requiredDuchyEntries +
          additionalActiveDuchies.take(minimumNumberOfRequiredDuchies - requiredDuchyEntries.size)
      } else {
        requiredDuchyEntries
      }

    if (includedDuchyEntries.size < minimumNumberOfRequiredDuchies) {
      throw IllegalStateException("Not enough active duchies to run the computation")
    }

    val measurementId: InternalId = idGenerator.generateInternalId()
    val externalMeasurementId: ExternalId = idGenerator.generateExternalId()
    val externalComputationId: ExternalId = idGenerator.generateExternalId()
    insertMeasurement(
      createMeasurementRequest,
      measurementConsumerId,
      measurementId,
      externalMeasurementId,
      externalComputationId,
      initialMeasurementState,
      measurementConsumerCertificateId,
    )

    includedDuchyEntries.forEach { entry ->
      insertComputationParticipant(
        measurementConsumerId,
        measurementId,
        InternalId(entry.internalDuchyId),
      )
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
    when (createMeasurementRequest.measurement.details.protocolConfig.protocolCase) {
      ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2,
      ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
        // For each EDP, insert a Requisition.
        insertRequisitions(
          measurementConsumerId = measurementConsumerId,
          measurementId = measurementId,
          dataProvidersMap = createMeasurementRequest.measurement.dataProvidersMap,
          dataProvideReaderResultsMap = dataProvideReaderResultsMap,
          initialRequisitionState = Requisition.State.PENDING_PARAMS,
        )
      }
      ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
        val fulfillingDuchies =
          includedDuchyEntries.filter { it.externalDuchyId != HmssProtocolConfig.aggregatorDuchyId }
        // For each EDP, insert a Requisition for each non-aggregator Duchy.
        insertRequisitions(
          measurementConsumerId = measurementConsumerId,
          measurementId = measurementId,
          dataProvidersMap = createMeasurementRequest.measurement.dataProvidersMap,
          dataProvideReaderResultsMap = dataProvideReaderResultsMap,
          initialRequisitionState = Requisition.State.PENDING_PARAMS,
          fulfillingDuchies = fulfillingDuchies,
        )
      }
      ProtocolConfig.ProtocolCase.TRUS_TEE -> {
        val fulfillingDuchy =
          includedDuchyEntries.first { it.externalDuchyId == TrusTeeProtocolConfig.duchyId }
        insertRequisitions(
          measurementConsumerId = measurementConsumerId,
          measurementId = measurementId,
          dataProvidersMap = createMeasurementRequest.measurement.dataProvidersMap,
          dataProvideReaderResultsMap = dataProvideReaderResultsMap,
          initialRequisitionState = Requisition.State.PENDING_PARAMS,
          fulfillingDuchies = listOf(fulfillingDuchy),
        )
      }
      ProtocolConfig.ProtocolCase.TRUS_TEE_V2 -> {
        val fulfillingDuchy =
          includedDuchyEntries.first { it.externalDuchyId == TrusTeeProtocolConfig.duchyId }
        // For each base vector, insert a Requisition.
        insertMultimeasurementRequisitions(
          measurementConsumerId = measurementConsumerId,
          measurementId = measurementId,
          vectors = createMeasurementRequest.measurement.multimeasurementVectorsList,
          dataProvideReaderResultsMap = dataProvideReaderResultsMap,
          fulfillingDuchy = fulfillingDuchy,
        )
      }
      ProtocolConfig.ProtocolCase.DIRECT,
      ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Invalid protocol,")
    }

    return createMeasurementRequest.measurement.copy {
      this.externalMeasurementId = externalMeasurementId.value
      this.externalComputationId = externalComputationId.value
      state = initialMeasurementState
    }
  }

  private fun TransactionScope.createDirectMeasurement(
    createMeasurementRequest: CreateMeasurementRequest,
    measurementConsumerId: InternalId,
    measurementConsumerCertificateId: InternalId,
    dataProvideReaderResultsMap: Map<Long, DataProviderReader.Result>,
  ): Measurement {
    val initialMeasurementState = Measurement.State.PENDING_REQUISITION_FULFILLMENT

    val measurementId: InternalId = idGenerator.generateInternalId()
    val externalMeasurementId: ExternalId = idGenerator.generateExternalId()
    insertMeasurement(
      createMeasurementRequest,
      measurementConsumerId,
      measurementId,
      externalMeasurementId,
      null,
      initialMeasurementState,
      measurementConsumerCertificateId,
    )

    // Insert into Requisitions for each EDP
    insertRequisitions(
      measurementConsumerId = measurementConsumerId,
      measurementId = measurementId,
      dataProvidersMap = createMeasurementRequest.measurement.dataProvidersMap,
      dataProvideReaderResultsMap = dataProvideReaderResultsMap,
      initialRequisitionState = Requisition.State.UNFULFILLED,
    )

    return createMeasurementRequest.measurement.copy {
      this.externalMeasurementId = externalMeasurementId.value
      state = initialMeasurementState
    }
  }

  /**
   * Returns [request] with `MeasurementDetails.population` set to the Population of the latest
   * ModelRollout of its `model_line`, so that a computation's PopulationSpec cannot change under
   * it.
   *
   * @throws RequiredFieldNotSetException if `model_line` is not set
   * @throws ModelRolloutNotFoundException if the model line has no ModelRollout
   * @throws ModelReleaseNotFoundException if the ModelRollout's ModelRelease is not found
   */
  private suspend fun TransactionScope.withPinnedPopulation(
    request: CreateMeasurementRequest
  ): CreateMeasurementRequest {
    val details = request.measurement.details
    if (!details.hasModelLine()) {
      throw RequiredFieldNotSetException("measurement.details.model_line")
    }
    val modelLine: ModelLineKey = details.modelLine
    val latestModelRollout: ModelRollout =
      StreamModelRollouts(
          StreamModelRolloutsRequestKt.filter {
            externalModelProviderId = modelLine.externalModelProviderId
            externalModelSuiteId = modelLine.externalModelSuiteId
            externalModelLineId = modelLine.externalModelLineId
          }
        )
        .execute(transactionContext)
        .map { it.modelRollout }
        .toList()
        .maxWithOrNull(MODEL_ROLLOUT_RECENCY_COMPARATOR)
        ?: throw ModelRolloutNotFoundException(
          ExternalId(modelLine.externalModelProviderId),
          ExternalId(modelLine.externalModelSuiteId),
          ExternalId(modelLine.externalModelLineId),
        ) {
          "ModelLine has no ModelRollout"
        }
    val modelRelease: ModelRelease =
      ModelReleaseReader()
        .readByExternalIds(
          transactionContext,
          ExternalId(latestModelRollout.externalModelReleaseId),
          ExternalId(latestModelRollout.externalModelSuiteId),
          ExternalId(latestModelRollout.externalModelProviderId),
        )
        ?.modelRelease
        ?: throw ModelReleaseNotFoundException(
          ExternalId(latestModelRollout.externalModelProviderId),
          ExternalId(latestModelRollout.externalModelSuiteId),
          ExternalId(latestModelRollout.externalModelReleaseId),
        )
    return request.copy {
      measurement =
        measurement.copy {
          this.details =
            this.details.copy {
              population = populationKey {
                externalDataProviderId = modelRelease.externalDataProviderId
                externalPopulationId = modelRelease.externalPopulationId
              }
            }
        }
    }
  }

  private fun TransactionScope.insertMeasurement(
    createMeasurementRequest: CreateMeasurementRequest,
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    externalMeasurementId: ExternalId,
    externalComputationId: ExternalId?,
    initialMeasurementState: Measurement.State,
    measurementConsumerCertificateId: InternalId,
  ) {
    transactionContext.bufferInsertMutation("Measurements") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("ExternalMeasurementId" to externalMeasurementId)
      if (externalComputationId != null) {
        set("ExternalComputationId" to externalComputationId)
      }
      if (createMeasurementRequest.requestId.isNotEmpty()) {
        set("CreateRequestId" to createMeasurementRequest.requestId)
      }
      if (createMeasurementRequest.measurement.providedMeasurementId.isNotEmpty()) {
        set("ProvidedMeasurementId" to createMeasurementRequest.measurement.providedMeasurementId)
      }
      set("CertificateId" to measurementConsumerCertificateId)
      set("State").toInt64(initialMeasurementState)
      set("MeasurementDetails").to(createMeasurementRequest.measurement.details)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }
  }

  private fun TransactionScope.insertComputationParticipant(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    duchyId: InternalId,
  ) {
    val participantDetails = ComputationParticipantDetails.getDefaultInstance()
    transactionContext.bufferInsertMutation("ComputationParticipants") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("DuchyId" to duchyId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("State").toInt64(ComputationParticipant.State.CREATED)
      set("ParticipantDetails").to(participantDetails)
    }
  }

  private fun TransactionScope.insertRequisitions(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    dataProvidersMap: Map<Long, Measurement.DataProviderValue>,
    dataProvideReaderResultsMap: Map<Long, DataProviderReader.Result>,
    initialRequisitionState: Requisition.State,
    fulfillingDuchies: List<DuchyIds.Entry> = emptyList(),
  ) {
    for ((externalDataProviderId, dataProviderValue) in dataProvidersMap) {
      val dataProviderReaderResult = dataProvideReaderResultsMap.getValue(externalDataProviderId)
      insertRequisition(
        measurementConsumerId = measurementConsumerId,
        measurementId = measurementId,
        dataProviderId = InternalId(dataProviderReaderResult.dataProviderId),
        details = dataProviderValue.toRequisitionDetails(),
        dataProviderCertificateId = InternalId(dataProviderReaderResult.certificateId),
        initialRequisitionState = initialRequisitionState,
        fulfillingDuchies = fulfillingDuchies,
      )
    }
  }

  private fun TransactionScope.insertMultimeasurementRequisitions(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    vectors: List<Measurement.MultimeasurementVector>,
    dataProvideReaderResultsMap: Map<Long, DataProviderReader.Result>,
    fulfillingDuchy: DuchyIds.Entry,
  ) {
    for (vector in vectors) {
      val dataProviderReaderResult =
        dataProvideReaderResultsMap.getValue(vector.externalDataProviderId)
      val details = requisitionDetails {
        vectorIndex = vector.index
        requisitionSpec = vector.requisitionSpec
      }
      insertRequisition(
        measurementConsumerId = measurementConsumerId,
        measurementId = measurementId,
        dataProviderId = InternalId(dataProviderReaderResult.dataProviderId),
        details = details,
        // Nothing in a multimeasurement Requisition is signed or encrypted for the DataProvider, so
        // this only records the DataProvider's current Certificate.
        dataProviderCertificateId = InternalId(dataProviderReaderResult.certificateId),
        initialRequisitionState = Requisition.State.PENDING_PARAMS,
        fulfillingDuchies = listOf(fulfillingDuchy),
      )
    }
  }

  private fun TransactionScope.insertRequisition(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    dataProviderId: InternalId,
    details: RequisitionDetails,
    dataProviderCertificateId: InternalId,
    initialRequisitionState: Requisition.State,
    fulfillingDuchies: List<DuchyIds.Entry> = emptyList(),
  ) {
    val requisitionId = idGenerator.generateInternalId()
    val externalRequisitionId = idGenerator.generateExternalId()
    val fulfillingDuchyId =
      if (fulfillingDuchies.isNotEmpty()) {
        // Requisitions for the same measurement might go to different duchies.
        val index = (externalRequisitionId.value % fulfillingDuchies.size).toInt()
        fulfillingDuchies[index].internalDuchyId
      } else {
        null
      }

    transactionContext.bufferInsertMutation("Requisitions") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("MeasurementId" to measurementId)
      set("RequisitionId" to requisitionId)
      set("DataProviderId" to dataProviderId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("ExternalRequisitionId" to externalRequisitionId)
      set("DataProviderCertificateId" to dataProviderCertificateId)
      set("State").toInt64(initialRequisitionState)
      fulfillingDuchyId?.let { set("FulfillingDuchyId" to it) }
      if (details.hasVectorIndex()) {
        set("VectorIndex" to details.vectorIndex.toLong())
      }
      set("RequisitionDetails").to(details)
    }
  }

  private suspend fun TransactionScope.findExistingMeasurements(
    createMeasurementRequests: List<CreateMeasurementRequest>,
    measurementConsumerId: InternalId,
  ): Map<String?, Measurement> {
    val params =
      object {
        val MEASUREMENT_CONSUMER_ID = "measurementConsumerId"
        val CREATE_REQUEST_ID = "createRequestId"
      }
    val whereClause =
      """
      WHERE MeasurementConsumerId = @${params.MEASUREMENT_CONSUMER_ID}
        AND CreateRequestId IS NOT NULL
        AND CreateRequestId IN UNNEST(@${params.CREATE_REQUEST_ID})
      """
        .trimIndent()

    val requestIds = createMeasurementRequests.map { it.requestId }
    return buildMap {
      if (requestIds.isNotEmpty()) {
        MeasurementReader(Measurement.View.DEFAULT, MeasurementReader.Index.CREATE_REQUEST_ID)
          .fillStatementBuilder {
            appendClause(whereClause)
            bind(params.MEASUREMENT_CONSUMER_ID to measurementConsumerId)
            bind(params.CREATE_REQUEST_ID).toStringArray(requestIds)
          }
          .execute(transactionContext)
          .collect {
            if (it.createRequestId != null) {
              put(it.createRequestId, it.measurement)
            }
          }
      }
    }
  }

  override fun ResultScope<List<Measurement>>.buildResult(): List<Measurement> {
    val measurements: List<Measurement> = checkNotNull(transactionResult)
    return measurements.map {
      if (it.hasCreateTime()) {
        // Existing measurement was returned instead of inserting new one, so keep its timestamps.
        it
      } else {
        it.copy {
          createTime = commitTimestamp.toProto()
          updateTime = commitTimestamp.toProto()
          etag = ETags.computeETag(commitTimestamp)
        }
      }
    }
  }
}

/** Orders ModelRollouts by recency, as the Population DataProvider resolves a model line. */
private val MODEL_ROLLOUT_RECENCY_COMPARATOR: Comparator<ModelRollout> =
  compareBy({ it.rolloutPeriodStartTime.toInstant() }, { it.createTime.toInstant() })

private suspend fun TransactionScope.readMeasurementConsumerId(
  externalMeasurementConsumerId: ExternalId
): InternalId {
  val column = "MeasurementConsumerId"
  return transactionContext
    .readRowUsingIndex(
      "MeasurementConsumers",
      "MeasurementConsumersByExternalId",
      Key.of(externalMeasurementConsumerId.value),
      column,
    )
    ?.let { struct -> InternalId(struct.getLong(column)) }
    ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId) {
      "MeasurementConsumer with external ID $externalMeasurementConsumerId not found"
    }
}

/**
 * Returns the internal Certificate Id if the revocation state has not been set and the current time
 * is inside the valid time period.
 *
 * Throws a [ErrorCode.CERTIFICATE_IS_INVALID] otherwise.
 */
private fun validateCertificate(certificateResult: CertificateReader.Result): InternalId {
  if (!certificateResult.isValid) {
    throw CertificateIsInvalidException()
  }

  return certificateResult.certificateId
}

private fun Measurement.DataProviderValue.toRequisitionDetails(): RequisitionDetails {
  val source = this
  return requisitionDetails {
    dataProviderPublicKey = source.dataProviderPublicKey
    encryptedRequisitionSpec = source.encryptedRequisitionSpec
    nonceHash = source.nonceHash

    // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting these
    // fields.
    dataProviderPublicKeySignature = source.dataProviderPublicKeySignature
    dataProviderPublicKeySignatureAlgorithmOid = source.dataProviderPublicKeySignatureAlgorithmOid
  }
}
