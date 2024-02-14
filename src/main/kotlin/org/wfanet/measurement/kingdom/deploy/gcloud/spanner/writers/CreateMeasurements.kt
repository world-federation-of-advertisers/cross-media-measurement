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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.CreateMeasurementRequest
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotCapableException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotActiveException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamDataProviders
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.CertificateReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementReader.Companion.getEtag
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SpannerWriter.TransactionScope

/**
 * Create measurements in the database.
 *
 * Throws one of the following subclasses of [KingdomInternalException] on [execute]:
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [DataProviderNotFoundException] DataProvider not found
 * * [DataProviderNotCapableException] DataProvider not capable of fulfilling Requisitions for the
 *   Measurement
 * * [MeasurementConsumerCertificateNotFoundException] MeasurementConsumer's Certificate not found
 * * [CertificateIsInvalidException] Certificate is invalid
 * * [DuchyNotActiveException] One or more required duchies were inactive at measurement creation
 *   time
 */
class CreateMeasurements(private val requests: List<CreateMeasurementRequest>) :
  SpannerWriter<List<Measurement>, List<Measurement>>() {

  /** [TransactionScope] for this [SpannerWriter]. */
  private class CreateTransactionScope(
    existing: TransactionScope,
    val measurementConsumerId: InternalId,
    val dataProvidersByExternalId: Map<ExternalId, DataProviderReader.Result>,
  ) : TransactionScope(existing.txn, existing.idGenerator)

  override suspend fun TransactionScope.runTransaction(): List<Measurement> {
    val measurementConsumerId: InternalId =
      readMeasurementConsumerId(
        ExternalId(requests.first().measurement.externalMeasurementConsumerId)
      )

    val existingMeasurements: Map<CreateMeasurementRequest, MeasurementReader.Result> =
      requests
        .mapNotNull { request ->
          findExistingMeasurement(request, measurementConsumerId)?.let { result ->
            request to result
          }
        }
        .toMap()
    val dataProvidersByExternalId: Map<ExternalId, DataProviderReader.Result> =
      readDataProviders(requests.filter { it !in existingMeasurements.keys })
    val scope = CreateTransactionScope(this, measurementConsumerId, dataProvidersByExternalId)

    return requests.map { existingMeasurements[it]?.measurement ?: scope.createMeasurement(it) }
  }

  /**
   * Reads the [DataProviderReader.Result]s for all of the DataProviders referenced by [requests].
   *
   * @throws DataProviderNotFoundException if any of the referenced DataProviders cannot be found
   */
  private suspend fun TransactionScope.readDataProviders(
    requests: Iterable<CreateMeasurementRequest>
  ): Map<ExternalId, DataProviderReader.Result> {
    val externalDataProviderIds: Set<ExternalId> =
      requests.flatMap { it.measurement.dataProvidersMap.keys.map(::ExternalId) }.toSet()
    val dataProvidersByExternalId: Map<ExternalId, DataProviderReader.Result> =
      StreamDataProviders(externalDataProviderIds)
        .execute(transactionContext)
        .toList()
        .associateBy { ExternalId(it.dataProvider.externalDataProviderId) }

    // Confirm that the query returned entries for all specified external DataProvider IDs.
    if (dataProvidersByExternalId.size < externalDataProviderIds.size) {
      for (externalDataProviderId in externalDataProviderIds) {
        if (externalDataProviderId !in dataProvidersByExternalId.keys) {
          throw DataProviderNotFoundException(externalDataProviderId)
        }
      }
    }

    return dataProvidersByExternalId
  }

  private suspend fun CreateTransactionScope.createMeasurement(
    request: CreateMeasurementRequest
  ): Measurement {
    // TODO(@SanjayVas): Check for DataProvider capabilities that are always required once we have
    // any.

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
    return when (request.measurement.details.protocolConfig.protocolCase) {
      ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2,
      ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2,
      ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
        createComputedMeasurement(request)
      }
      ProtocolConfig.ProtocolCase.DIRECT -> createDirectMeasurement(request)
      ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Protocol is not set.")
    }
  }

  private suspend fun CreateTransactionScope.createComputedMeasurement(
    request: CreateMeasurementRequest
  ): Measurement {
    val initialMeasurementState = Measurement.State.PENDING_REQUISITION_PARAMS
    val dataProviderValuesByExternalId: Map<ExternalId, Measurement.DataProviderValue> =
      request.measurement.dataProvidersMap.mapKeys { ExternalId(it.key) }
    val filteredDataProviders: Map<ExternalId, DataProviderReader.Result> =
      dataProvidersByExternalId.filterKeys { it in dataProviderValuesByExternalId.keys }

    if (request.measurement.details.protocolConfig.hasHonestMajorityShareShuffle()) {
      val incapableDataProvider: Map.Entry<ExternalId, DataProviderReader.Result>? =
        filteredDataProviders.entries.firstOrNull { (_, result) ->
          !result.dataProvider.details.capabilities.honestMajorityShareShuffleSupported
        }
      if (incapableDataProvider != null) {
        throw DataProviderNotCapableException(
          incapableDataProvider.key,
          "DataProvider with external ID ${incapableDataProvider.key} does not support the " +
            "Honest Majority Share Shuffle protocol",
        )
      }
    }

    val requiredExternalDuchyIds =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
      when (request.measurement.details.protocolConfig.protocolCase) {
        ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 -> Llv2ProtocolConfig.requiredExternalDuchyIds
        ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
          RoLlv2ProtocolConfig.requiredExternalDuchyIds
        ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
          HmssProtocolConfig.requiredExternalDuchyIds
        ProtocolConfig.ProtocolCase.DIRECT,
        ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Invalid protocol.")
      }
    val requiredDuchyIds =
      requiredExternalDuchyIds +
        readDataProviderRequiredDuchies(
          request.measurement.dataProvidersMap.keys.map { ExternalId(it) }.toSet()
        )
    val now = Clock.systemUTC().instant()
    val requiredDuchyEntries = DuchyIds.entries.filter { it.externalDuchyId in requiredDuchyIds }
    requiredDuchyEntries.forEach {
      if (!it.isActive(now)) {
        throw DuchyNotActiveException(it.externalDuchyId)
      }
    }
    val minimumNumberOfRequiredDuchies =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
      when (request.measurement.details.protocolConfig.protocolCase) {
        ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 ->
          Llv2ProtocolConfig.minimumNumberOfRequiredDuchies
        ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
          RoLlv2ProtocolConfig.minimumNumberOfRequiredDuchies
        ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
          HmssProtocolConfig.requiredExternalDuchyIds.size
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
      request,
      measurementId,
      externalMeasurementId,
      externalComputationId,
      initialMeasurementState,
    )

    includedDuchyEntries.forEach { entry ->
      insertComputationParticipant(
        measurementConsumerId,
        measurementId,
        InternalId(entry.internalDuchyId),
      )
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
    when (request.measurement.details.protocolConfig.protocolCase) {
      ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2,
      ProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
        // For each EDP, insert a Requisition.
        insertRequisitions(
          measurementId,
          request.measurement.dataProvidersMap,
          Requisition.State.PENDING_PARAMS,
        )
      }
      ProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
        insertRequisitions(
          measurementId,
          request.measurement.dataProvidersMap,
          Requisition.State.PENDING_PARAMS,
          includedDuchyEntries.drop(1),
        )
      }
      ProtocolConfig.ProtocolCase.DIRECT,
      ProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Invalid protocol,")
    }

    return request.measurement.copy {
      this.externalMeasurementId = externalMeasurementId.value
      this.externalComputationId = externalComputationId.value
      state = initialMeasurementState
    }
  }

  private suspend fun CreateTransactionScope.createDirectMeasurement(
    createMeasurementRequest: CreateMeasurementRequest
  ): Measurement {
    val initialMeasurementState = Measurement.State.PENDING_REQUISITION_FULFILLMENT

    val measurementId: InternalId = idGenerator.generateInternalId()
    val externalMeasurementId: ExternalId = idGenerator.generateExternalId()
    insertMeasurement(
      createMeasurementRequest,
      measurementId,
      externalMeasurementId,
      null,
      initialMeasurementState,
    )

    // Insert into Requisitions for each EDP
    insertRequisitions(
      measurementId,
      createMeasurementRequest.measurement.dataProvidersMap,
      Requisition.State.UNFULFILLED,
    )

    return createMeasurementRequest.measurement.copy {
      this.externalMeasurementId = externalMeasurementId.value
      state = initialMeasurementState
    }
  }

  private suspend fun CreateTransactionScope.insertMeasurement(
    createMeasurementRequest: CreateMeasurementRequest,
    measurementId: InternalId,
    externalMeasurementId: ExternalId,
    externalComputationId: ExternalId?,
    initialMeasurementState: Measurement.State,
  ) {
    val externalMeasurementConsumerId =
      ExternalId(createMeasurementRequest.measurement.externalMeasurementConsumerCertificateId)
    val reader =
      CertificateReader(CertificateReader.ParentType.MEASUREMENT_CONSUMER)
        .bindWhereClause(measurementConsumerId, externalMeasurementConsumerId)
    val measurementConsumerCertificateId =
      reader.execute(transactionContext).singleOrNull()?.let { validateCertificate(it) }
        ?: throw MeasurementConsumerCertificateNotFoundException(
          externalMeasurementConsumerId,
          externalMeasurementId,
        )

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
      set("State" to initialMeasurementState)
      set("MeasurementDetails" to createMeasurementRequest.measurement.details)
      setJson("MeasurementDetailsJson" to createMeasurementRequest.measurement.details)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }
  }

  private fun TransactionScope.insertComputationParticipant(
    measurementConsumerId: InternalId,
    measurementId: InternalId,
    duchyId: InternalId,
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

  private suspend fun CreateTransactionScope.insertRequisitions(
    measurementId: InternalId,
    dataProvidersMap: Map<Long, Measurement.DataProviderValue>,
    initialRequisitionState: Requisition.State,
    fulfillingDuchies: List<DuchyIds.Entry> = emptyList(),
  ) {
    for ((externalDataProviderId, dataProviderValue) in dataProvidersMap) {
      insertRequisition(
        measurementId,
        ExternalId(externalDataProviderId),
        dataProviderValue,
        initialRequisitionState,
        fulfillingDuchies,
      )
    }
  }

  private suspend fun CreateTransactionScope.insertRequisition(
    measurementId: InternalId,
    externalDataProviderId: ExternalId,
    dataProviderValue: Measurement.DataProviderValue,
    initialRequisitionState: Requisition.State,
    fulfillingDuchies: List<DuchyIds.Entry> = emptyList(),
  ) {
    val dataProviderId: InternalId =
      dataProvidersByExternalId.getValue(externalDataProviderId).dataProviderId
    val reader =
      CertificateReader(CertificateReader.ParentType.DATA_PROVIDER)
        .bindWhereClause(
          dataProviderId,
          ExternalId(dataProviderValue.externalDataProviderCertificateId),
        )

    val dataProviderCertificateId =
      reader.execute(transactionContext).singleOrNull()?.let { validateCertificate(it) }
        ?: throw DataProviderCertificateNotFoundException(
          externalDataProviderId,
          ExternalId(dataProviderValue.externalDataProviderCertificateId),
        )

    val requisitionId = idGenerator.generateInternalId()
    val externalRequisitionId = idGenerator.generateExternalId()
    val details: Requisition.Details =
      RequisitionKt.details {
        dataProviderPublicKey = dataProviderValue.dataProviderPublicKey
        encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
        nonceHash = dataProviderValue.nonceHash

        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting these
        // fields.
        dataProviderPublicKeySignature = dataProviderValue.dataProviderPublicKeySignature
        dataProviderPublicKeySignatureAlgorithmOid =
          dataProviderValue.dataProviderPublicKeySignatureAlgorithmOid
      }
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
      set("State" to initialRequisitionState)
      fulfillingDuchyId?.let { set("FulfillingDuchyId" to it) }
      set("RequisitionDetails" to details)
      setJson("RequisitionDetailsJson" to details)
    }
  }

  private suspend fun TransactionScope.findExistingMeasurement(
    createMeasurementRequest: CreateMeasurementRequest,
    measurementConsumerId: InternalId,
  ): MeasurementReader.Result? {
    val params =
      object {
        val MEASUREMENT_CONSUMER_ID = "measurementConsumerId"
        val CREATE_REQUEST_ID = "createRequestId"
      }
    val whereClause =
      """
      WHERE MeasurementConsumerId = @${params.MEASUREMENT_CONSUMER_ID}
        AND CreateRequestId = @${params.CREATE_REQUEST_ID}
      """
        .trimIndent()

    return MeasurementReader(Measurement.View.DEFAULT)
      .fillStatementBuilder {
        appendClause(whereClause)
        bind(params.MEASUREMENT_CONSUMER_ID to measurementConsumerId)
        bind(params.CREATE_REQUEST_ID to createMeasurementRequest.requestId)
      }
      .execute(transactionContext)
      .singleOrNullIfEmpty()
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
          etag = getEtag(commitTimestamp)
        }
      }
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
      column,
    )
    ?.let { struct -> InternalId(struct.getLong(column)) }
    ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId) {
      "MeasurementConsumer with external ID $externalMeasurementConsumerId not found"
    }
}

@OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
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
private fun validateCertificate(certificateResult: CertificateReader.Result): InternalId {
  if (!certificateResult.isValid) {
    throw CertificateIsInvalidException()
  }

  return certificateResult.certificateId
}
