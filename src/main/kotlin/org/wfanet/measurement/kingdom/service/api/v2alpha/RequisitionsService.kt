// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.DuchyEntry
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.PageToken
import org.wfanet.measurement.internal.kingdom.RefuseRequisitionRequest as InternalRefuseRequest
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.Requisition.DuchyValue
import org.wfanet.measurement.internal.kingdom.Requisition.Refusal as InternalRefusal
import org.wfanet.measurement.internal.kingdom.Requisition.State
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest

class RequisitionsService(private val internalRequisitionStub: RequisitionsCoroutineStub) :
  RequisitionsCoroutineImplBase() {

  override suspend fun listRequisitions(
    request: ListRequisitionsRequest
  ): ListRequisitionsResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    val pageSize =
      when {
        request.pageSize < 1 -> 1
        request.pageSize > 1000 -> 1000
        else -> request.pageSize
      }

    /**
     * StreamRequisitionsRequest filter depends on whether the page token is present or not. If it
     * is present, the filter in the request is ignored and the filter is retrieved from the page
     * token.
     */
    val streamRequest =
      StreamRequisitionsRequest.newBuilder()
        .apply {
          limit = pageSize
          if (request.pageToken.isNotBlank()) {
            val pageToken = PageToken.parseFrom(request.pageToken.base64UrlDecode())
            filterBuilder.apply {
              createdAfter = pageToken.createdAfter
              externalMeasurementConsumerId = pageToken.filter.externalMeasurementConsumerId
              externalDataProviderId = pageToken.filter.externalDataProviderId
              addAllStates(pageToken.filter.statesList)
            }
          } else {
            filterBuilder.apply {
              if (request.filter.measurement.isNotBlank()) {
                val key: MeasurementKey =
                  grpcRequireNotNull(MeasurementKey.fromName(request.filter.measurement)) {
                    "Resource name invalid"
                  }
                externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
              }
              if (request.parent.isNotBlank()) {
                val key: DataProviderKey =
                  grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
                    "Resource name invalid"
                  }
                externalDataProviderId = apiIdToExternalId(key.dataProviderId)
              }
              addAllStates(request.filter.statesList.map(Requisition.State::toInternal))
            }
          }
        }
        .build()

    val results: List<InternalRequisition> =
      internalRequisitionStub.streamRequisitions(streamRequest).toList()

    if (results.isEmpty()) {
      return ListRequisitionsResponse.getDefaultInstance()
    }

    return ListRequisitionsResponse.newBuilder()
      .addAllRequisitions(results.map(InternalRequisition::toRequisition))
      .setNextPageToken(
        buildPageToken {
          createdAfter = results.last().createTime
          filter = streamRequest.filter
        }
      )
      .build()
  }

  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    val key: RequisitionKey =
      grpcRequireNotNull(RequisitionKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }
    grpcRequire(request.refusal.justification != Refusal.Justification.JUSTIFICATION_UNSPECIFIED) {
      "Refusal details must be present"
    }

    val refuseRequest =
      InternalRefuseRequest.newBuilder()
        .apply {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          externalRequisitionId = apiIdToExternalId(key.requisitionId)
          refusalBuilder.apply {
            justification = request.refusal.justification.toInternal()
            message = request.refusal.message
          }
        }
        .build()

    val result = internalRequisitionStub.refuseRequisition(refuseRequest)

    return result.toRequisition()
  }
}

/** Converts an internal [Requisition] to a public [Requisition]. */
private fun InternalRequisition.toRequisition(): Requisition {
  check(Version.fromString(parentMeasurement.apiVersion) == Version.V2_ALPHA) {
    "Incompatible API version ${parentMeasurement.apiVersion}"
  }

  return buildRequisition {
    name = externalIdToApiId(externalRequisitionId)

    measurement = externalIdToApiId(externalMeasurementId)
    measurementConsumerCertificate =
      externalIdToApiId(parentMeasurement.externalMeasurementConsumerCertificateId)
    measurementSpec {
      data = parentMeasurement.measurementSpec
      signature = parentMeasurement.measurementSpecSignature
    }
    protocolConfig = parentMeasurement.externalProtocolConfigId
    encryptedRequisitionSpec = details.encryptedRequisitionSpec

    dataProviderCertificate =
      externalIdToApiId(this@toRequisition.externalDataProviderCertificateId)
    dataProviderPublicKey {
      data = details.dataProviderPublicKey
      signature = details.dataProviderPublicKeySignature
    }
    dataProviderParticipationSignature = details.dataProviderParticipationSignature

    addAllDuchies(duchiesMap.entries.map(MutableMap.MutableEntry<String, DuchyValue>::toDuchyEntry))

    state = this@toRequisition.state.toRequisitionState()
    buildRefusal {
      justification = details.refusal.justification.toRefusalJustification()
      message = details.refusal.message
    }
  }
}

internal inline fun buildRequisition(fill: (@Builder Requisition.Builder).() -> Unit) =
  Requisition.newBuilder().apply(fill).build()

internal inline fun Requisition.Builder.buildRefusal(fill: (@Builder Refusal.Builder).() -> Unit) =
  refusalBuilder.apply(fill)

internal inline fun Requisition.Builder.measurementSpec(
  fill: (@Builder SignedData.Builder).() -> Unit
) = measurementSpecBuilder.apply(fill)

internal inline fun Requisition.Builder.dataProviderPublicKey(
  fill: (@Builder SignedData.Builder).() -> Unit
) = dataProviderPublicKeyBuilder.apply(fill)

/** Converts an internal [InternalRefusal.Justification] to a public [Refusal.Justification]. */
private fun InternalRefusal.Justification.toRefusalJustification(): Refusal.Justification =
  when (this) {
    InternalRefusal.Justification.CONSENT_SIGNAL_INVALID ->
      Refusal.Justification.CONSENT_SIGNAL_INVALID
    InternalRefusal.Justification.SPECIFICATION_INVALID ->
      Refusal.Justification.SPECIFICATION_INVALID
    InternalRefusal.Justification.INSUFFICIENT_PRIVACY_BUDGET ->
      Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET
    InternalRefusal.Justification.UNFULFILLABLE -> Refusal.Justification.UNFULFILLABLE
    InternalRefusal.Justification.DECLINED -> Refusal.Justification.DECLINED
    else -> Refusal.Justification.JUSTIFICATION_UNSPECIFIED
  }

/** Converts a public [Refusal.Justification] to an internal [InternalRefusal.Justification]. */
private fun Refusal.Justification.toInternal(): InternalRefusal.Justification =
  when (this) {
    Refusal.Justification.CONSENT_SIGNAL_INVALID ->
      InternalRefusal.Justification.CONSENT_SIGNAL_INVALID
    Refusal.Justification.SPECIFICATION_INVALID ->
      InternalRefusal.Justification.SPECIFICATION_INVALID
    Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET ->
      InternalRefusal.Justification.INSUFFICIENT_PRIVACY_BUDGET
    Refusal.Justification.UNFULFILLABLE -> InternalRefusal.Justification.UNFULFILLABLE
    Refusal.Justification.DECLINED -> InternalRefusal.Justification.DECLINED
    else -> InternalRefusal.Justification.JUSTIFICATION_UNSPECIFIED
  }

/** Converts an internal [State] to a public [Requisition.State]. */
private fun State.toRequisitionState(): Requisition.State =
  when (this) {
    State.UNFULFILLED -> Requisition.State.UNFULFILLED
    State.FULFILLED -> Requisition.State.FULFILLED
    State.REFUSED -> Requisition.State.REFUSED
    else -> Requisition.State.STATE_UNSPECIFIED
  }

/** Converts a public [Requisition.State] to an internal [State]. */
private fun Requisition.State.toInternal(): State =
  when (this) {
    Requisition.State.UNFULFILLED -> State.UNFULFILLED
    Requisition.State.FULFILLED -> State.FULFILLED
    Requisition.State.REFUSED -> State.REFUSED
    else -> State.STATE_UNSPECIFIED
  }

internal inline fun buildDuchyEntryValue(fill: (@Builder DuchyEntry.Value.Builder).() -> Unit) =
  DuchyEntry.Value.newBuilder().apply(fill).build()

internal inline fun DuchyEntry.LiquidLegionsV2.Builder.elGamalPublicKey(
  fill: (@Builder SignedData.Builder).() -> Unit
) = elGamalPublicKeyBuilder.apply(fill)

internal inline fun DuchyEntry.Value.Builder.buildLiquidLegionsV2(
  fill: (@Builder DuchyEntry.LiquidLegionsV2.Builder).() -> Unit
) = liquidLegionsV2Builder.apply(fill).build()

/** Converts an internal [DuchyValue] to a public [DuchyEntry.Value]. */
private fun DuchyValue.toDuchyEntryValue(): DuchyEntry.Value {
  return buildDuchyEntryValue {
    duchyCertificate = externalIdToApiId(externalDuchyCertificateId)
    when (this@toDuchyEntryValue.protocolCase) {
      DuchyValue.ProtocolCase.LIQUID_LEGIONS_V2 ->
        buildLiquidLegionsV2 {
          elGamalPublicKey {
            data = this@toDuchyEntryValue.liquidLegionsV2.elGamalPublicKey
            signature = this@toDuchyEntryValue.liquidLegionsV2.elGamalPublicKeySignature
          }
        }
      else -> {}
    }
  }
}

internal inline fun buildDuchyEntry(fill: (@Builder DuchyEntry.Builder).() -> Unit) =
  DuchyEntry.newBuilder().apply(fill).build()

/** Converts an internal duchy map entry to a public [DuchyEntry]. */
private fun MutableMap.MutableEntry<String, DuchyValue>.toDuchyEntry(): DuchyEntry {
  return buildDuchyEntry {
    key = this@toDuchyEntry.key
    value = this@toDuchyEntry.value.toDuchyEntryValue()
  }
}

internal inline fun buildPageToken(fill: (@Builder PageToken.Builder).() -> Unit) =
  PageToken.newBuilder().apply(fill).build().toByteArray().base64UrlEncode()
