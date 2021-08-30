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

import com.google.protobuf.Timestamp
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.DuchyEntry
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.Requisition.State
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.Requisition.DuchyValue
import org.wfanet.measurement.internal.kingdom.Requisition.Refusal as InternalRefusal
import org.wfanet.measurement.internal.kingdom.Requisition.State as InternalState
import org.wfanet.measurement.internal.kingdom.RequisitionKt as InternalRequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 100
private const val WILDCARD = "-"

class RequisitionsService(private val internalRequisitionStub: RequisitionsCoroutineStub) :
  RequisitionsCoroutineImplBase() {

  override suspend fun listRequisitions(
    request: ListRequisitionsRequest
  ): ListRequisitionsResponse {
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }
    val parentKey: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }
    grpcRequire(
      (request.filter.measurement.isNotBlank() && parentKey.dataProviderId == WILDCARD) ||
        parentKey.dataProviderId != WILDCARD
    ) { "Either parent data provider or measurement filter must be provided" }

    val pageSize =
      when {
        request.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
        request.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
        else -> request.pageSize
      }

    val streamRequest = streamRequisitionsRequest {
      limit = pageSize
      filter =
        StreamRequisitionsRequestKt.filter {
          if (request.pageToken.isNotBlank()) {
            updatedAfter = Timestamp.parseFrom(request.pageToken.base64UrlDecode())
          }
          if (request.filter.measurement.isNotBlank()) {
            val measurementKey: MeasurementKey =
              grpcRequireNotNull(MeasurementKey.fromName(request.filter.measurement)) {
                "Resource name invalid"
              }
            externalMeasurementConsumerId = apiIdToExternalId(measurementKey.measurementConsumerId)
          }
          if (parentKey.dataProviderId != WILDCARD) {
            externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
          }
          states +=
            request.filter.statesList.map { state ->
              state.toInternal().also { internalState ->
                grpcRequire(internalState != InternalState.STATE_UNSPECIFIED) { "State is invalid" }
              }
            }
        }
    }

    val results: List<InternalRequisition> =
      internalRequisitionStub.streamRequisitions(streamRequest).toList()

    if (results.isEmpty()) {
      return ListRequisitionsResponse.getDefaultInstance()
    }

    return listRequisitionsResponse {
      requisitions += results.map(InternalRequisition::toRequisition)
      if (results.size == pageSize) {
        nextPageToken = results.last().updateTime.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    val key: RequisitionKey =
      grpcRequireNotNull(RequisitionKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }
    grpcRequire(request.refusal.justification != Refusal.Justification.JUSTIFICATION_UNSPECIFIED) {
      "Refusal details must be present"
    }

    val refuseRequest = refuseRequisitionRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      externalRequisitionId = apiIdToExternalId(key.requisitionId)
      refusal =
        InternalRequisitionKt.refusal {
          justification = request.refusal.justification.toInternal()
          message = request.refusal.message
        }
    }

    val result = internalRequisitionStub.refuseRequisition(refuseRequest)

    return result.toRequisition()
  }
}

/** Converts an internal [Requisition] to a public [Requisition]. */
private fun InternalRequisition.toRequisition(): Requisition {
  check(Version.fromString(parentMeasurement.apiVersion) == Version.V2_ALPHA) {
    "Incompatible API version ${parentMeasurement.apiVersion}"
  }

  return requisition {
    name =
      RequisitionKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(externalRequisitionId)
        )
        .toName()

    measurement =
      MeasurementKey(
          externalIdToApiId(externalMeasurementConsumerId),
          externalIdToApiId(externalMeasurementId)
        )
        .toName()
    measurementConsumerCertificate =
      MeasurementConsumerCertificateKey(
          externalIdToApiId(externalMeasurementConsumerId),
          externalIdToApiId(parentMeasurement.externalMeasurementConsumerCertificateId)
        )
        .toName()
    measurementSpec =
      signedData {
        data = parentMeasurement.measurementSpec
        signature = parentMeasurement.measurementSpecSignature
      }
    protocolConfig = parentMeasurement.externalProtocolConfigId
    encryptedRequisitionSpec = details.encryptedRequisitionSpec

    dataProviderCertificate =
      DataProviderCertificateKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(this@toRequisition.externalDataProviderCertificateId)
        )
        .toName()
    dataProviderPublicKey =
      signedData {
        data = details.dataProviderPublicKey
        signature = details.dataProviderPublicKeySignature
      }
    dataProviderParticipationSignature = details.dataProviderParticipationSignature

    duchies += duchiesMap.entries.map(Map.Entry<String, DuchyValue>::toDuchyEntry)

    state = this@toRequisition.state.toRequisitionState()
    if (state == State.REFUSED) {
      refusal =
        refusal {
          justification = details.refusal.justification.toRefusalJustification()
          message = details.refusal.message
        }
    }
  }
}

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
    InternalRefusal.Justification.JUSTIFICATION_UNSPECIFIED,
    InternalRefusal.Justification.UNRECOGNIZED -> Refusal.Justification.JUSTIFICATION_UNSPECIFIED
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
    Refusal.Justification.JUSTIFICATION_UNSPECIFIED, Refusal.Justification.UNRECOGNIZED ->
      InternalRefusal.Justification.JUSTIFICATION_UNSPECIFIED
  }

/** Converts an internal [InternalState] to a public [State]. */
private fun InternalState.toRequisitionState(): State =
  when (this) {
    InternalState.UNFULFILLED -> State.UNFULFILLED
    InternalState.FULFILLED -> State.FULFILLED
    InternalState.REFUSED -> State.REFUSED
    InternalState.STATE_UNSPECIFIED, InternalState.UNRECOGNIZED -> State.STATE_UNSPECIFIED
  }

/** Converts a public [State] to an internal [InternalState]. */
private fun State.toInternal(): InternalState =
  when (this) {
    State.UNFULFILLED -> InternalState.UNFULFILLED
    State.FULFILLED -> InternalState.FULFILLED
    State.REFUSED -> InternalState.REFUSED
    State.STATE_UNSPECIFIED, State.UNRECOGNIZED -> InternalState.STATE_UNSPECIFIED
  }

/** Converts an internal [DuchyValue] to a public [DuchyEntry.Value]. */
private fun DuchyValue.toDuchyEntryValue(): DuchyEntry.Value {
  val value = this
  return value {
    duchyCertificate = externalIdToApiId(externalDuchyCertificateId)
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (value.protocolCase) {
      DuchyValue.ProtocolCase.LIQUID_LEGIONS_V2 ->
        liquidLegionsV2 =
          liquidLegionsV2 {
            elGamalPublicKey =
              signedData {
                data = value.liquidLegionsV2.elGamalPublicKey
                signature = value.liquidLegionsV2.elGamalPublicKeySignature
              }
          }
      DuchyValue.ProtocolCase.PROTOCOL_NOT_SET -> {}
    }
  }
}

/** Converts an internal duchy map entry to a public [DuchyEntry]. */
private fun Map.Entry<String, DuchyValue>.toDuchyEntry(): DuchyEntry {
  val mapEntry = this
  return duchyEntry {
    key = mapEntry.key
    value = mapEntry.value.toDuchyEntryValue()
  }
}
