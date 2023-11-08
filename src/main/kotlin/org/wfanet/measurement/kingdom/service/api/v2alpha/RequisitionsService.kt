/*
 * Copyright 2020 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.any
import com.google.protobuf.kotlin.unpack
import io.grpc.Status
import io.grpc.StatusException
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.ListRequisitionsPageToken
import org.wfanet.measurement.api.v2alpha.ListRequisitionsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.DuchyEntry
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.Requisition.State
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionParentKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsPageToken
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.directRequisitionParams
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.Requisition.DuchyValue
import org.wfanet.measurement.internal.kingdom.Requisition.Refusal as InternalRefusal
import org.wfanet.measurement.internal.kingdom.Requisition.State as InternalState
import org.wfanet.measurement.internal.kingdom.RequisitionKt as InternalRequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class RequisitionsService(
  private val internalRequisitionStub: RequisitionsCoroutineStub,
) : RequisitionsCoroutineImplBase() {

  private enum class Permission {
    LIST,
    REFUSE,
    FULFILL;

    fun deniedStatus(name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)."
      )
    }
  }

  override suspend fun listRequisitions(
    request: ListRequisitionsRequest
  ): ListRequisitionsResponse {
    fun permissionDeniedStatus() = Permission.LIST.deniedStatus("${request.parent}/requisitions")

    val parentKey: RequisitionParentKey =
      DataProviderKey.fromName(request.parent)
        ?: MeasurementKey.fromName(request.parent)
        ?: throw Status.INVALID_ARGUMENT.withDescription("parent is invalid").asRuntimeException()

    val principal: MeasurementPrincipal = principalFromCurrentContext
    when (parentKey) {
      is DataProviderKey ->
        if (parentKey != principal.resourceKey) {
          throw permissionDeniedStatus().asRuntimeException()
        }
      is MeasurementKey ->
        if (parentKey.parentKey != principal.resourceKey) {
          throw permissionDeniedStatus().asRuntimeException()
        }
    }

    val pageSize: Int =
      when {
        request.pageSize < 0 ->
          throw Status.INVALID_ARGUMENT.withDescription("page_size is negative")
            .asRuntimeException()
        request.pageSize == 0 -> DEFAULT_PAGE_SIZE
        else -> request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }
    val pageToken: ListRequisitionsPageToken? =
      if (request.pageToken.isEmpty()) null
      else ListRequisitionsPageToken.parseFrom(request.pageToken.base64UrlDecode())

    val internalRequest =
      buildInternalStreamRequisitionsRequest(request.filter, parentKey, pageSize, pageToken)
    val internalRequisitions: List<InternalRequisition> =
      try {
        internalRequisitionStub.streamRequisitions(internalRequest).toList()
      } catch (ex: StatusException) {
        throw when (ex.status.code) {
            Status.Code.INVALID_ARGUMENT ->
              Status.INVALID_ARGUMENT.withDescription("Required field unspecified or invalid")
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            else -> Status.UNKNOWN
          }
          .withCause(ex)
          .asRuntimeException()
      }

    if (internalRequisitions.isEmpty()) {
      return ListRequisitionsResponse.getDefaultInstance()
    }

    return listRequisitionsResponse {
      requisitions +=
        internalRequisitions
          .subList(0, min(internalRequisitions.size, pageSize))
          .map(InternalRequisition::toRequisition)

      if (internalRequisitions.size > pageSize) {
        nextPageToken =
          buildNextPageToken(request.filter, internalRequest.filter, internalRequisitions)
            .toByteString()
            .base64UrlEncode()
      }
    }
  }

  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    val key: CanonicalRequisitionKey =
      grpcRequireNotNull(CanonicalRequisitionKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }
    grpcRequire(request.refusal.justification != Refusal.Justification.JUSTIFICATION_UNSPECIFIED) {
      "Refusal details must be present"
    }

    val authenticatedPrincipal = principalFromCurrentContext
    if (key.parentKey != authenticatedPrincipal.resourceKey) {
      throw Permission.REFUSE.deniedStatus(request.name).asRuntimeException()
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

    val result =
      try {
        internalRequisitionStub.refuseRequisition(refuseRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.INVALID_ARGUMENT ->
            failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid." }
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "Requisition not found." }
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { "Requisition or Measurement state illegal." }
          Status.Code.DEADLINE_EXCEEDED -> throw ex.status.withCause(ex).asRuntimeException()
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    return result.toRequisition()
  }

  override suspend fun fulfillDirectRequisition(
    request: FulfillDirectRequisitionRequest
  ): FulfillDirectRequisitionResponse {
    val key =
      grpcRequireNotNull(CanonicalRequisitionKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    grpcRequire(request.nonce != 0L) { "nonce unspecified" }
    val encryptedResultCiphertext =
      if (request.hasEncryptedResult()) {
        grpcRequire(
          request.encryptedResult.typeUrl ==
            ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
        ) {
          "encrypted_result must contain an encrypted SignedMessage"
        }
        request.encryptedResult.ciphertext
      } else {
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop reading this
        // field.
        grpcRequire(!request.encryptedResultCiphertext.isEmpty) {
          "Neither encrypted_result nor encrypted_result_ciphertext specified"
        }
        request.encryptedResultCiphertext
      }

    val authenticatedPrincipal = principalFromCurrentContext
    if (key.parentKey != authenticatedPrincipal.resourceKey) {
      throw Permission.FULFILL.deniedStatus(request.name).asRuntimeException()
    }

    val fulfillRequest = fulfillRequisitionRequest {
      externalRequisitionId = apiIdToExternalId(key.requisitionId)
      nonce = request.nonce
      directParams = directRequisitionParams {
        externalDataProviderId = apiIdToExternalId(key.dataProviderId)
        encryptedData = encryptedResultCiphertext
        if (request.certificate.isNotEmpty()) {
          val dataProviderCertificateKey =
            DataProviderCertificateKey.fromName(request.certificate)
              ?: throw Status.INVALID_ARGUMENT.withDescription("Invalid result certificate")
                .asRuntimeException()
          externalCertificateId = apiIdToExternalId(dataProviderCertificateKey.certificateId)
        }
        apiVersion = Version.V2_ALPHA.string
      }
    }
    try {
      internalRequisitionStub.fulfillRequisition(fulfillRequest)
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("Requisition not found")
          Status.Code.INVALID_ARGUMENT ->
            Status.INVALID_ARGUMENT.withDescription("Required field unspecified or invalid")
          Status.Code.FAILED_PRECONDITION ->
            Status.FAILED_PRECONDITION.withDescription(
              "Requisition or Measurement state illegal, or Duchy not found"
            )
          else -> Status.UNKNOWN
        }
        .withCause(e)
        .asRuntimeException()
    }

    return fulfillDirectRequisitionResponse { state = State.FULFILLED }
  }
}

/** Converts an internal [Requisition] to a public [Requisition]. */
private fun InternalRequisition.toRequisition(): Requisition {
  val requisitionKey =
    CanonicalRequisitionKey(
      externalIdToApiId(externalDataProviderId),
      externalIdToApiId(externalRequisitionId)
    )
  val measurementApiVersion = Version.fromString(parentMeasurement.apiVersion)
  val packedMeasurementSpec = any {
    value = parentMeasurement.measurementSpec
    typeUrl =
      when (measurementApiVersion) {
        Version.V2_ALPHA -> ProtoReflection.getTypeUrl(MeasurementSpec.getDescriptor())
      }
  }
  val measurementSpec: MeasurementSpec = packedMeasurementSpec.unpack()

  return requisition {
    name = requisitionKey.toName()

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
    this.measurementSpec = signedMessage {
      setMessage(packedMeasurementSpec)
      signature = parentMeasurement.measurementSpecSignature
      signatureAlgorithmOid = parentMeasurement.measurementSpecSignatureAlgorithmOid
    }

    protocolConfig =
      parentMeasurement.protocolConfig.toProtocolConfig(
        measurementSpec.measurementTypeCase,
        parentMeasurement.dataProvidersCount,
      )

    encryptedRequisitionSpec = encryptedMessage {
      ciphertext = details.encryptedRequisitionSpec
      typeUrl =
        when (measurementApiVersion) {
          Version.V2_ALPHA -> ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
        }
    }

    dataProviderCertificate =
      DataProviderCertificateKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(this@toRequisition.dataProviderCertificate.externalCertificateId)
        )
        .toName()
    dataProviderPublicKey = signedMessage {
      setMessage(
        any {
          value = details.dataProviderPublicKey
          typeUrl =
            when (measurementApiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
            }
        }
      )
      signature = details.dataProviderPublicKeySignature
      signatureAlgorithmOid = details.dataProviderPublicKeySignatureAlgorithmOid
    }
    nonce = details.nonce

    duchies += duchiesMap.entries.map { it.toDuchyEntry(measurementApiVersion) }

    state = this@toRequisition.state.toRequisitionState()
    if (state == State.REFUSED) {
      refusal = refusal {
        justification = details.refusal.justification.toRefusalJustification()
        message = details.refusal.message
      }
    }
    measurementState = this@toRequisition.parentMeasurement.state.toState()
  }
}

/** Converts an internal [InternalRefusal.Justification] to a public [Refusal.Justification]. */
private fun InternalRefusal.Justification.toRefusalJustification(): Refusal.Justification =
  when (this) {
    InternalRefusal.Justification.CONSENT_SIGNAL_INVALID ->
      Refusal.Justification.CONSENT_SIGNAL_INVALID
    InternalRefusal.Justification.SPECIFICATION_INVALID -> Refusal.Justification.SPEC_INVALID
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
    Refusal.Justification.SPEC_INVALID -> InternalRefusal.Justification.SPECIFICATION_INVALID
    Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET ->
      InternalRefusal.Justification.INSUFFICIENT_PRIVACY_BUDGET
    Refusal.Justification.UNFULFILLABLE -> InternalRefusal.Justification.UNFULFILLABLE
    Refusal.Justification.DECLINED -> InternalRefusal.Justification.DECLINED
    Refusal.Justification.JUSTIFICATION_UNSPECIFIED,
    Refusal.Justification.UNRECOGNIZED -> InternalRefusal.Justification.JUSTIFICATION_UNSPECIFIED
  }

/** Converts an internal [InternalState] to a public [State]. */
private fun InternalState.toRequisitionState(): State =
  when (this) {
    InternalState.PENDING_PARAMS,
    InternalState.UNFULFILLED -> State.UNFULFILLED
    InternalState.FULFILLED -> State.FULFILLED
    InternalState.REFUSED -> State.REFUSED
    InternalState.STATE_UNSPECIFIED,
    InternalState.UNRECOGNIZED -> State.STATE_UNSPECIFIED
  }

/** Converts a public [State] to an internal [InternalState]. */
private fun State.toInternal(): InternalState =
  when (this) {
    State.UNFULFILLED -> InternalState.UNFULFILLED
    State.FULFILLED -> InternalState.FULFILLED
    State.REFUSED -> InternalState.REFUSED
    State.STATE_UNSPECIFIED,
    State.UNRECOGNIZED -> InternalState.STATE_UNSPECIFIED
  }

/** Converts an internal [DuchyValue] to a public [DuchyEntry.Value]. */
private fun DuchyValue.toDuchyEntryValue(
  externalDuchyId: String,
  apiVersion: Version
): DuchyEntry.Value {
  val duchyCertificateKey =
    DuchyCertificateKey(externalDuchyId, externalIdToApiId(externalDuchyCertificateId))

  val source = this
  return value {
    duchyCertificate = duchyCertificateKey.toName()
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Annotated with @NotNull.
    when (source.protocolCase) {
      DuchyValue.ProtocolCase.LIQUID_LEGIONS_V2 -> {
        liquidLegionsV2 = source.liquidLegionsV2.toDuchyEntryLlV2(apiVersion)
      }
      DuchyValue.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
        reachOnlyLiquidLegionsV2 = source.reachOnlyLiquidLegionsV2.toDuchyEntryLlV2(apiVersion)
      }
      DuchyValue.ProtocolCase.PROTOCOL_NOT_SET -> error("protocol not set")
    }
  }
}

private fun ComputationParticipant.LiquidLegionsV2Details.toDuchyEntryLlV2(
  apiVersion: Version
): DuchyEntry.LiquidLegionsV2 {
  val source = this
  return liquidLegionsV2 {
    elGamalPublicKey = signedMessage {
      setMessage(
        any {
          value = source.elGamalPublicKey
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(ElGamalPublicKey.getDescriptor())
            }
        }
      )
      signature = source.elGamalPublicKeySignature
      signatureAlgorithmOid = source.elGamalPublicKeySignatureAlgorithmOid
    }
  }
}

/** Converts an internal duchy map entry to a public [DuchyEntry]. */
private fun Map.Entry<String, DuchyValue>.toDuchyEntry(apiVersion: Version): DuchyEntry {
  val mapEntry = this
  return duchyEntry {
    key = mapEntry.key
    value = mapEntry.value.toDuchyEntryValue(mapEntry.key, apiVersion)
  }
}

private fun buildInternalStreamRequisitionsRequest(
  filter: ListRequisitionsRequest.Filter,
  parentKey: RequisitionParentKey,
  pageSize: Int,
  pageToken: ListRequisitionsPageToken?
): StreamRequisitionsRequest {
  val requestStates = filter.statesList
  return streamRequisitionsRequest {
    this.filter =
      StreamRequisitionsRequestKt.filter {
        when (parentKey) {
          is DataProviderKey -> {
            externalDataProviderId = ApiId(parentKey.dataProviderId).externalId.value
          }
          is MeasurementKey -> {
            externalMeasurementConsumerId = ApiId(parentKey.measurementConsumerId).externalId.value
            externalMeasurementId = ApiId(parentKey.measurementId).externalId.value
          }
        }

        if (requestStates.isEmpty()) {
          // If no state filter set in public request, include all visible states.
          states += InternalState.UNFULFILLED
          states += InternalState.FULFILLED
          states += InternalState.REFUSED
        } else {
          states += requestStates.map { it.toInternal() }
        }

        measurementStates += filter.measurementStatesList.flatMap { it.toInternalState() }

        if (pageToken != null) {
          if (
            pageToken.externalDataProviderId != externalDataProviderId ||
              pageToken.externalMeasurementConsumerId != externalMeasurementConsumerId ||
              pageToken.externalMeasurementId != externalMeasurementId ||
              pageToken.statesList != filter.statesList ||
              pageToken.measurementStatesList != filter.measurementStatesList
          ) {
            throw Status.INVALID_ARGUMENT.withDescription(
                "Arguments other than page_size must remain the same for subsequent page requests"
              )
              .asRuntimeException()
          }
          externalDataProviderIdAfter = pageToken.lastRequisition.externalDataProviderId
          externalRequisitionIdAfter = pageToken.lastRequisition.externalRequisitionId
        }
      }
    // Fetch one extra to determine if we need to set next_page_token in response.
    limit = pageSize + 1
  }
}

private fun buildNextPageToken(
  filter: ListRequisitionsRequest.Filter,
  internalFilter: StreamRequisitionsRequest.Filter,
  internalRequisitions: List<InternalRequisition>
): ListRequisitionsPageToken {
  return listRequisitionsPageToken {
    if (internalFilter.externalDataProviderId != 0L) {
      externalDataProviderId = internalFilter.externalDataProviderId
    }
    if (internalFilter.externalMeasurementConsumerId != 0L) {
      externalMeasurementConsumerId = internalFilter.externalMeasurementConsumerId
    }
    if (internalFilter.externalMeasurementId != 0L) {
      externalMeasurementId = internalFilter.externalMeasurementId
    }
    states += filter.statesList
    measurementStates += filter.measurementStatesList
    lastRequisition = previousPageEnd {
      externalDataProviderId =
        internalRequisitions[internalRequisitions.lastIndex - 1].externalDataProviderId
      externalRequisitionId =
        internalRequisitions[internalRequisitions.lastIndex - 1].externalRequisitionId
    }
  }
}
