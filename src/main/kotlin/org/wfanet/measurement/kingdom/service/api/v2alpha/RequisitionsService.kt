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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
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
import org.wfanet.measurement.api.v2alpha.GetRequisitionRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsPageToken
import org.wfanet.measurement.api.v2alpha.ListRequisitionsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.DuchyEntry
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.Requisition.State
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt
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
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.directRequisitionParams
import org.wfanet.measurement.internal.kingdom.HonestMajorityShareShuffleParams
import org.wfanet.measurement.internal.kingdom.LiquidLegionsV2Params
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.Requisition.DuchyValue
import org.wfanet.measurement.internal.kingdom.Requisition.State as InternalState
import org.wfanet.measurement.internal.kingdom.RequisitionDetailsKt
import org.wfanet.measurement.internal.kingdom.RequisitionRefusal as InternalRefusal
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.getRequisitionRequest
import org.wfanet.measurement.internal.kingdom.refuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.requisitionRefusal as internalRequisitionRefusal
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest

private const val DEFAULT_PAGE_SIZE = 10
private const val MAX_PAGE_SIZE = 500

class RequisitionsService(
  private val internalRequisitionStub: RequisitionsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RequisitionsCoroutineImplBase(coroutineContext) {

  private enum class Permission {
    LIST,
    GET,
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

    if (request.parent.isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("parent not set").asRuntimeException()
    }
    val parentKey: RequisitionParentKey =
      DataProviderKey.fromName(request.parent)
        ?: MeasurementKey.fromName(request.parent)
        ?: throw Status.INVALID_ARGUMENT.withDescription("parent invalid").asRuntimeException()

    if (request.filter.measurementStatesList.isNotEmpty()) {
      if (
        // Avoid breaking known existing callers where the measurement state filter is redundant.
        !(request.filter.statesList == listOf(State.UNFULFILLED) &&
          request.filter.measurementStatesList ==
            listOf(Measurement.State.AWAITING_REQUISITION_FULFILLMENT))
      ) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "filter.measurement_states is no longer supported"
          )
          .asRuntimeException()
      }
    }

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
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
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

  override suspend fun getRequisition(request: GetRequisitionRequest): Requisition {
    val key: CanonicalRequisitionKey =
      grpcRequireNotNull(CanonicalRequisitionKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val authenticatedPrincipal = principalFromCurrentContext
    if (key.parentKey != authenticatedPrincipal.resourceKey) {
      throw Permission.GET.deniedStatus(request.name).asRuntimeException()
    }

    val getRequest = getRequisitionRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      externalRequisitionId = apiIdToExternalId(key.requisitionId)
    }

    val result =
      try {
        internalRequisitionStub.getRequisition(getRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          Status.Code.UNAVAILABLE -> Status.UNAVAILABLE
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return result.toRequisition()
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
      refusal = internalRequisitionRefusal {
        justification = request.refusal.justification.toInternal()
        message = request.refusal.message
      }
      etag = request.etag
    }

    val result =
      try {
        internalRequisitionStub.refuseRequisition(refuseRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
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
      if (request.hasFulfillmentContext()) {
        fulfillmentContext =
          RequisitionDetailsKt.fulfillmentContext {
            buildLabel = request.fulfillmentContext.buildLabel
            warnings += request.fulfillmentContext.warningsList
          }
      }
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

      etag = request.etag
    }
    try {
      internalRequisitionStub.fulfillRequisition(fulfillRequest)
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }

    return fulfillDirectRequisitionResponse { state = State.FULFILLED }
  }
}

/** Converts an internal [Requisition] to a public [Requisition]. */
private fun InternalRequisition.toRequisition(): Requisition {
  val requisitionKey =
    CanonicalRequisitionKey(
      externalIdToApiId(externalDataProviderId),
      externalIdToApiId(externalRequisitionId),
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
  val dataProviderPublicKey = any {
    value = details.dataProviderPublicKey
    typeUrl =
      when (measurementApiVersion) {
        Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
      }
  }

  return requisition {
    name = requisitionKey.toName()

    measurement =
      MeasurementKey(
          externalIdToApiId(externalMeasurementConsumerId),
          externalIdToApiId(externalMeasurementId),
        )
        .toName()
    measurementConsumerCertificate =
      MeasurementConsumerCertificateKey(
          externalIdToApiId(externalMeasurementConsumerId),
          externalIdToApiId(parentMeasurement.externalMeasurementConsumerCertificateId),
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
    // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this field.
    encryptedRequisitionSpecCiphertext = details.encryptedRequisitionSpec

    dataProviderCertificate =
      DataProviderCertificateKey(
          externalIdToApiId(externalDataProviderId),
          externalIdToApiId(this@toRequisition.dataProviderCertificate.externalCertificateId),
        )
        .toName()
    this.dataProviderPublicKey = dataProviderPublicKey
    nonce = details.nonce

    // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this field.
    signedDataProviderPublicKey = signedMessage {
      setMessage(dataProviderPublicKey)
      signature = details.dataProviderPublicKeySignature
      signatureAlgorithmOid = details.dataProviderPublicKeySignatureAlgorithmOid
    }

    duchies +=
      duchiesMap.entries
        .filter { it.value.availableForFulfillment() }
        .map {
          it.toDuchyEntry(measurementApiVersion, this@toRequisition.externalFulfillingDuchyId)
        }

    state = this@toRequisition.state.toRequisitionState()
    if (state == State.REFUSED) {
      refusal = refusal {
        justification = details.refusal.justification.toRefusalJustification()
        message = details.refusal.message
      }
    }
    if (details.hasFulfillmentContext()) {
      fulfillmentContext =
        RequisitionKt.fulfillmentContext {
          buildLabel = details.fulfillmentContext.buildLabel
          warnings += details.fulfillmentContext.warningsList
        }
    }
    measurementState = this@toRequisition.parentMeasurement.state.toState()
    updateTime = this@toRequisition.updateTime
    etag = this@toRequisition.etag
  }
}

private fun DuchyValue.availableForFulfillment(): Boolean {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
  return when (protocolCase) {
    DuchyValue.ProtocolCase.LIQUID_LEGIONS_V2,
    DuchyValue.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> true
    DuchyValue.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
      !honestMajorityShareShuffle.tinkPublicKey.isEmpty
    }
    DuchyValue.ProtocolCase.PROTOCOL_NOT_SET -> false
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

/** Converts a public [State] to an internal [InternalState]. */
private fun State.toInternal(): InternalState =
  when (this) {
    State.UNFULFILLED -> InternalState.UNFULFILLED
    State.FULFILLED -> InternalState.FULFILLED
    State.REFUSED -> InternalState.REFUSED
    State.WITHDRAWN -> InternalState.WITHDRAWN
    State.STATE_UNSPECIFIED,
    State.UNRECOGNIZED -> InternalState.STATE_UNSPECIFIED
  }

/** Converts an internal [DuchyValue] to a public [DuchyEntry.Value]. */
private fun DuchyValue.toDuchyEntryValue(
  externalDuchyId: String,
  externalFulfillingDuchyId: String,
  apiVersion: Version,
): DuchyEntry.Value {
  val duchyCertificateKey =
    DuchyCertificateKey(externalDuchyId, externalIdToApiId(externalDuchyCertificateId))

  val source = this
  return value {
    duchyCertificate = duchyCertificateKey.toName()
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Annotated with @NotNull.
    when (source.protocolCase) {
      DuchyValue.ProtocolCase.LIQUID_LEGIONS_V2 -> {
        liquidLegionsV2 = source.liquidLegionsV2.toLlv2DuchyEntry(apiVersion)
      }
      DuchyValue.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
        reachOnlyLiquidLegionsV2 = source.reachOnlyLiquidLegionsV2.toLlv2DuchyEntry(apiVersion)
      }
      DuchyValue.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
        val containingEncryptionKey = !source.honestMajorityShareShuffle.tinkPublicKey.isEmpty
        val isFulfillingDuchy = externalDuchyId == externalFulfillingDuchyId
        honestMajorityShareShuffle =
          if (containingEncryptionKey && !isFulfillingDuchy) {
            source.honestMajorityShareShuffle.toHmssDuchyEntryWithPublicKey(apiVersion)
          } else {
            DuchyEntry.HonestMajorityShareShuffle.getDefaultInstance()
          }
      }
      DuchyValue.ProtocolCase.PROTOCOL_NOT_SET -> error("protocol not set")
    }
  }
}

private fun LiquidLegionsV2Params.toLlv2DuchyEntry(
  apiVersion: Version
): DuchyEntry.LiquidLegionsV2 {
  val source = this
  return DuchyEntryKt.liquidLegionsV2 {
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

private fun HonestMajorityShareShuffleParams.toHmssDuchyEntryWithPublicKey(
  apiVersion: Version
): DuchyEntry.HonestMajorityShareShuffle {
  val source = this
  return DuchyEntryKt.honestMajorityShareShuffle {
    publicKey = signedMessage {
      setMessage(
        any {
          value = source.tinkPublicKey
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
            }
        }
      )
      signature = source.tinkPublicKeySignature
      signatureAlgorithmOid = source.tinkPublicKeySignatureAlgorithmOid
    }
  }
}

/** Converts an internal duchy map entry to a public [DuchyEntry]. */
private fun Map.Entry<String, DuchyValue>.toDuchyEntry(
  apiVersion: Version,
  externalFulfillingDuchyId: String,
): DuchyEntry {
  val mapEntry = this
  return duchyEntry {
    key = mapEntry.key
    value = mapEntry.value.toDuchyEntryValue(mapEntry.key, externalFulfillingDuchyId, apiVersion)
  }
}

private fun buildInternalStreamRequisitionsRequest(
  filter: ListRequisitionsRequest.Filter,
  parentKey: RequisitionParentKey,
  pageSize: Int,
  pageToken: ListRequisitionsPageToken?,
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
          states += InternalState.WITHDRAWN
        } else {
          states += requestStates.map { it.toInternal() }
        }

        if (pageToken != null) {
          if (
            pageToken.externalDataProviderId != externalDataProviderId ||
              pageToken.externalMeasurementConsumerId != externalMeasurementConsumerId ||
              pageToken.externalMeasurementId != externalMeasurementId ||
              pageToken.statesList != filter.statesList
          ) {
            throw Status.INVALID_ARGUMENT.withDescription(
                "Arguments other than page_size must remain the same for subsequent page requests"
              )
              .asRuntimeException()
          }
          after =
            StreamRequisitionsRequestKt.FilterKt.after {
              updateTime = pageToken.lastRequisition.updateTime
              externalDataProviderId = pageToken.lastRequisition.externalDataProviderId
              externalRequisitionId = pageToken.lastRequisition.externalRequisitionId
            }
        }
      }
    // Fetch one extra to determine if we need to set next_page_token in response.
    limit = pageSize + 1
  }
}

private fun buildNextPageToken(
  filter: ListRequisitionsRequest.Filter,
  internalFilter: StreamRequisitionsRequest.Filter,
  internalRequisitions: List<InternalRequisition>,
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
    lastRequisition = previousPageEnd {
      updateTime = internalRequisitions[internalRequisitions.lastIndex - 1].updateTime
      externalDataProviderId =
        internalRequisitions[internalRequisitions.lastIndex - 1].externalDataProviderId
      externalRequisitionId =
        internalRequisitions[internalRequisitions.lastIndex - 1].externalRequisitionId
    }
  }
}
