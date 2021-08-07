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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.State
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKey
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.kingdom.CancelMeasurementRequest as InternalCancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.GetMeasurementRequest as InternalGetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.DataProviderValue
import org.wfanet.measurement.internal.kingdom.Measurement.State as InternalState
import org.wfanet.measurement.internal.kingdom.Measurement.View as InternalMeasurementView
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class MeasurementsService(private val internalMeasurementsStub: MeasurementsCoroutineStub) :
  MeasurementsCoroutineImplBase() {

  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    val key =
      grpcRequireNotNull(MeasurementKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val internalGetMeasurementRequest =
      InternalGetMeasurementRequest.newBuilder()
        .apply {
          externalMeasurementId = apiIdToExternalId(key.measurementId)
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
          measurementView = InternalMeasurementView.DEFAULT
        }
        .build()

    val internalMeasurement = internalMeasurementsStub.getMeasurement(internalGetMeasurementRequest)

    return internalMeasurement.toMeasurement()
  }

  override suspend fun createMeasurement(request: CreateMeasurementRequest): Measurement {
    val measurement = request.measurement

    val measurementConsumerCertificateKey =
      grpcRequireNotNull(
        MeasurementConsumerCertificateKey.fromName(measurement.measurementConsumerCertificate)
      ) { "Measurement Consumer Certificate resource name is either unspecified or invalid" }

    val measurementSpec = measurement.measurementSpec
    grpcRequire(!measurementSpec.data.isEmpty && !measurementSpec.signature.isEmpty) {
      "Measurement spec is either unspecified or invalid"
    }

    grpcRequire(!measurement.serializedDataProviderList.isEmpty) {
      "Serialized Data Provider list is either unspecified or invalid"
    }

    grpcRequire(!measurement.dataProviderListSalt.isEmpty) {
      "Data Provider list salt is either unspecified or invalid"
    }

    grpcRequire(measurement.dataProvidersList.isNotEmpty()) { "Data Providers list is empty" }

    val internalMeasurement =
      internalMeasurementsStub.createMeasurement(
        request.measurement.toInternal(measurementConsumerCertificateKey)
      )

    return internalMeasurement.toMeasurement()
  }

  override suspend fun listMeasurements(
    request: ListMeasurementsRequest
  ): ListMeasurementsResponse {
    val key =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    val pageSize =
      when {
        request.pageSize == 0 -> DEFAULT_PAGE_SIZE
        request.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
        else -> request.pageSize
      }

    val streamMeasurementsRequest = buildStreamMeasurementsRequest {
      limit = pageSize
      measurementView = InternalMeasurementView.DEFAULT
      filter {
        externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
        if (request.pageToken.isNotBlank()) {
          updatedAfter = Timestamp.parseFrom(request.pageToken.base64UrlDecode())
        }
        for (state in request.filter.statesList) {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
          when (state) {
            State.AWAITING_REQUISITION_FULFILLMENT ->
              addAllStates(
                listOf(
                  InternalState.PENDING_REQUISITION_PARAMS,
                  InternalState.PENDING_REQUISITION_FULFILLMENT
                )
              )
            State.COMPUTING ->
              addAllStates(
                listOf(
                  InternalState.PENDING_PARTICIPANT_CONFIRMATION,
                  InternalState.PENDING_COMPUTATION
                )
              )
            State.SUCCEEDED -> addStates(InternalState.SUCCEEDED)
            State.FAILED -> addStates(InternalState.FAILED)
            State.CANCELLED -> addStates(InternalState.CANCELLED)
            State.STATE_UNSPECIFIED, State.UNRECOGNIZED ->
              failGrpc(Status.INVALID_ARGUMENT) { "State must be valid" }
          }
        }
      }
    }

    val results: List<InternalMeasurement> =
      internalMeasurementsStub.streamMeasurements(streamMeasurementsRequest).toList()

    if (results.isEmpty()) {
      return ListMeasurementsResponse.getDefaultInstance()
    }

    return ListMeasurementsResponse.newBuilder()
      .addAllMeasurement(results.map(InternalMeasurement::toMeasurement))
      .setNextPageToken(results.last().updateTime.toByteArray().base64UrlEncode())
      .build()
  }

  override suspend fun cancelMeasurement(request: CancelMeasurementRequest): Measurement {
    val key =
      grpcRequireNotNull(MeasurementKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val internalCancelMeasurementRequest =
      InternalCancelMeasurementRequest.newBuilder()
        .apply {
          externalMeasurementId = apiIdToExternalId(key.measurementId)
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
        }
        .build()

    val internalMeasurement =
      internalMeasurementsStub.cancelMeasurement(internalCancelMeasurementRequest)

    return internalMeasurement.toMeasurement()
  }
}

internal inline fun buildMeasurement(fill: (@Builder Measurement.Builder).() -> Unit) =
  Measurement.newBuilder().apply(fill).build()

internal inline fun Measurement.Builder.measurementSpec(
  fill: (@Builder SignedData.Builder).() -> Unit
) = measurementSpecBuilder.apply(fill)

/** Converts an internal [InternalState] to a public [State]. */
private fun InternalState.toState(): State =
  when (this) {
    InternalState.PENDING_REQUISITION_PARAMS, InternalState.PENDING_REQUISITION_FULFILLMENT ->
      State.AWAITING_REQUISITION_FULFILLMENT
    InternalState.PENDING_PARTICIPANT_CONFIRMATION, InternalState.PENDING_COMPUTATION ->
      State.COMPUTING
    InternalState.SUCCEEDED -> State.SUCCEEDED
    InternalState.FAILED -> State.FAILED
    InternalState.CANCELLED -> State.CANCELLED
    InternalState.STATE_UNSPECIFIED, InternalState.UNRECOGNIZED -> State.STATE_UNSPECIFIED
  }

/** Converts an internal [InternalMeasurement] to a public [Measurement]. */
private fun InternalMeasurement.toMeasurement(): Measurement {
  check(Version.fromString(details.apiVersion) == Version.V2_ALPHA) {
    "Incompatible API version ${details.apiVersion}"
  }

  return buildMeasurement {
    name =
      MeasurementKey(
          externalIdToApiId(externalMeasurementConsumerId),
          externalIdToApiId(externalMeasurementId)
        )
        .toName()
    measurementConsumerCertificate =
      MeasurementConsumerCertificateKey(
          externalIdToApiId(externalMeasurementConsumerId),
          externalIdToApiId(externalMeasurementConsumerCertificateId)
        )
        .toName()
    measurementSpec {
      data = details.measurementSpec
      signature = details.measurementSpecSignature
    }
    serializedDataProviderList = details.dataProviderList
    dataProviderListSalt = details.dataProviderListSalt
    addAllDataProviders(
      dataProvidersMap.entries.map(Map.Entry<Long, DataProviderValue>::toDataProviderEntry)
    )
    protocolConfig = ProtocolConfigKey(externalProtocolConfigId).toName()
    state = this@toMeasurement.state.toState()
    aggregatorCertificate = details.aggregatorCertificate
    encryptedResult = details.encryptedResult
    measurementReferenceId = providedMeasurementId
  }
}

internal inline fun DataProviderEntry.Value.Builder.dataProviderPublicKey(
  fill: (@Builder SignedData.Builder).() -> Unit
) = dataProviderPublicKeyBuilder.apply(fill)

/** Converts an internal [DataProviderValue] to a public [DataProviderEntry.Value]. */
private fun DataProviderValue.toDataProviderEntryValue(
  dataProviderId: String
): DataProviderEntry.Value {
  return buildDataProviderEntryValue {
    dataProviderCertificate =
      DataProviderCertificateKey(
          dataProviderId,
          externalIdToApiId(externalDataProviderCertificateId)
        )
        .toName()
    dataProviderPublicKey {
      data = this@toDataProviderEntryValue.dataProviderPublicKey
      signature = dataProviderPublicKeySignature
    }
    encryptedRequisitionSpec = this@toDataProviderEntryValue.encryptedRequisitionSpec
  }
}

internal inline fun buildDataProviderEntryValue(
  fill: (@Builder DataProviderEntry.Value.Builder).() -> Unit
) = DataProviderEntry.Value.newBuilder().apply(fill).build()

internal inline fun buildDataProviderEntry(fill: (@Builder DataProviderEntry.Builder).() -> Unit) =
  DataProviderEntry.newBuilder().apply(fill).build()

/** Converts an internal data provider map entry to a public [DataProviderEntry]. */
private fun Map.Entry<Long, DataProviderValue>.toDataProviderEntry(): DataProviderEntry {
  return buildDataProviderEntry {
    key = DataProviderKey(externalIdToApiId(this@toDataProviderEntry.key)).toName()
    value =
      this@toDataProviderEntry.value.toDataProviderEntryValue(
        externalIdToApiId(this@toDataProviderEntry.key)
      )
  }
}

internal inline fun buildInternalMeasurement(
  fill: (@Builder InternalMeasurement.Builder).() -> Unit
) = InternalMeasurement.newBuilder().apply(fill).build()

/** Converts a public [Measurement] to an internal [InternalMeasurement] for creation. */
private fun Measurement.toInternal(
  measurementConsumerCertificateKey: MeasurementConsumerCertificateKey
): InternalMeasurement {
  return buildInternalMeasurement {
    providedMeasurementId = measurementReferenceId
    externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerCertificateKey.measurementConsumerId)
    externalMeasurementConsumerCertificateId =
      apiIdToExternalId(measurementConsumerCertificateKey.certificateId)
    putAllDataProviders(
      this@toInternal.dataProvidersList.associateBy(
        {
          val key =
            grpcRequireNotNull(DataProviderKey.fromName(it.key)) {
              "Data Provider resource name is either unspecified or invalid"
            }
          apiIdToExternalId(key.dataProviderId)
        },
        {
          val key =
            grpcRequireNotNull(
              DataProviderCertificateKey.fromName(it.value.dataProviderCertificate)
            ) { "Data Provider certificate resource name is either unspecified or invalid" }

          val publicKey = it.value.dataProviderPublicKey
          grpcRequire(!publicKey.data.isEmpty && !publicKey.signature.isEmpty) {
            "Data Provider public key is either unspecified or invalid"
          }

          grpcRequire(!it.value.encryptedRequisitionSpec.isEmpty) {
            "Encrypted Requisition spec is either unspecified or invalid"
          }

          DataProviderValue.newBuilder()
            .apply {
              externalDataProviderCertificateId = apiIdToExternalId(key.certificateId)
              dataProviderPublicKey = it.value.dataProviderPublicKey.data
              dataProviderPublicKeySignature = it.value.dataProviderPublicKey.signature
              encryptedRequisitionSpec = it.value.encryptedRequisitionSpec
            }
            .build()
        }
      )
    )
    detailsBuilder.apply {
      apiVersion = Version.V2_ALPHA.string
      measurementSpec = this@toInternal.measurementSpec.data
      measurementSpecSignature = this@toInternal.measurementSpec.signature
      dataProviderList = this@toInternal.serializedDataProviderList
      dataProviderListSalt = this@toInternal.dataProviderListSalt
    }
    detailsJson = details.toJson()
  }
}

internal inline fun buildStreamMeasurementsRequest(
  fill: (@Builder StreamMeasurementsRequest.Builder).() -> Unit
) = StreamMeasurementsRequest.newBuilder().apply(fill).build()

internal inline fun StreamMeasurementsRequest.Builder.filter(
  fill: (@Builder StreamMeasurementsRequest.Filter.Builder).() -> Unit
) = filterBuilder.apply(fill)
