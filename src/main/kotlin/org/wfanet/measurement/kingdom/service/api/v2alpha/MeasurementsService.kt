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

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.util.AbstractMap
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.State
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKey
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.DataProviderValue
import org.wfanet.measurement.internal.kingdom.Measurement.State as InternalState
import org.wfanet.measurement.internal.kingdom.Measurement.View as InternalMeasurementView
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest
import org.wfanet.measurement.internal.kingdom.measurement as internalMeasurement
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class MeasurementsService(private val internalMeasurementsStub: MeasurementsCoroutineStub) :
  MeasurementsCoroutineImplBase() {

  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    val key =
      grpcRequireNotNull(MeasurementKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val internalGetMeasurementRequest = getMeasurementRequest {
      externalMeasurementId = apiIdToExternalId(key.measurementId)
      externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
      measurementView = InternalMeasurementView.DEFAULT
    }

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
      "Measurement spec is unspecified"
    }

    val parsedMeasurementSpec =
      try {
        MeasurementSpec.parseFrom(measurementSpec.data)
      } catch (e: InvalidProtocolBufferException) {
        failGrpc(Status.INVALID_ARGUMENT) { "Failed to parse measurement spec" }
      }
    parsedMeasurementSpec.validate()

    grpcRequire(!measurement.serializedDataProviderList.isEmpty) {
      "Serialized Data Provider list is unspecified"
    }

    grpcRequire(!measurement.dataProviderListSalt.isEmpty) {
      "Data Provider list salt is unspecified"
    }

    grpcRequire(measurement.dataProvidersList.isNotEmpty()) { "Data Providers list is empty" }
    val dataProvidersMap = mutableMapOf<Long, DataProviderValue>()
    measurement.dataProvidersList.forEach {
      with(it.validateAndMap()) { dataProvidersMap[key] = value }
    }

    val internalMeasurement =
      internalMeasurementsStub.createMeasurement(
        request.measurement.toInternal(measurementConsumerCertificateKey, dataProvidersMap)
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

    val streamMeasurementsRequest = streamMeasurementsRequest {
      limit = pageSize
      measurementView = InternalMeasurementView.DEFAULT
      filter =
        filter {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
          if (request.pageToken.isNotBlank()) {
            updatedAfter = Timestamp.parseFrom(request.pageToken.base64UrlDecode())
          }
          for (state in request.filter.statesList) {
            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
            when (state) {
              State.AWAITING_REQUISITION_FULFILLMENT -> {
                states += InternalState.PENDING_REQUISITION_PARAMS
                states += InternalState.PENDING_REQUISITION_FULFILLMENT
              }
              State.COMPUTING -> {
                states += InternalState.PENDING_PARTICIPANT_CONFIRMATION
                states += InternalState.PENDING_COMPUTATION
              }
              State.SUCCEEDED -> states += InternalState.SUCCEEDED
              State.FAILED -> states += InternalState.FAILED
              State.CANCELLED -> states += InternalState.CANCELLED
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

    return listMeasurementsResponse {
      measurement += results.map(InternalMeasurement::toMeasurement)
      nextPageToken = results.last().updateTime.toByteArray().base64UrlEncode()
    }
  }

  override suspend fun cancelMeasurement(request: CancelMeasurementRequest): Measurement {
    val key =
      grpcRequireNotNull(MeasurementKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val internalCancelMeasurementRequest = cancelMeasurementRequest {
      externalMeasurementId = apiIdToExternalId(key.measurementId)
      externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    }

    val internalMeasurement =
      internalMeasurementsStub.cancelMeasurement(internalCancelMeasurementRequest)

    return internalMeasurement.toMeasurement()
  }
}

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

  return measurement {
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
    measurementSpec =
      signedData {
        data = details.measurementSpec
        signature = details.measurementSpecSignature
      }
    serializedDataProviderList = details.dataProviderList
    dataProviderListSalt = details.dataProviderListSalt
    dataProviders +=
      dataProvidersMap.entries.map(Map.Entry<Long, DataProviderValue>::toDataProviderEntry)
    protocolConfig = ProtocolConfigKey(externalProtocolConfigId).toName()
    state = this@toMeasurement.state.toState()
    aggregatorCertificate = details.aggregatorCertificate
    encryptedResult = details.encryptedResult
    measurementReferenceId = providedMeasurementId
  }
}

/** Converts an internal [DataProviderValue] to a public [DataProviderEntry.Value]. */
private fun DataProviderValue.toDataProviderEntryValue(
  dataProviderId: String
): DataProviderEntry.Value {
  val dataProviderValue = this
  return DataProviderEntryKt.value {
    dataProviderCertificate =
      DataProviderCertificateKey(
          dataProviderId,
          externalIdToApiId(externalDataProviderCertificateId)
        )
        .toName()
    dataProviderPublicKey =
      signedData {
        data = dataProviderValue.dataProviderPublicKey
        signature = dataProviderPublicKeySignature
      }
    encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
  }
}

/** Converts an internal data provider map entry to a public [DataProviderEntry]. */
private fun Map.Entry<Long, DataProviderValue>.toDataProviderEntry(): DataProviderEntry {
  val mapEntry = this
  return dataProviderEntry {
    key = DataProviderKey(externalIdToApiId(mapEntry.key)).toName()
    value = mapEntry.value.toDataProviderEntryValue(externalIdToApiId(mapEntry.key))
  }
}

/** Converts a public [Measurement] to an internal [InternalMeasurement] for creation. */
private fun Measurement.toInternal(
  measurementConsumerCertificateKey: MeasurementConsumerCertificateKey,
  dataProvidersMap: Map<Long, DataProviderValue>
): InternalMeasurement {
  val publicMeasurement = this
  return internalMeasurement {
    providedMeasurementId = measurementReferenceId
    externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerCertificateKey.measurementConsumerId)
    externalMeasurementConsumerCertificateId =
      apiIdToExternalId(measurementConsumerCertificateKey.certificateId)
    dataProviders.putAll(dataProvidersMap)
    details =
      details {
        apiVersion = Version.V2_ALPHA.string
        measurementSpec = publicMeasurement.measurementSpec.data
        measurementSpecSignature = publicMeasurement.measurementSpec.signature
        dataProviderList = publicMeasurement.serializedDataProviderList
        dataProviderListSalt = publicMeasurement.dataProviderListSalt
      }
  }
}

/** Validates a [MeasurementSpec] for a request. */
private fun MeasurementSpec.validate() {
  grpcRequire(!this.measurementPublicKey.isEmpty) { "Measurement public key is unspecified" }

  grpcRequire(
    this.cipherSuite.kem !=
      HybridCipherSuite.KeyEncapsulationMechanism.KEY_ENCAPSULATION_MECHANISM_UNSPECIFIED &&
      this.cipherSuite.dem !=
        HybridCipherSuite.DataEncapsulationMechanism.DATA_ENCAPSULATION_MECHANISM_UNSPECIFIED
  ) { "Measurement cipher suite is unspecified" }

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  when (this.measurementTypeCase) {
    MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
      val reachPrivacyParams = this.reachAndFrequency.reachPrivacyParams
      grpcRequire(reachPrivacyParams.epsilon > 0 && reachPrivacyParams.delta > 0) {
        "Reach privacy params are unspecified"
      }

      val frequencyPrivacyParams = this.reachAndFrequency.frequencyPrivacyParams
      grpcRequire(frequencyPrivacyParams.epsilon > 0 && frequencyPrivacyParams.delta > 0) {
        "Frequency privacy params are unspecified"
      }
    }
    MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
      failGrpc(Status.INVALID_ARGUMENT) { "Measurement type is unspecified" }
  }
}

/** Validates a [DataProviderEntry] for a request and then creates a map entry from it. */
private fun DataProviderEntry.validateAndMap(): Map.Entry<Long, DataProviderValue> {
  val dataProviderKey =
    grpcRequireNotNull(DataProviderKey.fromName(this.key)) {
      "Data Provider resource name is either unspecified or invalid"
    }

  val dataProviderCertificateKey =
    grpcRequireNotNull(DataProviderCertificateKey.fromName(this.value.dataProviderCertificate)) {
      "Data Provider certificate resource name is either unspecified or invalid"
    }

  val publicKey = this.value.dataProviderPublicKey
  grpcRequire(!publicKey.data.isEmpty && !publicKey.signature.isEmpty) {
    "Data Provider public key is unspecified"
  }

  grpcRequire(!this.value.encryptedRequisitionSpec.isEmpty) {
    "Encrypted Requisition spec is unspecified"
  }

  val dataProviderEntry = this
  val dataProviderValue = dataProviderValue {
    externalDataProviderCertificateId = apiIdToExternalId(dataProviderCertificateKey.certificateId)
    dataProviderPublicKey = publicKey.data
    dataProviderPublicKeySignature = publicKey.signature
    encryptedRequisitionSpec = dataProviderEntry.value.encryptedRequisitionSpec
  }

  return AbstractMap.SimpleEntry(
    apiIdToExternalId(dataProviderKey.dataProviderId),
    dataProviderValue
  )
}
