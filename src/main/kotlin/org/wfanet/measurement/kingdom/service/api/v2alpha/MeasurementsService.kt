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
import io.grpc.Status
import java.util.AbstractMap
import java.util.Collections
import kotlin.math.min
import kotlinx.coroutines.flow.toList
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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.DataProviderValue
import org.wfanet.measurement.internal.kingdom.Measurement.State as InternalState
import org.wfanet.measurement.internal.kingdom.Measurement.View as InternalMeasurementView
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementPageToken
import org.wfanet.measurement.internal.kingdom.MeasurementPageTokenKt.previousPageEnd
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest
import org.wfanet.measurement.internal.kingdom.measurementPageToken
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

    grpcRequire(measurement.dataProvidersList.isNotEmpty()) { "Data Providers list is empty" }
    val dataProvidersMap = mutableMapOf<Long, DataProviderValue>()
    measurement.dataProvidersList.forEach {
      with(it.validateAndMap()) {
        grpcRequire(!dataProvidersMap.containsKey(key)) {
          "Duplicated keys found in the data_providers."
        }
        dataProvidersMap[key] = value
      }
    }

    grpcRequire(parsedMeasurementSpec.nonceHashesCount == measurement.dataProvidersCount) {
      "nonce_hash list size is not equal to the data_providers list size."
    }

    val internalMeasurement =
      internalMeasurementsStub.createMeasurement(
        request.measurement.toInternal(
          measurementConsumerCertificateKey,
          dataProvidersMap,
          parsedMeasurementSpec
        )
      )

    return internalMeasurement.toMeasurement()
  }

  override suspend fun listMeasurements(
    request: ListMeasurementsRequest
  ): ListMeasurementsResponse {
    val measurementPageToken = request.toMeasurementPageToken()

    val results: List<InternalMeasurement> =
      internalMeasurementsStub
        .streamMeasurements(measurementPageToken.toStreamMeasurementsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListMeasurementsResponse.getDefaultInstance()
    }

    return listMeasurementsResponse {
      measurement +=
        results
          .subList(0, min(results.size, measurementPageToken.pageSize))
          .map(InternalMeasurement::toMeasurement)
      if (results.size > measurementPageToken.pageSize) {
        val pageToken =
          measurementPageToken.copy {
            lastMeasurement =
              previousPageEnd {
                externalMeasurementId = results[results.lastIndex - 1].externalMeasurementId
              }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
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

/** Validates a [MeasurementSpec] for a request. */
private fun MeasurementSpec.validate() {
  grpcRequire(!measurementPublicKey.isEmpty) { "Measurement public key is unspecified" }

  grpcRequire(nonceHashesCount == nonceHashesList.toSet().size) {
    "Duplicated values found in nonce_hashes"
  }

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  when (measurementTypeCase) {
    MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
      val reachPrivacyParams = reachAndFrequency.reachPrivacyParams
      grpcRequire(reachPrivacyParams.epsilon > 0 && reachPrivacyParams.delta > 0) {
        "Reach privacy params are unspecified"
      }

      val frequencyPrivacyParams = reachAndFrequency.frequencyPrivacyParams
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
    grpcRequireNotNull(DataProviderKey.fromName(key)) {
      "Data Provider resource name is either unspecified or invalid"
    }

  val dataProviderCertificateKey =
    grpcRequireNotNull(DataProviderCertificateKey.fromName(value.dataProviderCertificate)) {
      "Data Provider certificate resource name is either unspecified or invalid"
    }

  val publicKey = value.dataProviderPublicKey
  grpcRequire(!publicKey.data.isEmpty && !publicKey.signature.isEmpty) {
    "Data Provider public key is unspecified"
  }

  grpcRequire(!value.encryptedRequisitionSpec.isEmpty) {
    "Encrypted requisition spec is unspecified"
  }
  grpcRequire(!value.nonceHash.isEmpty) { "Nonce hash is unspecified" }

  val dataProviderValue = dataProviderValue {
    externalDataProviderCertificateId = apiIdToExternalId(dataProviderCertificateKey.certificateId)
    dataProviderPublicKey = publicKey.data
    dataProviderPublicKeySignature = publicKey.signature
    encryptedRequisitionSpec = value.encryptedRequisitionSpec
    nonceHash = value.nonceHash
  }

  return AbstractMap.SimpleEntry(
    apiIdToExternalId(dataProviderKey.dataProviderId),
    dataProviderValue
  )
}

/** Convert a list of public [State] to a list of internal [InternalState]. */
private fun List<State>.toInternal(): List<InternalState> {
  val source = this
  val internalStatesList = mutableListOf<InternalState>()
  for (state in source) {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (state) {
      State.AWAITING_REQUISITION_FULFILLMENT -> {
        internalStatesList.add(InternalState.PENDING_REQUISITION_PARAMS)
        internalStatesList.add(InternalState.PENDING_REQUISITION_FULFILLMENT)
      }
      State.COMPUTING -> {
        internalStatesList.add(InternalState.PENDING_PARTICIPANT_CONFIRMATION)
        internalStatesList.add(InternalState.PENDING_COMPUTATION)
      }
      State.SUCCEEDED -> internalStatesList.add(InternalState.SUCCEEDED)
      State.FAILED -> internalStatesList.add(InternalState.FAILED)
      State.CANCELLED -> internalStatesList.add(InternalState.CANCELLED)
      State.STATE_UNSPECIFIED, State.UNRECOGNIZED ->
        failGrpc(Status.INVALID_ARGUMENT) { "State must be valid" }
    }
  }

  return Collections.unmodifiableList(internalStatesList)
}

/** Converts a public [ListMeasurementsRequest] to an internal [MeasurementPageToken]. */
private fun ListMeasurementsRequest.toMeasurementPageToken(): MeasurementPageToken {
  val source = this

  val key =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(source.parent)) {
      "Resource name is either unspecified or invalid"
    }
  grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

  val externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)

  val measurementStatesList = source.filter.statesList.toInternal()

  return if (source.pageToken.isNotBlank()) {
    MeasurementPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.externalMeasurementConsumerId == externalMeasurementConsumerId) {
        "Arguments must be kept the same when using a page token"
      }

      grpcRequire(
        measurementStatesList.containsAll(states) && states.containsAll(measurementStatesList)
      ) { "Arguments must be kept the same when using a page token" }

      if (source.pageSize in 1..MAX_PAGE_SIZE) {
        pageSize = source.pageSize
      }
    }
  } else {
    measurementPageToken {
      pageSize =
        when {
          source.pageSize == 0 -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }

      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      states += measurementStatesList
      lastMeasurement = MeasurementPageToken.PreviousPageEnd.getDefaultInstance()
    }
  }
}

/** Converts an internal [MeasurementPageToken] to an internal [StreamMeasurementsRequest]. */
private fun MeasurementPageToken.toStreamMeasurementsRequest(): StreamMeasurementsRequest {
  val source = this
  return streamMeasurementsRequest {
    // get 1 more than the actual page size for deciding whether or not to set page token
    limit = source.pageSize + 1
    measurementView = InternalMeasurementView.DEFAULT
    filter =
      filter {
        externalMeasurementConsumerId = source.externalMeasurementConsumerId
        states += source.statesList
        externalMeasurementIdAfter = source.lastMeasurement.externalMeasurementId
      }
  }
}
