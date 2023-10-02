/*
 * Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import java.util.AbstractMap
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsPageToken
import org.wfanet.measurement.api.v2alpha.ListMeasurementsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.listMeasurementsPageToken
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.DataProviderValue
import org.wfanet.measurement.internal.kingdom.Measurement.View as InternalMeasurementView
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementRequest as internalCreateMeasurementRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

private const val MISSING_RESOURCE_NAME_ERROR = "Resource name is either unspecified or invalid"

class MeasurementsService(
  private val internalMeasurementsStub: MeasurementsCoroutineStub,
  private val noiseMechanisms: List<NoiseMechanism>,
  private val reachOnlyLlV2Enabled: Boolean,
) : MeasurementsCoroutineImplBase() {

  override suspend fun getMeasurement(request: GetMeasurementRequest): Measurement {
    val authenticatedMeasurementConsumerKey = getAuthenticatedMeasurementConsumerKey()

    val key =
      grpcRequireNotNull(MeasurementKey.fromName(request.name)) { MISSING_RESOURCE_NAME_ERROR }

    if (authenticatedMeasurementConsumerKey.measurementConsumerId != key.measurementConsumerId) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Cannot get a Measurement from another MeasurementConsumer"
      }
    }

    val internalGetMeasurementRequest = getMeasurementRequest {
      externalMeasurementId = apiIdToExternalId(key.measurementId)
      externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    }

    val internalMeasurement =
      try {
        internalMeasurementsStub.getMeasurement(internalGetMeasurementRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "Measurement not found" }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    return internalMeasurement.toMeasurement()
  }

  override suspend fun createMeasurement(request: CreateMeasurementRequest): Measurement {
    val authenticatedMeasurementConsumerKey = getAuthenticatedMeasurementConsumerKey()

    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "parent is either unspecified or invalid"
      }

    if (parentKey != authenticatedMeasurementConsumerKey) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Cannot create a Measurement for another MeasurementConsumer"
      }
    }

    val measurementConsumerCertificateKey =
      grpcRequireNotNull(
        MeasurementConsumerCertificateKey.fromName(
          request.measurement.measurementConsumerCertificate
        )
      ) {
        "measurement_consumer_certificate is either unspecified or invalid"
      }
    grpcRequire(
      measurementConsumerCertificateKey.measurementConsumerId == parentKey.measurementConsumerId
    ) {
      "measurement_consumer_certificate does not belong to ${request.parent}"
    }

    grpcRequire(request.measurement.hasMeasurementSpec()) { "measurement_spec is unspecified" }

    val parsedMeasurementSpec =
      try {
        MeasurementSpec.parseFrom(request.measurement.measurementSpec.data)
      } catch (e: InvalidProtocolBufferException) {
        failGrpc(Status.INVALID_ARGUMENT) { "Failed to parse measurement spec" }
      }
    parsedMeasurementSpec.validate()

    grpcRequire(request.measurement.dataProvidersList.isNotEmpty()) {
      "Data Providers list is empty"
    }
    val dataProvidersMap = mutableMapOf<Long, DataProviderValue>()
    request.measurement.dataProvidersList.forEach {
      with(it.validateAndMap()) {
        grpcRequire(!dataProvidersMap.containsKey(key)) {
          "Duplicated keys found in the data_providers."
        }
        dataProvidersMap[key] = value
      }
    }

    grpcRequire(parsedMeasurementSpec.nonceHashesCount == request.measurement.dataProvidersCount) {
      "nonce_hash list size is not equal to the data_providers list size."
    }

    val internalRequest = internalCreateMeasurementRequest {
      measurement =
        request.measurement.toInternal(
          measurementConsumerCertificateKey,
          dataProvidersMap,
          parsedMeasurementSpec,
          noiseMechanisms.map { it.toInternal() },
          reachOnlyLlV2Enabled
        )
      requestId = request.requestId
    }
    val internalMeasurement =
      try {
        internalMeasurementsStub.createMeasurement(internalRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.INVALID_ARGUMENT ->
            failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid" }
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { ex.message ?: "Failed precondition" }
          Status.Code.NOT_FOUND ->
            failGrpc(Status.NOT_FOUND, ex) { "MeasurementConsumer not found." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    return internalMeasurement.toMeasurement()
  }

  override suspend fun listMeasurements(
    request: ListMeasurementsRequest
  ): ListMeasurementsResponse {
    val authenticatedMeasurementConsumerKey = getAuthenticatedMeasurementConsumerKey()

    val listMeasurementsPageToken = request.toListMeasurementsPageToken()

    if (
      apiIdToExternalId(authenticatedMeasurementConsumerKey.measurementConsumerId) !=
        listMeasurementsPageToken.externalMeasurementConsumerId
    ) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Cannot list Measurements for other MeasurementConsumers"
      }
    }

    val results: List<InternalMeasurement> =
      internalMeasurementsStub
        .streamMeasurements(listMeasurementsPageToken.toStreamMeasurementsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListMeasurementsResponse.getDefaultInstance()
    }

    return listMeasurementsResponse {
      measurements +=
        results.subList(0, min(results.size, listMeasurementsPageToken.pageSize)).map {
          internalMeasurement ->
          internalMeasurement.toMeasurement()
        }
      if (results.size > listMeasurementsPageToken.pageSize) {
        val pageToken =
          listMeasurementsPageToken.copy {
            lastMeasurement = previousPageEnd {
              externalMeasurementId = results[results.lastIndex - 1].externalMeasurementId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun cancelMeasurement(request: CancelMeasurementRequest): Measurement {
    val authenticatedMeasurementConsumerKey = getAuthenticatedMeasurementConsumerKey()

    val key =
      grpcRequireNotNull(MeasurementKey.fromName(request.name)) { MISSING_RESOURCE_NAME_ERROR }

    if (authenticatedMeasurementConsumerKey.measurementConsumerId != key.measurementConsumerId) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Cannot cancel a Measurement for another MeasurementConsumer"
      }
    }

    val internalCancelMeasurementRequest = cancelMeasurementRequest {
      externalMeasurementId = apiIdToExternalId(key.measurementId)
      externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    }

    val internalMeasurement =
      try {
        internalMeasurementsStub.cancelMeasurement(internalCancelMeasurementRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.INVALID_ARGUMENT ->
            failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid" }
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "Measurement not found." }
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { "Measurement state illegal." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    return internalMeasurement.toMeasurement()
  }
}

private fun DifferentialPrivacyParams.hasValidEpsilonAndDelta(): Boolean {
  return this.epsilon > 0 && this.delta >= 0
}

/** Validates a [MeasurementSpec] for a request. */
private fun MeasurementSpec.validate() {
  grpcRequire(!measurementPublicKey.isEmpty) { "Measurement public key is unspecified" }

  grpcRequire(nonceHashesCount == nonceHashesList.toSet().size) {
    "Duplicated values found in nonce_hashes"
  }

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  when (measurementTypeCase) {
    MeasurementSpec.MeasurementTypeCase.REACH -> {
      grpcRequire(reach.privacyParams.hasValidEpsilonAndDelta()) {
        "Reach privacy params are invalid"
      }

      grpcRequire(vidSamplingInterval.width > 0) { "Vid sampling interval is unspecified" }
    }
    MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
      grpcRequire(reachAndFrequency.reachPrivacyParams.hasValidEpsilonAndDelta()) {
        "Reach privacy params are invalid"
      }

      grpcRequire(reachAndFrequency.frequencyPrivacyParams.hasValidEpsilonAndDelta()) {
        "Frequency privacy params are invalid"
      }
      grpcRequire(reachAndFrequency.maximumFrequency > 0) {
        "maximum_frequency must be greater than 0"
      }

      grpcRequire(vidSamplingInterval.width > 0) { "Vid sampling interval is unspecified" }
    }
    MeasurementSpec.MeasurementTypeCase.IMPRESSION -> {
      grpcRequire(impression.privacyParams.hasValidEpsilonAndDelta()) {
        "Impressions privacy params are invalid"
      }

      grpcRequire(impression.maximumFrequencyPerUser > 0) {
        "Maximum frequency per user is unspecified"
      }
    }
    MeasurementSpec.MeasurementTypeCase.DURATION -> {
      grpcRequire(duration.privacyParams.hasValidEpsilonAndDelta()) {
        "Duration privacy params are invalid"
      }

      grpcRequire(duration.hasMaximumWatchDurationPerUser()) {
        "Maximum watch duration per user is unspecified"
      }
    }
    MeasurementSpec.MeasurementTypeCase.POPULATION -> {
      grpcRequire(modelLine.isNotEmpty()) { "Model Line is unspecified" }
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
    dataProviderPublicKeySignatureAlgorithmOid = publicKey.signatureAlgorithmOid
    encryptedRequisitionSpec = value.encryptedRequisitionSpec
    nonceHash = value.nonceHash
  }

  return AbstractMap.SimpleEntry(
    apiIdToExternalId(dataProviderKey.dataProviderId),
    dataProviderValue
  )
}

/** Converts a public [ListMeasurementsRequest] to an internal [ListMeasurementsPageToken]. */
private fun ListMeasurementsRequest.toListMeasurementsPageToken(): ListMeasurementsPageToken {
  val source = this

  val key =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(source.parent)) {
      MISSING_RESOURCE_NAME_ERROR
    }
  grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

  val externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)

  val measurementStatesList = source.filter.statesList

  return if (source.pageToken.isNotBlank()) {
    ListMeasurementsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.externalMeasurementConsumerId == externalMeasurementConsumerId) {
        "Arguments must be kept the same when using a page token"
      }

      grpcRequire(
        measurementStatesList.containsAll(states) && states.containsAll(measurementStatesList)
      ) {
        "Arguments must be kept the same when using a page token"
      }

      if (source.pageSize in 1..MAX_PAGE_SIZE) {
        pageSize = source.pageSize
      }
    }
  } else {
    listMeasurementsPageToken {
      pageSize =
        when {
          source.pageSize == 0 -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }

      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      states += measurementStatesList
    }
  }
}

/** Converts an internal [ListMeasurementsPageToken] to an internal [StreamMeasurementsRequest]. */
private fun ListMeasurementsPageToken.toStreamMeasurementsRequest(): StreamMeasurementsRequest {
  val source = this
  return streamMeasurementsRequest {
    // get 1 more than the actual page size for deciding whether to set page token
    limit = source.pageSize + 1
    measurementView = InternalMeasurementView.DEFAULT
    filter = filter {
      externalMeasurementConsumerId = source.externalMeasurementConsumerId
      states += source.statesList.map { it.toInternalState() }.flatten()
      if (source.hasLastMeasurement()) {
        after =
          StreamMeasurementsRequestKt.FilterKt.after {
            updateTime = source.lastMeasurement.updateTime
            measurement = measurementKey {
              externalMeasurementConsumerId = source.externalMeasurementConsumerId
              externalMeasurementId = source.lastMeasurement.externalMeasurementId
            }
          }
      }
    }
  }
}

private fun getAuthenticatedMeasurementConsumerKey(): MeasurementConsumerKey {
  val principal: MeasurementPrincipal = principalFromCurrentContext

  if (principal !is MeasurementConsumerPrincipal) {
    failGrpc(Status.PERMISSION_DENIED) { "Caller cannot get a Measurement" }
  }

  return principal.resourceKey
}
