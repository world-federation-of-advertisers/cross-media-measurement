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

import com.google.protobuf.any
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.api.v2alpha.ReplaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderRequiredDuchiesRequest

class DataProvidersService(private val internalClient: DataProvidersCoroutineStub) :
  DataProvidersCoroutineService() {

  override suspend fun getDataProvider(request: GetDataProviderRequest): DataProvider {
    val key: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is DataProviderPrincipal -> {
        if (principal.resourceKey.dataProviderId != key.dataProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot get other DataProviders" }
        }
      }
      is MeasurementConsumerPrincipal -> {}
      is ModelProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to get DataProviders"
        }
      }
    }

    val internalDataProvider =
      try {
        internalClient.getDataProvider(
          getDataProviderRequest { externalDataProviderId = apiIdToExternalId(key.dataProviderId) }
        )
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "DataProvider not found" }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    return internalDataProvider.toDataProvider()
  }

  override suspend fun replaceDataProviderRequiredDuchies(
    request: ReplaceDataProviderRequiredDuchiesRequest
  ): DataProvider {
    val key: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val requiredExternalDuchyIds: List<String> =
      request.requiredDuchiesList.map { name ->
        val duchyKey =
          grpcRequireNotNull(DuchyKey.fromName(name)) { "required_duchies entry invalid" }
        duchyKey.duchyId
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != key) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Permission for method replaceDataProviderRequiredDuchies denied on resource $request.name"
      }
    }

    val internalDataProvider: InternalDataProvider =
      try {
        internalClient.replaceDataProviderRequiredDuchies(
          replaceDataProviderRequiredDuchiesRequest {
            externalDataProviderId = apiIdToExternalId(key.dataProviderId)
            this.requiredExternalDuchyIds += requiredExternalDuchyIds
          }
        )
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "DataProvider not found" }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }
    return internalDataProvider.toDataProvider()
  }

  override suspend fun replaceDataAvailabilityInterval(
    request: ReplaceDataAvailabilityIntervalRequest
  ): DataProvider {
    val key: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != key) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Permission for method replaceDataAvailabilityInterval denied on resource $request.name"
      }
    }

    grpcRequire(
      request.dataAvailabilityInterval.startTime.seconds > 0 &&
        request.dataAvailabilityInterval.endTime.seconds > 0
    ) {
      "Both start_time and end_time are required in data_availability_interval"
    }

    grpcRequire(
      Timestamps.compare(
        request.dataAvailabilityInterval.startTime,
        request.dataAvailabilityInterval.endTime
      ) < 0
    ) {
      "data_availability_interval start_time must be before end_time"
    }

    val internalDataProvider: InternalDataProvider =
      try {
        internalClient.replaceDataAvailabilityInterval(
          replaceDataAvailabilityIntervalRequest {
            externalDataProviderId = apiIdToExternalId(key.dataProviderId)
            dataAvailabilityInterval = request.dataAvailabilityInterval
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("DataProvider not found")
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .asRuntimeException()
      }
    return internalDataProvider.toDataProvider()
  }
}

private fun InternalDataProvider.toDataProvider(): DataProvider {
  val apiVersion = Version.fromString(details.apiVersion)
  val dataProviderId: String = externalIdToApiId(externalDataProviderId)
  val certificateId: String = externalIdToApiId(certificate.externalCertificateId)

  val source = this
  return dataProvider {
    name = DataProviderKey(dataProviderId).toName()
    certificate = DataProviderCertificateKey(dataProviderId, certificateId).toName()
    certificateDer = source.certificate.details.x509Der
    publicKey = signedMessage {
      setMessage(
        any {
          value = source.details.publicKey
          typeUrl =
            when (apiVersion) {
              Version.V2_ALPHA -> ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
            }
        }
      )
      signature = source.details.publicKeySignature
      signatureAlgorithmOid = source.details.publicKeySignatureAlgorithmOid
    }
    requiredDuchies += source.requiredExternalDuchyIdsList.map { DuchyKey(it).toName() }
    dataAvailabilityInterval = source.details.dataAvailabilityInterval
  }
}
