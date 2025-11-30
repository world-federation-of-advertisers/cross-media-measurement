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
import com.google.type.Interval
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.ReplaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.api.v2alpha.ReplaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderCapabilities as InternalDataProviderCapabilities
import org.wfanet.measurement.internal.kingdom.DataProviderKt as InternalDataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelLineKey as InternalModelLineKey
import org.wfanet.measurement.internal.kingdom.dataProviderCapabilities as internalDataProviderCapabilities
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.modelLineKey
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalsRequest as internalReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderRequiredDuchiesRequest

class DataProvidersService(
  private val internalClient: DataProvidersCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : DataProvidersCoroutineService(coroutineContext) {

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
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
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
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalDataProvider.toDataProvider()
  }

  override suspend fun replaceDataAvailabilityIntervals(
    request: ReplaceDataAvailabilityIntervalsRequest
  ): DataProvider {
    val key: DataProviderKey =
      DataProviderKey.fromName(request.name)
        ?: throw Status.INVALID_ARGUMENT.withDescription("Resource name unspecified or invalid")
          .asRuntimeException()

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != key) {
      throw permissionDeniedStatus("replaceDataAvailabilityIntervals", request.name)
        .asRuntimeException()
    }

    val internalRequest = internalReplaceDataAvailabilityIntervalsRequest {
      externalDataProviderId = ApiId(key.dataProviderId).externalId.value

      for (entry: DataProvider.DataAvailabilityMapEntry in request.dataAvailabilityIntervalsList) {
        val modelLineKey =
          ModelLineKey.fromName(entry.key)
            ?: throw Status.INVALID_ARGUMENT.withDescription("Invalid ModelLine resource name")
              .asRuntimeException()
        val interval: Interval = entry.value
        if (interval.startTime.seconds == 0L) {
          throw Status.INVALID_ARGUMENT.withDescription("start_time not set").asRuntimeException()
        }
        if (interval.endTime.seconds == 0L) {
          throw Status.INVALID_ARGUMENT.withDescription("end_time not set").asRuntimeException()
        }
        if (Timestamps.compare(interval.startTime, interval.endTime) > 0) {
          throw Status.INVALID_ARGUMENT.withDescription("end_time is before start_time")
            .asRuntimeException()
        }

        dataAvailabilityIntervals +=
          InternalDataProviderKt.dataAvailabilityMapEntry {
            this.key = modelLineKey {
              externalModelProviderId = ApiId(modelLineKey.modelProviderId).externalId.value
              externalModelSuiteId = ApiId(modelLineKey.modelSuiteId).externalId.value
              externalModelLineId = ApiId(modelLineKey.modelLineId).externalId.value
            }
            value = interval
          }
      }
    }

    val internalDataProvider: InternalDataProvider =
      try {
        internalClient.replaceDataAvailabilityIntervals(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
          else -> Status.INTERNAL
        }.toExternalStatusRuntimeException(e)
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
        request.dataAvailabilityInterval.endTime,
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
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalDataProvider.toDataProvider()
  }

  override suspend fun replaceDataProviderCapabilities(
    request: ReplaceDataProviderCapabilitiesRequest
  ): DataProvider {
    val key: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != key) {
      failGrpc(Status.PERMISSION_DENIED) {
        "Permission for method replaceDataProviderCapabilities denied on resource $request.name"
      }
    }

    val response: InternalDataProvider =
      try {
        internalClient.replaceDataProviderCapabilities(
          replaceDataProviderCapabilitiesRequest {
            externalDataProviderId = ApiId(key.dataProviderId).externalId.value
            capabilities = request.capabilities.toInternal()
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          Status.Code.CANCELLED -> Status.CANCELLED
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return response.toDataProvider()
  }

  private fun permissionDeniedStatus(permission: String, resourceName: String) =
    Status.PERMISSION_DENIED.withDescription(
      "Permission $permission denied on resource $resourceName (or it might not exist)"
    )
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
    for (internalEntry in source.dataAvailabilityIntervalsList) {
      val internalKey: InternalModelLineKey = internalEntry.key
      dataAvailabilityIntervals +=
        DataProviderKt.dataAvailabilityMapEntry {
          key =
            ModelLineKey(
                ExternalId(internalKey.externalModelProviderId).apiId.value,
                ExternalId(internalKey.externalModelSuiteId).apiId.value,
                ExternalId(internalKey.externalModelLineId).apiId.value,
              )
              .toName()
          value = internalEntry.value
        }
    }
    dataAvailabilityInterval = source.details.dataAvailabilityInterval
    capabilities = source.details.capabilities.toCapabilities()
  }
}

private fun InternalDataProviderCapabilities.toCapabilities(): DataProvider.Capabilities {
  val source = this
  return DataProviderKt.capabilities {
    honestMajorityShareShuffleSupported = source.honestMajorityShareShuffleSupported
    trusTeeSupported = source.trusTeeSupported
  }
}

private fun DataProvider.Capabilities.toInternal(): InternalDataProviderCapabilities {
  val source = this
  return internalDataProviderCapabilities {
    honestMajorityShareShuffleSupported = source.honestMajorityShareShuffleSupported
    trusTeeSupported = source.trusTeeSupported
  }
}
