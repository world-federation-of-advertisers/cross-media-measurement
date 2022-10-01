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

import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest

private val API_VERSION = Version.V2_ALPHA

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
}

private fun InternalDataProvider.toDataProvider(): DataProvider {
  check(Version.fromString(details.apiVersion) == API_VERSION) {
    "Incompatible API version ${details.apiVersion}"
  }
  val internalDataProvider = this
  val dataProviderId: String = externalIdToApiId(externalDataProviderId)
  val certificateId: String = externalIdToApiId(certificate.externalCertificateId)

  return dataProvider {
    name = DataProviderKey(dataProviderId).toName()
    certificate = DataProviderCertificateKey(dataProviderId, certificateId).toName()
    certificateDer = internalDataProvider.certificate.details.x509Der
    publicKey = signedData {
      data = internalDataProvider.details.publicKey
      signature = internalDataProvider.details.publicKeySignature
    }
  }
}
