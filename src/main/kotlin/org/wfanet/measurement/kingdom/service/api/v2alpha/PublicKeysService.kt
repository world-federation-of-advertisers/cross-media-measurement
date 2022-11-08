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
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DataProviderPublicKeyKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPublicKeyKey
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.PublicKeysGrpcKt.PublicKeysCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.UpdatePublicKeyRequest
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.updatePublicKeyRequest

class PublicKeysService(private val internalPublicKeysStub: PublicKeysCoroutineStub) :
  PublicKeysCoroutineImplBase() {

  override suspend fun updatePublicKey(request: UpdatePublicKeyRequest): PublicKey {
    val principal: MeasurementPrincipal = principalFromCurrentContext

    val publicKeyKey =
      grpcRequireNotNull(createPublicKeyResourceKey(request.publicKey.name)) {
        "Resource name is either unspecified or invalid"
      }

    val certificateKey =
      grpcRequireNotNull(createCertificateResourceKey(request.publicKey.certificate)) {
        "Certificate name is either unspecified or invalid"
      }

    when (principal) {
      is DataProviderPrincipal -> {
        when (publicKeyKey) {
          is DataProviderPublicKeyKey -> {
            if (
              apiIdToExternalId(principal.resourceKey.dataProviderId) !=
                apiIdToExternalId(publicKeyKey.dataProviderId)
            ) {
              failGrpc(Status.PERMISSION_DENIED) {
                "Cannot update Public Key belonging to another DataProvider"
              }
            }
          }
          else -> {
            failGrpc(Status.PERMISSION_DENIED) { "Caller can only update Public Key for itself" }
          }
        }
      }
      is MeasurementConsumerPrincipal -> {
        when (publicKeyKey) {
          is MeasurementConsumerPublicKeyKey -> {
            if (
              apiIdToExternalId(principal.resourceKey.measurementConsumerId) !=
                apiIdToExternalId(publicKeyKey.measurementConsumerId)
            ) {
              failGrpc(Status.PERMISSION_DENIED) {
                "Cannot update Public Key belonging to another MeasurementConsumer"
              }
            }
          }
          else -> {
            failGrpc(Status.PERMISSION_DENIED) { "Caller can only update Public Key for itself" }
          }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to update Public Key"
        }
      }
    }

    grpcRequire(
      (publicKeyKey is MeasurementConsumerPublicKeyKey &&
        certificateKey is MeasurementConsumerCertificateKey &&
        publicKeyKey.measurementConsumerId == certificateKey.measurementConsumerId) ||
        (publicKeyKey is DataProviderPublicKeyKey &&
          certificateKey is DataProviderCertificateKey &&
          publicKeyKey.dataProviderId == certificateKey.dataProviderId)
    ) {
      "Resource name does not have same parent as Certificate name"
    }

    grpcRequire(
      !request.publicKey.publicKey.data.isEmpty && !request.publicKey.publicKey.signature.isEmpty
    ) {
      "EncryptionPublicKey is unspecified"
    }

    val updateRequest = updatePublicKeyRequest {
      when (certificateKey) {
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = apiIdToExternalId(certificateKey.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(certificateKey.certificateId)
        }
        is DataProviderCertificateKey -> {
          externalDataProviderId = apiIdToExternalId(certificateKey.dataProviderId)
          externalCertificateId = apiIdToExternalId(certificateKey.certificateId)
        }
      }
      apiVersion = Version.V2_ALPHA.toString()
      publicKey = request.publicKey.publicKey.data
      publicKeySignature = request.publicKey.publicKey.signature
    }
    try {
      internalPublicKeysStub.updatePublicKey(updateRequest)
    } catch (ex: StatusException) {
      when (ex.status.code) {
        Status.Code.INVALID_ARGUMENT ->
          failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid" }
        Status.Code.FAILED_PRECONDITION ->
          failGrpc(Status.FAILED_PRECONDITION, ex) { "Certificate not found." }
        Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "Not found." }
        else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
      }
    }

    return request.publicKey
  }
}

/** Checks the resource name against multiple public key [ResourceKey] to find the right one. */
private fun createPublicKeyResourceKey(name: String): ResourceKey? {
  return when {
    DataProviderPublicKeyKey.fromName(name) != null -> DataProviderPublicKeyKey.fromName(name)
    MeasurementConsumerPublicKeyKey.fromName(name) != null ->
      MeasurementConsumerPublicKeyKey.fromName(name)
    else -> null
  }
}

/** Checks the resource name against multiple certificate [ResourceKey] to find the right one. */
private fun createCertificateResourceKey(name: String): ResourceKey? {
  return when {
    DataProviderCertificateKey.fromName(name) != null -> DataProviderCertificateKey.fromName(name)
    MeasurementConsumerCertificateKey.fromName(name) != null ->
      MeasurementConsumerCertificateKey.fromName(name)
    else -> null
  }
}
