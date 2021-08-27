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

import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderPublicKeyKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPublicKeyKey
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.PublicKeysGrpcKt.PublicKeysCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ResourceKey
import org.wfanet.measurement.api.v2alpha.UpdatePublicKeyRequest
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.updatePublicKeyRequest

class PublicKeysService(private val internalPublicKeysStub: PublicKeysCoroutineStub) :
  PublicKeysCoroutineImplBase() {

  override suspend fun updatePublicKey(request: UpdatePublicKeyRequest): PublicKey {
    val publicKeyKey =
      grpcRequireNotNull(createPublicKeyResourceKey(request.publicKey.name)) {
        "Resource name is either unspecified or invalid"
      }

    grpcRequire(
      !request.publicKey.publicKey.data.isEmpty && !request.publicKey.publicKey.signature.isEmpty
    ) { "EncryptionPublicKey is unspecified" }

    val certificateKey =
      grpcRequireNotNull(createCertificateResourceKey(request.publicKey.certificate)) {
        "Certificate name is either unspecified or invalid"
      }

    grpcRequire(
      (publicKeyKey is MeasurementConsumerPublicKeyKey &&
        certificateKey is MeasurementConsumerCertificateKey) ||
        (publicKeyKey is DataProviderPublicKeyKey && certificateKey is DataProviderCertificateKey)
    ) { "Resource name does not have same parent as Certificate name" }

    internalPublicKeysStub.updatePublicKey(
      updatePublicKeyRequest {
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
    )

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
