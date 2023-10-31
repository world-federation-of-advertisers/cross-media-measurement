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
import com.google.protobuf.kotlin.unpack
import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.PublicKeyKey
import org.wfanet.measurement.api.v2alpha.PublicKeysGrpcKt.PublicKeysCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.UpdatePublicKeyRequest
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.updatePublicKeyRequest

class PublicKeysService(private val internalPublicKeysStub: PublicKeysCoroutineStub) :
  PublicKeysCoroutineImplBase() {

  private enum class Permission {
    UPDATE;

    fun deniedStatus(name: String): Status =
      Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)"
      )
  }

  override suspend fun updatePublicKey(request: UpdatePublicKeyRequest): PublicKey {
    val publicKeyKey =
      grpcRequireNotNull(PublicKeyKey.fromName(request.publicKey.name)) {
        "Resource name is either unspecified or invalid"
      }

    val authenticatedPrincipal: MeasurementPrincipal = principalFromCurrentContext
    if (authenticatedPrincipal.resourceKey != publicKeyKey.parentKey) {
      throw Permission.UPDATE.deniedStatus(request.publicKey.name).asRuntimeException()
    }

    val certificateKey =
      grpcRequireNotNull(CertificateKey.fromName(request.publicKey.certificate)) {
        "Certificate name is either unspecified or invalid"
      }

    grpcRequire(certificateKey.parentKey == publicKeyKey.parentKey) {
      "Resource name does not have same parent as Certificate name"
    }
    grpcRequire(request.publicKey.hasPublicKey()) { "public_key.public_key unspecified" }
    try {
      request.publicKey.publicKey.message.unpack<EncryptionPublicKey>()
    } catch (e: InvalidProtocolBufferException) {
      throw Status.INVALID_ARGUMENT.withCause(e)
        .withDescription(
          "public_key.public_key.message does not contain a valid EncryptionPublicKey"
        )
        .asRuntimeException()
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
        else -> error("Unhandled CertificateKey type")
      }
      apiVersion = API_VERSION.string
      publicKey = request.publicKey.publicKey.message.value
      publicKeySignature = request.publicKey.publicKey.signature
      publicKeySignatureAlgorithmOid = request.publicKey.publicKey.signatureAlgorithmOid
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

  companion object {
    private val API_VERSION = Version.V2_ALPHA
  }
}
