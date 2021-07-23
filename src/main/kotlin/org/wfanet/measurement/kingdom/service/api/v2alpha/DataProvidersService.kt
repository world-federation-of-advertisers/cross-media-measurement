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

import com.google.protobuf.ByteString
import io.grpc.Status
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CreateDataProviderRequest
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest as InternalGetDataProviderRequest

private val API_VERSION = Version.V2_ALPHA

class DataProvidersService(private val internalClient: DataProvidersCoroutineStub) :
  DataProvidersCoroutineService() {
  override suspend fun createDataProvider(request: CreateDataProviderRequest): DataProvider {
    val dataProvider = request.dataProvider
    grpcRequire(with(dataProvider.publicKey) { !data.isEmpty && !signature.isEmpty }) {
      "public_key is not fully specified"
    }
    grpcRequire(!dataProvider.preferredCertificateDer.isEmpty) {
      "preferred_certificate_der is not specified"
    }

    val x509Certificate: X509Certificate =
      try {
        readCertificate(dataProvider.preferredCertificateDer)
      } catch (e: CertificateException) {
        throw Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription("Cannot parse preferred_certificate_der")
          .asRuntimeException()
      }
    val skid: ByteString =
      grpcRequireNotNull(x509Certificate.subjectKeyIdentifier) {
        "Cannot find Subject Key Identifier of preferred certificate"
      }

    val internalResponse: InternalDataProvider =
      internalClient.createDataProvider(
        buildInternalDataProvider {
          preferredCertificate {
            subjectKeyIdentifier = skid
            notValidBefore = x509Certificate.notBefore.toInstant().toProtoTime()
            notValidAfter = x509Certificate.notAfter.toInstant().toProtoTime()
            detailsBuilder.x509Der = dataProvider.preferredCertificateDer
          }
          details {
            apiVersion = API_VERSION.string
            publicKey = dataProvider.publicKey.data
            publicKeySignature = dataProvider.publicKey.signature
          }
        }
        // TODO(world-federation-of-advertisers/cross-media-measurement#119): Add authenticated user
        // as owner.
        )
    return internalResponse.toDataProvider()
  }

  override suspend fun getDataProvider(request: GetDataProviderRequest): DataProvider {
    val key: DataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }
    // TODO(world-federation-of-advertisers/cross-media-measurement#119): Pass credentials for
    // ownership check.
    val internalResponse: InternalDataProvider =
      internalClient.getDataProvider(
        buildInternalGetDataProviderRequest {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
        }
      )
    return internalResponse.toDataProvider()
  }
}

@DslMarker @Target(AnnotationTarget.CLASS, AnnotationTarget.TYPE) internal annotation class Builder

internal inline fun buildDataProvider(fill: (@Builder DataProvider.Builder).() -> Unit) =
  DataProvider.newBuilder().apply(fill).build()

internal inline fun DataProvider.Builder.publicKey(fill: (@Builder SignedData.Builder).() -> Unit) {
  publicKeyBuilder.apply(fill)
}

internal inline fun buildInternalDataProvider(
  fill: (@Builder InternalDataProvider.Builder).() -> Unit
) = InternalDataProvider.newBuilder().apply(fill).build()

internal inline fun InternalDataProvider.Builder.preferredCertificate(
  fill: (@Builder InternalCertificate.Builder).() -> Unit
) {
  preferredCertificateBuilder.apply(fill)
}

internal inline fun InternalDataProvider.Builder.details(
  fill: (@Builder InternalDataProvider.Details.Builder).() -> Unit
) {
  detailsBuilder.apply(fill)
}

internal inline fun buildInternalGetDataProviderRequest(
  fill: (@Builder InternalGetDataProviderRequest.Builder).() -> Unit
) = InternalGetDataProviderRequest.newBuilder().apply(fill).build()

private fun InternalDataProvider.toDataProvider(): DataProvider {
  check(Version.fromString(details.apiVersion) == API_VERSION) {
    "Incompatible API version ${details.apiVersion}"
  }
  val internalDataProvider = this
  val dataProviderId: String = externalIdToApiId(externalDataProviderId)
  val certificateId: String = externalIdToApiId(preferredCertificate.externalCertificateId)

  return buildDataProvider {
    name = DataProviderKey(dataProviderId).toName()
    preferredCertificate = DataProviderCertificateKey(dataProviderId, certificateId).toName()
    preferredCertificateDer = internalDataProvider.preferredCertificate.details.x509Der
    publicKey {
      data = internalDataProvider.details.publicKey
      signature = internalDataProvider.details.publicKeySignature
    }
  }
}
