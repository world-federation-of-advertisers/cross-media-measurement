// Copyright 2020 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.Certificate.RevocationState
import org.wfanet.measurement.api.v2alpha.CertificateKey
import org.wfanet.measurement.api.v2alpha.CertificateParentKey
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CreateCertificateRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.GetCertificateRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ReleaseCertificateHoldRequest
import org.wfanet.measurement.api.v2alpha.RevokeCertificateRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.Certificate.RevocationState as InternalRevocationState
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.getCertificateRequest
import org.wfanet.measurement.internal.kingdom.releaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest

class CertificatesService(private val internalCertificatesStub: CertificatesCoroutineStub) :
  CertificatesCoroutineImplBase() {

  private enum class Permission {
    GET,
    CREATE,
    REVOKE,
    RELEASE_HOLD
  }

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    val key: CertificateKey =
      grpcRequireNotNull(parseCertificateKey(request.name)) {
        "Resource name unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (!principal.isAuthorizedToGet(key)) {
      throw permissionDeniedStatus(Permission.GET, request.name).asRuntimeException()
    }

    val internalGetCertificateRequest = getCertificateRequest {
      externalCertificateId = ApiId(key.certificateId).externalId.value
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = ApiId(key.dataProviderId).externalId.value
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = ApiId(key.measurementConsumerId).externalId.value
        }
        is ModelProviderCertificateKey -> {
          externalModelProviderId = ApiId(key.modelProviderId).externalId.value
        }
      }
    }

    val internalCertificate =
      try {
        internalCertificatesStub.getCertificate(internalGetCertificateRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.INVALID_ARGUMENT ->
            failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }
    return internalCertificate.toCertificate()
  }

  override suspend fun createCertificate(request: CreateCertificateRequest): Certificate {
    val parentKey: CertificateParentKey =
      parseCertificateParentKey(request.parent)
        ?: throw Status.INVALID_ARGUMENT.withDescription("parent not specified or invalid")
          .asRuntimeException()

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != parentKey) {
      throw permissionDeniedStatus(Permission.CREATE, "${request.parent}/certificates")
        .asRuntimeException()
    }

    val internalCertificate = internalCertificate {
      fillCertificateFromDer(request.certificate.x509Der)
      when (parentKey) {
        is DataProviderKey -> {
          externalDataProviderId = ApiId(parentKey.dataProviderId).externalId.value
        }
        is DuchyKey -> {
          externalDuchyId = parentKey.duchyId
        }
        is MeasurementConsumerKey -> {
          externalMeasurementConsumerId = ApiId(parentKey.measurementConsumerId).externalId.value
        }
        is ModelProviderKey -> {
          externalModelProviderId = ApiId(parentKey.modelProviderId).externalId.value
        }
      }
    }

    val response =
      try {
        internalCertificatesStub.createCertificate(internalCertificate)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "Not found" }
          Status.Code.ALREADY_EXISTS ->
            failGrpc(Status.ALREADY_EXISTS, ex) {
              "Certificate with the subject key identifier (SKID) already exists."
            }
          Status.Code.INVALID_ARGUMENT ->
            failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }
    return response.toCertificate()
  }

  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    val key: CertificateKey =
      grpcRequireNotNull(parseCertificateKey(request.name)) {
        "Resource name unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != key.parentKey) {
      throw permissionDeniedStatus(Permission.REVOKE, request.name).asRuntimeException()
    }

    grpcRequire(request.revocationState != RevocationState.REVOCATION_STATE_UNSPECIFIED) {
      "Revocation State unspecified"
    }

    val internalRevokeCertificateRequest = revokeCertificateRequest {
      externalCertificateId = ApiId(key.certificateId).externalId.value
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = ApiId(key.dataProviderId).externalId.value
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = ApiId(key.measurementConsumerId).externalId.value
        }
        is ModelProviderCertificateKey -> {
          externalModelProviderId = ApiId(key.modelProviderId).externalId.value
        }
      }
      revocationState = request.revocationState.toInternal()
    }

    val internalCertificate =
      try {
        internalCertificatesStub.revokeCertificate(internalRevokeCertificateRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "Not found" }
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { "Certificate is in wrong State." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }
    return internalCertificate.toCertificate()
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    val key: CertificateKey =
      grpcRequireNotNull(parseCertificateKey(request.name)) {
        "Resource name unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != key.parentKey) {
      throw permissionDeniedStatus(Permission.RELEASE_HOLD, request.name).asRuntimeException()
    }

    val internalReleaseCertificateHoldRequest = releaseCertificateHoldRequest {
      externalCertificateId = ApiId(key.certificateId).externalId.value
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = ApiId(key.dataProviderId).externalId.value
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = ApiId(key.measurementConsumerId).externalId.value
        }
        is ModelProviderCertificateKey -> {
          externalModelProviderId = ApiId(key.modelProviderId).externalId.value
        }
      }
    }

    val internalCertificate =
      try {
        internalCertificatesStub.releaseCertificateHold(internalReleaseCertificateHoldRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { ex.message ?: "Not found" }
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { "Certificate is in wrong State." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }
    return internalCertificate.toCertificate()
  }

  companion object {
    private val CERTIFICATE_KEY_PARSERS: List<(String) -> CertificateKey?> =
      listOf(
        DataProviderCertificateKey::fromName,
        DuchyCertificateKey::fromName,
        MeasurementConsumerCertificateKey::fromName,
        ModelProviderCertificateKey::fromName
      )
    private val CERTIFICATE_PARENT_KEY_PARSERS: List<(String) -> CertificateParentKey?> =
      listOf(
        DataProviderKey::fromName,
        DuchyKey::fromName,
        MeasurementConsumerKey::fromName,
        ModelProviderKey::fromName
      )

    /**
     * Checks the resource name against multiple certificate [ResourceKey]s to find the right one.
     */
    private fun parseCertificateKey(name: String): CertificateKey? {
      for (parse in CERTIFICATE_KEY_PARSERS) {
        return parse(name) ?: continue
      }
      return null
    }

    private fun parseCertificateParentKey(name: String): CertificateParentKey? {
      for (parse in CERTIFICATE_PARENT_KEY_PARSERS) {
        return parse(name) ?: continue
      }
      return null
    }

    /**
     * Returns whether this [MeasurementPrincipal] is authorized to get [certificateKey].
     *
     * The rules are as follows:
     * * Any Certificate can be read by a principal that maps to its parent resource.
     * * A ModelProvider or Duchy Certificate can be read by any authenticated principal.
     * * A DataProvider certificate can be read by any MeasurementConsumer or ModelProvider
     *   principal.
     * * A MeasurementConsumer certificate can be read by any DataProvider principal.
     */
    private fun MeasurementPrincipal.isAuthorizedToGet(certificateKey: CertificateKey): Boolean {
      if (resourceKey == certificateKey.parentKey) {
        return true
      }

      return when (certificateKey) {
        is DuchyCertificateKey,
        is ModelProviderCertificateKey -> true
        is DataProviderCertificateKey ->
          this is MeasurementConsumerPrincipal || this is ModelProviderPrincipal
        is MeasurementConsumerCertificateKey -> this is DataProviderPrincipal
      }
    }

    private fun permissionDeniedStatus(permission: Permission, name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $permission denied on resource $name (or it might not exist)"
      )
    }
  }
}

/** Converts an internal [InternalCertificate] to a public [Certificate]. */
private fun InternalCertificate.toCertificate(): Certificate {
  val certificateApiId = externalIdToApiId(externalCertificateId)

  val name =
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (parentCase) {
      InternalCertificate.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID ->
        MeasurementConsumerCertificateKey(
            externalIdToApiId(externalMeasurementConsumerId),
            certificateApiId
          )
          .toName()
      InternalCertificate.ParentCase.EXTERNAL_DATA_PROVIDER_ID ->
        DataProviderCertificateKey(externalIdToApiId(externalDataProviderId), certificateApiId)
          .toName()
      InternalCertificate.ParentCase.EXTERNAL_DUCHY_ID ->
        DuchyCertificateKey(externalDuchyId, certificateApiId).toName()
      InternalCertificate.ParentCase.EXTERNAL_MODEL_PROVIDER_ID ->
        ModelProviderCertificateKey(externalIdToApiId(externalModelProviderId), certificateApiId)
          .toName()
      InternalCertificate.ParentCase.PARENT_NOT_SET ->
        failGrpc(Status.INTERNAL) { "Parent missing" }
    }

  return certificate {
    this.name = name
    x509Der = this@toCertificate.details.x509Der
    revocationState = this@toCertificate.revocationState.toRevocationState()
  }
}

/** Converts an internal [InternalRevocationState] to a public [RevocationState]. */
private fun InternalRevocationState.toRevocationState(): RevocationState =
  when (this) {
    InternalRevocationState.REVOKED -> RevocationState.REVOKED
    InternalRevocationState.HOLD -> RevocationState.HOLD
    InternalRevocationState.UNRECOGNIZED,
    InternalRevocationState.REVOCATION_STATE_UNSPECIFIED ->
      RevocationState.REVOCATION_STATE_UNSPECIFIED
  }

/** Converts a public [RevocationState] to an internal [InternalRevocationState]. */
private fun RevocationState.toInternal(): InternalRevocationState =
  when (this) {
    RevocationState.REVOKED -> InternalRevocationState.REVOKED
    RevocationState.HOLD -> InternalRevocationState.HOLD
    RevocationState.UNRECOGNIZED,
    RevocationState.REVOCATION_STATE_UNSPECIFIED ->
      InternalRevocationState.REVOCATION_STATE_UNSPECIFIED
  }
