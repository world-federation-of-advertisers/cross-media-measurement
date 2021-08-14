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
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.Certificate.RevocationState
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CreateCertificateRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.GetCertificateRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ReleaseCertificateHoldRequest
import org.wfanet.measurement.api.v2alpha.RevokeCertificateRequest
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.Certificate.RevocationState as InternalRevocationState
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.GetCertificateRequest as InternalGetCertificateRequest
import org.wfanet.measurement.internal.kingdom.ReleaseCertificateHoldRequest as InternalReleaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.RevokeCertificateRequest as InternalRevokeCertificateRequest

class CertificatesService(private val internalCertificatesStub: CertificatesCoroutineStub) :
  CertificatesCoroutineImplBase() {

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    val key =
      grpcRequireNotNull(createResourceKey(request.name)) { "Resource name unspecified or invalid" }

    val internalGetCertificateRequest = buildInternalGetCertificateRequest {
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
      }
    }

    return internalCertificatesStub.getCertificate(internalGetCertificateRequest).toCertificate()
  }

  override suspend fun createCertificate(request: CreateCertificateRequest): Certificate {
    val dataProviderKey = DataProviderKey.fromName(request.parent)
    val duchyKey = DuchyKey.fromName(request.parent)
    val measurementConsumerKey = MeasurementConsumerKey.fromName(request.parent)

    val internalCertificate =
      parseCertificateDer(request.certificate.x509Der)
        .toBuilder()
        .apply {
          when {
            dataProviderKey != null ->
              externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
            duchyKey != null -> externalDuchyId = duchyKey.duchyId
            measurementConsumerKey != null ->
              externalMeasurementConsumerId =
                apiIdToExternalId(measurementConsumerKey.measurementConsumerId)
            else -> failGrpc(Status.INVALID_ARGUMENT) { "Parent unspecified or invalid" }
          }
        }
        .build()

    return internalCertificatesStub.createCertificate(internalCertificate).toCertificate()
  }

  override suspend fun revokeCertificate(request: RevokeCertificateRequest): Certificate {
    val key =
      grpcRequireNotNull(createResourceKey(request.name)) { "Resource name unspecified or invalid" }

    grpcRequire(request.revocationState != RevocationState.REVOCATION_STATE_UNSPECIFIED) {
      "Revocation State unspecified"
    }

    val internalRevokeCertificateRequest = buildInternalRevokeCertificateRequest {
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
      }
      revocationState = request.revocationState.toInternal()
    }

    return internalCertificatesStub
      .revokeCertificate(internalRevokeCertificateRequest)
      .toCertificate()
  }

  override suspend fun releaseCertificateHold(request: ReleaseCertificateHoldRequest): Certificate {
    val key =
      grpcRequireNotNull(createResourceKey(request.name)) { "Resource name unspecified or invalid" }

    val internalReleaseCertificateHoldRequest = buildInternalReleaseCertificateHoldRequest {
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(key.certificateId)
        }
      }
    }

    return internalCertificatesStub
      .releaseCertificateHold(internalReleaseCertificateHoldRequest)
      .toCertificate()
  }
}

/** Converts an internal [InternalCertificate] to a public [Certificate]. */
private fun InternalCertificate.toCertificate(): Certificate {
  return buildCertificate {
    name =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (this@toCertificate.parentCase) {
        InternalCertificate.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID ->
          MeasurementConsumerCertificateKey(
              measurementConsumerId =
                externalIdToApiId(this@toCertificate.externalMeasurementConsumerId),
              certificateId = externalIdToApiId(this@toCertificate.externalCertificateId)
            )
            .toName()
        InternalCertificate.ParentCase.EXTERNAL_DATA_PROVIDER_ID ->
          DataProviderCertificateKey(
              dataProviderId = externalIdToApiId(this@toCertificate.externalDataProviderId),
              certificateId = externalIdToApiId(this@toCertificate.externalCertificateId)
            )
            .toName()
        InternalCertificate.ParentCase.EXTERNAL_DUCHY_ID ->
          DuchyCertificateKey(
              duchyId = this@toCertificate.externalDuchyId,
              certificateId = externalIdToApiId(this@toCertificate.externalCertificateId)
            )
            .toName()
        InternalCertificate.ParentCase.PARENT_NOT_SET ->
          failGrpc(Status.INTERNAL) { "Parent missing" }
      }
    x509Der = this@toCertificate.details.x509Der
    revocationState = this@toCertificate.revocationState.toRevocationState()
  }
}

internal inline fun buildCertificate(fill: (@Builder Certificate.Builder).() -> Unit) =
  Certificate.newBuilder().apply(fill).build()

/** Converts an internal [InternalRevocationState] to a public [RevocationState]. */
private fun InternalRevocationState.toRevocationState(): RevocationState =
  when (this) {
    InternalRevocationState.REVOKED -> RevocationState.REVOKED
    InternalRevocationState.HOLD -> RevocationState.HOLD
    InternalRevocationState.UNRECOGNIZED, InternalRevocationState.REVOCATION_STATE_UNSPECIFIED ->
      RevocationState.REVOCATION_STATE_UNSPECIFIED
  }

/** Converts a public [RevocationState] to an internal [InternalRevocationState]. */
private fun RevocationState.toInternal(): InternalRevocationState =
  when (this) {
    RevocationState.REVOKED -> InternalRevocationState.REVOKED
    RevocationState.HOLD -> InternalRevocationState.HOLD
    RevocationState.UNRECOGNIZED, RevocationState.REVOCATION_STATE_UNSPECIFIED ->
      InternalRevocationState.REVOCATION_STATE_UNSPECIFIED
  }

internal inline fun buildInternalGetCertificateRequest(
  fill: (@Builder InternalGetCertificateRequest.Builder).() -> Unit
) = InternalGetCertificateRequest.newBuilder().apply(fill).build()

internal inline fun buildInternalRevokeCertificateRequest(
  fill: (@Builder InternalRevokeCertificateRequest.Builder).() -> Unit
) = InternalRevokeCertificateRequest.newBuilder().apply(fill).build()

internal inline fun buildInternalReleaseCertificateHoldRequest(
  fill: (@Builder InternalReleaseCertificateHoldRequest.Builder).() -> Unit
) = InternalReleaseCertificateHoldRequest.newBuilder().apply(fill).build()

/** Checks the resource name against multiple certificate [ResourceKey] to find the right one. */
private fun createResourceKey(name: String): Any? {
  val dataProviderCertificateKey = DataProviderCertificateKey.fromName(name)
  if (dataProviderCertificateKey != null) {
    return dataProviderCertificateKey
  }
  val duchyCertificateKey = DuchyCertificateKey.fromName(name)
  if (duchyCertificateKey != null) {
    return duchyCertificateKey
  }
  val measurementConsumerCertificateKey = MeasurementConsumerCertificateKey.fromName(name)
  if (measurementConsumerCertificateKey != null) {
    return measurementConsumerCertificateKey
  }
  return null
}
