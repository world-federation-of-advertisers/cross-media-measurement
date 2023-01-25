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
import org.wfanet.measurement.api.v2alpha.CertificateParentKey
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CreateCertificateRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
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
import org.wfanet.measurement.common.identity.apiIdToExternalId
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

  override suspend fun getCertificate(request: GetCertificateRequest): Certificate {
    val key =
      grpcRequireNotNull(createResourceKey(request.name)) { "Resource name unspecified or invalid" }

    val principal: MeasurementPrincipal = principalFromCurrentContext

    val internalGetCertificateRequest = getCertificateRequest {
      externalCertificateId = apiIdToExternalId(key.certificateId)
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)

          when (principal) {
            is DataProviderPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.dataProviderId) != externalDataProviderId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot get another DataProvider's Certificate"
                }
              }
            }
            is MeasurementConsumerPrincipal -> {}
            is ModelProviderPrincipal -> {}
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to get a DataProvider's Certificate"
              }
            }
          }
        }
        is DuchyCertificateKey -> externalDuchyId = key.duchyId
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)

          when (principal) {
            is DataProviderPrincipal -> {}
            is MeasurementConsumerPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.measurementConsumerId) !=
                  externalMeasurementConsumerId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot get another MeasurementConsumer's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to get a MeasurementConsumer's Certificate"
              }
            }
          }
        }
        is ModelProviderCertificateKey ->
          externalModelProviderId = apiIdToExternalId(key.modelProviderId)
        else -> failGrpc(Status.INTERNAL) { "Unsupported parent: ${key.toName()}" }
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
    val dataProviderKey = DataProviderKey.fromName(request.parent)
    val duchyKey = DuchyKey.fromName(request.parent)
    val measurementConsumerKey = MeasurementConsumerKey.fromName(request.parent)
    val modelProviderKey = ModelProviderKey.fromName(request.parent)

    val principal: MeasurementPrincipal = principalFromCurrentContext

    val internalCertificate = internalCertificate {
      fillCertificateFromDer(request.certificate.x509Der)
      when {
        dataProviderKey != null -> {
          externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)

          when (principal) {
            is DataProviderPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.dataProviderId) != externalDataProviderId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot create another DataProvider's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to create a DataProvider's Certificate"
              }
            }
          }
        }
        duchyKey != null -> {
          externalDuchyId = duchyKey.duchyId

          when (principal) {
            is DuchyPrincipal -> {
              if (principal.resourceKey.duchyId != externalDuchyId) {
                failGrpc(Status.PERMISSION_DENIED) { "Cannot create another Duchy's Certificate" }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to create a Duchy's Certificate"
              }
            }
          }
        }
        measurementConsumerKey != null -> {
          externalMeasurementConsumerId =
            apiIdToExternalId(measurementConsumerKey.measurementConsumerId)

          when (principal) {
            is MeasurementConsumerPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.measurementConsumerId) !=
                  externalMeasurementConsumerId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot create another MeasurementConsumer's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to create a MeasurementConsumer's Certificate"
              }
            }
          }
        }
        modelProviderKey != null -> {
          externalModelProviderId = apiIdToExternalId(modelProviderKey.modelProviderId)

          when (principal) {
            is ModelProviderPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.modelProviderId) != externalModelProviderId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot create another ModelProvider's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to create a ModelProvider's Certificate"
              }
            }
          }
        }
        else -> failGrpc(Status.INVALID_ARGUMENT) { "Parent unspecified or invalid" }
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
    val principal: MeasurementPrincipal = principalFromCurrentContext

    val key =
      grpcRequireNotNull(createResourceKey(request.name)) { "Resource name unspecified or invalid" }

    grpcRequire(request.revocationState != RevocationState.REVOCATION_STATE_UNSPECIFIED) {
      "Revocation State unspecified"
    }

    val internalRevokeCertificateRequest = revokeCertificateRequest {
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)

          when (principal) {
            is DataProviderPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.dataProviderId) != externalDataProviderId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot revoke another DataProvider's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to revoke a DataProvider's Certificate"
              }
            }
          }
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId
          externalCertificateId = apiIdToExternalId(key.certificateId)

          when (principal) {
            is DuchyPrincipal -> {
              if (principal.resourceKey.duchyId != externalDuchyId) {
                failGrpc(Status.PERMISSION_DENIED) { "Cannot revoke another Duchy's Certificate" }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to revoke a Duchy's Certificate"
              }
            }
          }
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
          externalCertificateId = apiIdToExternalId(key.certificateId)

          when (principal) {
            is MeasurementConsumerPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.measurementConsumerId) !=
                  externalMeasurementConsumerId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot revoke another MeasurementConsumer's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to revoke a MeasurementConsumer's Certificate"
              }
            }
          }
        }
        is ModelProviderCertificateKey -> {
          externalModelProviderId = apiIdToExternalId(key.modelProviderId)
          externalCertificateId = apiIdToExternalId(key.certificateId)

          when (principal) {
            is ModelProviderPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.modelProviderId) != externalModelProviderId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot revoke another ModelProvider's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to revoke a ModelProvider's Certificate"
              }
            }
          }
        }
        else -> failGrpc(Status.INVALID_ARGUMENT) { "Parent unspecified or invalid" }
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
    val principal: MeasurementPrincipal = principalFromCurrentContext

    val key =
      grpcRequireNotNull(createResourceKey(request.name)) { "Resource name unspecified or invalid" }

    val internalReleaseCertificateHoldRequest = releaseCertificateHoldRequest {
      externalCertificateId = apiIdToExternalId(key.certificateId)
      when (key) {
        is DataProviderCertificateKey -> {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)

          when (principal) {
            is DataProviderPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.dataProviderId) != externalDataProviderId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot release another DataProvider's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to release a DataProvider's Certificate"
              }
            }
          }
        }
        is DuchyCertificateKey -> {
          externalDuchyId = key.duchyId

          when (principal) {
            is DuchyPrincipal -> {
              if (principal.resourceKey.duchyId != externalDuchyId) {
                failGrpc(Status.PERMISSION_DENIED) { "Cannot release another Duchy's Certificate" }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to release a Duchy's Certificate"
              }
            }
          }
        }
        is MeasurementConsumerCertificateKey -> {
          externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)

          when (principal) {
            is MeasurementConsumerPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.measurementConsumerId) !=
                  externalMeasurementConsumerId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot release another MeasurementConsumer's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to release a MeasurementConsumer's Certificate"
              }
            }
          }
        }
        is ModelProviderCertificateKey -> {
          externalModelProviderId = apiIdToExternalId(key.modelProviderId)

          when (principal) {
            is ModelProviderPrincipal -> {
              if (
                apiIdToExternalId(principal.resourceKey.modelProviderId) != externalModelProviderId
              ) {
                failGrpc(Status.PERMISSION_DENIED) {
                  "Cannot release another ModelProvider's Certificate"
                }
              }
            }
            else -> {
              failGrpc(Status.PERMISSION_DENIED) {
                "Caller does not have permission to release a ModelProvider's Certificate"
              }
            }
          }
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

private val CERTIFICATE_PARENT_KEY_PARSERS: List<(String) -> CertificateParentKey?> =
  listOf(
    DataProviderCertificateKey::fromName,
    DuchyCertificateKey::fromName,
    MeasurementConsumerCertificateKey::fromName,
    ModelProviderCertificateKey::fromName
  )

/** Checks the resource name against multiple certificate [ResourceKey]s to find the right one. */
private fun createResourceKey(name: String): CertificateParentKey? {
  for (parse in CERTIFICATE_PARENT_KEY_PARSERS) {
    return parse(name) ?: continue
  }
  return null
}
