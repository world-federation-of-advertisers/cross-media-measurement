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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.toList
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
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
import org.wfanet.measurement.api.v2alpha.GetCertificateRequest
import org.wfanet.measurement.api.v2alpha.ListCertificatesPageToken
import org.wfanet.measurement.api.v2alpha.ListCertificatesPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListCertificatesRequest
import org.wfanet.measurement.api.v2alpha.ListCertificatesResponse
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
import org.wfanet.measurement.api.v2alpha.listCertificatesPageToken
import org.wfanet.measurement.api.v2alpha.listCertificatesResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Certificate as InternalCertificate
import org.wfanet.measurement.internal.kingdom.Certificate.RevocationState as InternalRevocationState
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamCertificatesRequest
import org.wfanet.measurement.internal.kingdom.StreamCertificatesRequestKt
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.getCertificateRequest
import org.wfanet.measurement.internal.kingdom.releaseCertificateHoldRequest
import org.wfanet.measurement.internal.kingdom.revokeCertificateRequest
import org.wfanet.measurement.internal.kingdom.streamCertificatesRequest

class CertificatesService(
  private val internalCertificatesStub: CertificatesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : CertificatesCoroutineImplBase(coroutineContext) {

  private enum class Permission {
    GET,
    LIST,
    CREATE,
    REVOKE,
    RELEASE_HOLD,
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
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalCertificate.toCertificate()
  }

  override suspend fun listCertificates(
    request: ListCertificatesRequest
  ): ListCertificatesResponse {
    grpcRequire(request.pageSize >= 0) { "page_size must be >= 0" }

    val parentKey: CertificateParentKey =
      parseCertificateParentKey(request.parent)
        ?: throw Status.INVALID_ARGUMENT.withDescription("parent not specified or invalid")
          .asRuntimeException()
    if (principalFromCurrentContext.resourceKey != parentKey) {
      throw permissionDeniedStatus(Permission.LIST, "${request.parent}/certificates")
        .asRuntimeException()
    }

    val pageToken: ListCertificatesPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        ListCertificatesPageToken.parseFrom(request.pageToken.base64UrlDecode())
      }
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
    val internalRequest = streamCertificatesRequest {
      filter =
        StreamCertificatesRequestKt.filter {
          when (parentKey) {
            is DataProviderKey -> {
              externalDataProviderId = ApiId(parentKey.dataProviderId).externalId.value
            }
            is DuchyKey -> {
              externalDuchyId = parentKey.duchyId
            }
            is MeasurementConsumerKey -> {
              externalMeasurementConsumerId =
                ApiId(parentKey.measurementConsumerId).externalId.value
            }
            is ModelProviderKey -> {
              externalModelProviderId = ApiId(parentKey.modelProviderId).externalId.value
            }
          }
          subjectKeyIdentifiers += request.filter.subjectKeyIdentifiersList

          if (pageToken != null) {
            val pageTokenParent = pageToken.parentKey
            if (
              pageTokenParent.externalMeasurementConsumerId != externalMeasurementConsumerId ||
                pageTokenParent.externalDataProviderId != externalDataProviderId ||
                pageTokenParent.externalDuchyId != externalDuchyId ||
                pageTokenParent.externalModelProviderId != externalModelProviderId ||
                pageToken.subjectKeyIdentifiersList != subjectKeyIdentifiers
            ) {
              throw Status.INVALID_ARGUMENT.withDescription(
                  "Arguments other than page_size must remain the same for subsequent page requests"
                )
                .asRuntimeException()
            }
            after = pageToken.lastCertificate.toOrderedKey()
          }
        }
      limit = pageSize + 1
    }

    val internalCertificates: List<InternalCertificate> =
      try {
        internalCertificatesStub.streamCertificates(internalRequest).toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    if (internalCertificates.isEmpty()) {
      return ListCertificatesResponse.getDefaultInstance()
    }

    val internalCertificateSubList =
      internalCertificates.subList(0, internalCertificates.size.coerceAtMost(pageSize))
    return listCertificatesResponse {
      certificates += internalCertificateSubList.map(InternalCertificate::toCertificate)
      if (internalCertificates.size > pageSize) {
        nextPageToken =
          buildNextPageToken(internalRequest.filter, internalCertificateSubList.last())
            .toByteString()
            .base64UrlEncode()
      }
    }
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
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.ALREADY_EXISTS -> Status.ALREADY_EXISTS
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
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
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
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
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalCertificate.toCertificate()
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000

    private val CERTIFICATE_KEY_PARSERS: List<(String) -> CertificateKey?> =
      listOf(
        DataProviderCertificateKey::fromName,
        DuchyCertificateKey::fromName,
        MeasurementConsumerCertificateKey::fromName,
        ModelProviderCertificateKey::fromName,
      )
    private val CERTIFICATE_PARENT_KEY_PARSERS: List<(String) -> CertificateParentKey?> =
      listOf(
        DataProviderKey::fromName,
        DuchyKey::fromName,
        MeasurementConsumerKey::fromName,
        ModelProviderKey::fromName,
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
     * * A DataProvider certificate can be read by any MeasurementConsumer, ModelProvider,
     * * or Duchy principal.
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
          this is MeasurementConsumerPrincipal ||
            this is ModelProviderPrincipal ||
            this is DuchyPrincipal
        is MeasurementConsumerCertificateKey -> this is DataProviderPrincipal
      }
    }

    private fun permissionDeniedStatus(permission: Permission, name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $permission denied on resource $name (or it might not exist)"
      )
    }

    private fun ListCertificatesPageToken.PreviousPageEnd.toOrderedKey() =
      StreamCertificatesRequestKt.orderedKey {
        notValidBefore = this@toOrderedKey.notValidBefore

        externalCertificateId = this@toOrderedKey.externalCertificateId

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
        when (parentKey.externalParentIdCase) {
          ListCertificatesPageToken.ParentKey.ExternalParentIdCase.EXTERNAL_DATA_PROVIDER_ID -> {
            externalDataProviderId = parentKey.externalDataProviderId
          }
          ListCertificatesPageToken.ParentKey.ExternalParentIdCase
            .EXTERNAL_MEASUREMENT_CONSUMER_ID -> {
            externalMeasurementConsumerId = parentKey.externalMeasurementConsumerId
          }
          ListCertificatesPageToken.ParentKey.ExternalParentIdCase.EXTERNAL_DUCHY_ID -> {
            externalDuchyId = parentKey.externalDuchyId
          }
          ListCertificatesPageToken.ParentKey.ExternalParentIdCase.EXTERNAL_MODEL_PROVIDER_ID -> {
            externalModelProviderId = parentKey.externalModelProviderId
          }
          ListCertificatesPageToken.ParentKey.ExternalParentIdCase.EXTERNALPARENTID_NOT_SET ->
            error("external_parent_id not set")
        }
      }

    private fun buildNextPageToken(
      internalFilter: StreamCertificatesRequest.Filter,
      lastCertificate: InternalCertificate,
    ) = listCertificatesPageToken {
      parentKey =
        ListCertificatesPageTokenKt.parentKey {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
          when (internalFilter.parentCase) {
            StreamCertificatesRequest.Filter.ParentCase.EXTERNAL_DATA_PROVIDER_ID -> {
              externalDataProviderId = internalFilter.externalDataProviderId
            }
            StreamCertificatesRequest.Filter.ParentCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> {
              externalMeasurementConsumerId = internalFilter.externalMeasurementConsumerId
            }
            StreamCertificatesRequest.Filter.ParentCase.EXTERNAL_DUCHY_ID -> {
              externalDuchyId = internalFilter.externalDuchyId
            }
            StreamCertificatesRequest.Filter.ParentCase.EXTERNAL_MODEL_PROVIDER_ID -> {
              externalModelProviderId = internalFilter.externalModelProviderId
            }
            StreamCertificatesRequest.Filter.ParentCase.PARENT_NOT_SET ->
              throw IllegalArgumentException("parent not set")
          }
        }
      subjectKeyIdentifiers += internalFilter.subjectKeyIdentifiersList

      this.lastCertificate =
        ListCertificatesPageTokenKt.previousPageEnd {
          notValidBefore = lastCertificate.notValidBefore
          externalCertificateId = lastCertificate.externalCertificateId
          parentKey = this@listCertificatesPageToken.parentKey
        }
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
            certificateApiId,
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

  val source = this
  return certificate {
    this.name = name
    x509Der = source.details.x509Der
    revocationState = source.revocationState.toRevocationState()
    subjectKeyIdentifier = source.subjectKeyIdentifier
  }
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
