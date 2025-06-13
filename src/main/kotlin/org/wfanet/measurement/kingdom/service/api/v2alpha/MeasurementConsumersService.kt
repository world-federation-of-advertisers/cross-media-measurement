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
import com.google.protobuf.any
import com.google.protobuf.kotlin.unpack
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.accountFromCurrentContext
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AddMeasurementConsumerOwnerRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.RemoveMeasurementConsumerOwnerRequest
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer as InternalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.addMeasurementConsumerOwnerRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer as internalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.measurementConsumerDetails
import org.wfanet.measurement.internal.kingdom.removeMeasurementConsumerOwnerRequest

private val API_VERSION = Version.V2_ALPHA

class MeasurementConsumersService(
  private val internalClient: InternalMeasurementConsumersCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MeasurementConsumersCoroutineService(coroutineContext) {

  override suspend fun createMeasurementConsumer(
    request: CreateMeasurementConsumerRequest
  ): MeasurementConsumer {
    val account = accountFromCurrentContext

    val measurementConsumer = request.measurementConsumer

    try {
      measurementConsumer.publicKey.message.unpack<EncryptionPublicKey>()
    } catch (e: InvalidProtocolBufferException) {
      throw Status.INVALID_ARGUMENT.withCause(e)
        .withDescription(
          "measurement_consumer.public_key.message must contain an EncryptionPublicKey"
        )
        .asRuntimeException()
    }
    grpcRequire(!measurementConsumer.publicKey.signature.isEmpty) {
      "measurement_consumer.public_key.signature is unspecified"
    }
    grpcRequire(measurementConsumer.measurementConsumerCreationToken.isNotEmpty()) {
      "measurement_consumer.measurement_consumer_creation_token is unspecified"
    }

    val createRequest = createMeasurementConsumerRequest {
      this.measurementConsumer = internalMeasurementConsumer {
        certificate = parseCertificateDer(measurementConsumer.certificateDer)
        details = measurementConsumerDetails {
          apiVersion = API_VERSION.string
          publicKey = measurementConsumer.publicKey.message.value
          publicKeySignature = measurementConsumer.publicKey.signature
          publicKeySignatureAlgorithmOid = measurementConsumer.publicKey.signatureAlgorithmOid
        }
      }
      externalAccountId = account.externalAccountId
      measurementConsumerCreationTokenHash =
        Hashing.hashSha256(apiIdToExternalId(measurementConsumer.measurementConsumerCreationToken))
    }

    val internalMeasurementConsumer =
      try {
        internalClient.createMeasurementConsumer(createRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          Status.Code.PERMISSION_DENIED -> Status.PERMISSION_DENIED
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return internalMeasurementConsumer.toMeasurementConsumer()
  }

  override suspend fun getMeasurementConsumer(
    request: GetMeasurementConsumerRequest
  ): MeasurementConsumer {
    val principal: MeasurementPrincipal = principalFromCurrentContext

    val key: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
    when (principal) {
      is DataProviderPrincipal -> {}
      is MeasurementConsumerPrincipal -> {
        if (
          apiIdToExternalId(principal.resourceKey.measurementConsumerId) !=
            externalMeasurementConsumerId
        ) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot get another MeasurementConsumer" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) {
          "Caller does not have permission to get MeasurementConsumers"
        }
      }
    }

    val getRequest = getMeasurementConsumerRequest {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
    }
    val internalResponse =
      try {
        internalClient.getMeasurementConsumer(getRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return internalResponse.toMeasurementConsumer()
  }

  override suspend fun addMeasurementConsumerOwner(
    request: AddMeasurementConsumerOwnerRequest
  ): MeasurementConsumer {
    val account = accountFromCurrentContext

    val measurementConsumerKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerKey.measurementConsumerId)

    if (!account.externalOwnedMeasurementConsumerIdsList.contains(externalMeasurementConsumerId)) {
      failGrpc(Status.PERMISSION_DENIED) { "Account doesn't own MeasurementConsumer" }
    }

    val accountKey: AccountKey =
      grpcRequireNotNull(AccountKey.fromName(request.account)) { "Account unspecified or invalid" }

    val internalAddMeasurementConsumerOwnerRequest = addMeasurementConsumerOwnerRequest {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalAccountId = apiIdToExternalId(accountKey.accountId)
    }

    val internalMeasurementConsumer =
      try {
        internalClient.addMeasurementConsumerOwner(internalAddMeasurementConsumerOwnerRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalMeasurementConsumer.toMeasurementConsumer()
  }

  override suspend fun removeMeasurementConsumerOwner(
    request: RemoveMeasurementConsumerOwnerRequest
  ): MeasurementConsumer {
    val account = accountFromCurrentContext

    val measurementConsumerKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    val externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerKey.measurementConsumerId)

    if (!account.externalOwnedMeasurementConsumerIdsList.contains(externalMeasurementConsumerId)) {
      failGrpc(Status.PERMISSION_DENIED) { "Account doesn't own MeasurementConsumer" }
    }

    val accountKey: AccountKey =
      grpcRequireNotNull(AccountKey.fromName(request.account)) { "Account unspecified or invalid" }

    val internalRemoveMeasurementConsumerOwnerRequest = removeMeasurementConsumerOwnerRequest {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalAccountId = apiIdToExternalId(accountKey.accountId)
    }

    val internalMeasurementConsumer =
      try {
        internalClient.removeMeasurementConsumerOwner(internalRemoveMeasurementConsumerOwnerRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }
    return internalMeasurementConsumer.toMeasurementConsumer()
  }
}

private fun InternalMeasurementConsumer.toMeasurementConsumer(): MeasurementConsumer {
  val measurementConsumerId: String = externalIdToApiId(externalMeasurementConsumerId)
  val certificateId: String = externalIdToApiId(certificate.externalCertificateId)
  val apiVersion = Version.fromString(details.apiVersion)

  val source = this
  return measurementConsumer {
    name = MeasurementConsumerKey(measurementConsumerId).toName()
    certificate = MeasurementConsumerCertificateKey(measurementConsumerId, certificateId).toName()
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
  }
}
