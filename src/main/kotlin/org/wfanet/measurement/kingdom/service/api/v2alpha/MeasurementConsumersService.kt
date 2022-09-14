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
import org.wfanet.measurement.api.accountFromCurrentContext
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AddMeasurementConsumerOwnerRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
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
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer as InternalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.addMeasurementConsumerOwnerRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.getMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.measurementConsumer as internalMeasurementConsumer
import org.wfanet.measurement.internal.kingdom.removeMeasurementConsumerOwnerRequest

private val API_VERSION = Version.V2_ALPHA

class MeasurementConsumersService(
  private val internalClient: InternalMeasurementConsumersCoroutineStub
) : MeasurementConsumersCoroutineService() {

  override suspend fun createMeasurementConsumer(
    request: CreateMeasurementConsumerRequest
  ): MeasurementConsumer {
    val account = accountFromCurrentContext

    val measurementConsumer = request.measurementConsumer

    grpcRequire(!measurementConsumer.publicKey.data.isEmpty) { "public_key.data is missing" }
    grpcRequire(!measurementConsumer.publicKey.signature.isEmpty) {
      "public_key.signature is missing"
    }
    grpcRequire(measurementConsumer.measurementConsumerCreationToken.isNotBlank()) {
      "Measurement Consumer creation token is unspecified"
    }

    val createRequest = createMeasurementConsumerRequest {
      this.measurementConsumer = internalMeasurementConsumer {
        certificate = parseCertificateDer(measurementConsumer.certificateDer)
        details = details {
          apiVersion = API_VERSION.string
          publicKey = measurementConsumer.publicKey.data
          publicKeySignature = measurementConsumer.publicKey.signature
        }
      }
      externalAccountId = account.externalAccountId
      measurementConsumerCreationTokenHash =
        hashSha256(apiIdToExternalId(measurementConsumer.measurementConsumerCreationToken))
    }

    val internalMeasurementConsumer =
      try {
        internalClient.createMeasurementConsumer(createRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.INVALID_ARGUMENT ->
            failGrpc(Status.INVALID_ARGUMENT, ex) { "Required field unspecified or invalid." }
          Status.Code.PERMISSION_DENIED ->
            failGrpc(Status.PERMISSION_DENIED, ex) {
              "Measurement Consumer creation token is not valid."
            }
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { "Account not found or inactivated." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
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
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND ->
            failGrpc(Status.NOT_FOUND, ex) { "MeasurementConsumer not found" }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
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
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { "Account not found." }
          Status.Code.NOT_FOUND ->
            failGrpc(Status.NOT_FOUND, ex) { "MeasurementConsumer not found." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
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
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) {
              "Account not found or Account doesn't own the MeasurementConsumer."
            }
          Status.Code.NOT_FOUND ->
            failGrpc(Status.NOT_FOUND, ex) { "MeasurementConsumer not found." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }
    return internalMeasurementConsumer.toMeasurementConsumer()
  }
}

private fun InternalMeasurementConsumer.toMeasurementConsumer(): MeasurementConsumer {
  check(Version.fromString(details.apiVersion) == API_VERSION) {
    "Incompatible API version ${details.apiVersion}"
  }
  val internalMeasurementConsumer = this
  val measurementConsumerId: String = externalIdToApiId(externalMeasurementConsumerId)
  val certificateId: String = externalIdToApiId(certificate.externalCertificateId)

  return measurementConsumer {
    name = MeasurementConsumerKey(measurementConsumerId).toName()
    certificate = MeasurementConsumerCertificateKey(measurementConsumerId, certificateId).toName()
    certificateDer = internalMeasurementConsumer.certificate.details.x509Der
    publicKey = signedData {
      data = internalMeasurementConsumer.details.publicKey
      signature = internalMeasurementConsumer.details.publicKeySignature
    }
  }
}
