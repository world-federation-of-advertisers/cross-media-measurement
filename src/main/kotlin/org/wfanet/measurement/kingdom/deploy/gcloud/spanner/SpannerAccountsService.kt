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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.AuthenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerCreationTokenResponse
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.GenerateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.GetOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.OpenIdRequestParams
import org.wfanet.measurement.internal.kingdom.ReplaceAccountIdentityRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenResponse
import org.wfanet.measurement.internal.kingdom.openIdRequestParams
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdConnectIdentityReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdRequestParamsReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ActivateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurementConsumerCreationToken
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.GenerateOpenIdRequestParams
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceAccountIdentityWithNewOpenIdConnectIdentity

private const val VALID_SECONDS = 3600L

class SpannerAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : AccountsCoroutineImplBase() {
  override suspend fun createAccount(request: Account): Account {
    try {
      val externalCreatorAccountId: ExternalId? =
        if (request.externalCreatorAccountId != 0L) {
          ExternalId(request.externalCreatorAccountId)
        } else {
          null
        }

      val externalOwnedMeasurementConsumerId: ExternalId? =
        if (request.externalOwnedMeasurementConsumerId != 0L) {
          ExternalId(request.externalOwnedMeasurementConsumerId)
        } else {
          null
        }

      return CreateAccount(
          externalCreatorAccountId = externalCreatorAccountId,
          externalOwnedMeasurementConsumerId = externalOwnedMeasurementConsumerId
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.ACCOUNT_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Creator account not found" }
        ErrorCode.PERMISSION_DENIED ->
          failGrpc(Status.PERMISSION_DENIED) {
            "Caller does not own the owned measurement consumer"
          }
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_NOT_FOUND_BY_DATA_PROVIDER,
        ErrorCode.REQUISITION_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.DUCHY_NOT_FOUND,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_MEASUREMENT_CONSUMER,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.DATA_PROVIDER_CERTIFICATE_NOT_FOUND,
        ErrorCode.MEASUREMENT_CONSUMER_CERTIFICATE_NOT_FOUND,
        ErrorCode.DUCHY_CERTIFICATE_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_MEASUREMENT,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun createMeasurementConsumerCreationToken(
    request: CreateMeasurementConsumerCreationTokenRequest
  ): CreateMeasurementConsumerCreationTokenResponse {
    return createMeasurementConsumerCreationTokenResponse {
      measurementConsumerCreationToken =
        CreateMeasurementConsumerCreationToken().execute(client, idGenerator)
    }
  }

  override suspend fun activateAccount(request: ActivateAccountRequest): Account {
    try {
      return ActivateAccount(
          externalAccountId = ExternalId(request.externalAccountId),
          activationToken = ExternalId(request.activationToken),
          issuer = request.identity.issuer,
          subject = request.identity.subject
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.PERMISSION_DENIED ->
          failGrpc(Status.PERMISSION_DENIED) { "Activation token is not valid for this account" }
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY ->
          failGrpc(Status.INVALID_ARGUMENT) { "Issuer and subject pair already exists" }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL ->
          failGrpc(Status.PERMISSION_DENIED) { "Cannot activate an account again" }
        ErrorCode.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Account to activate has not been found" }
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.REQUISITION_NOT_FOUND_BY_DATA_PROVIDER,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.DUCHY_NOT_FOUND,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_MEASUREMENT_CONSUMER,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.DATA_PROVIDER_CERTIFICATE_NOT_FOUND,
        ErrorCode.MEASUREMENT_CONSUMER_CERTIFICATE_NOT_FOUND,
        ErrorCode.DUCHY_CERTIFICATE_NOT_FOUND,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_MEASUREMENT,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    try {
      return ReplaceAccountIdentityWithNewOpenIdConnectIdentity(
          externalAccountId = ExternalId(request.externalAccountId),
          issuer = request.identity.issuer,
          subject = request.identity.subject
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY ->
          failGrpc(Status.INVALID_ARGUMENT) { "Issuer and subject pair already exists" }
        ErrorCode.ACCOUNT_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "Account was not found" }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) { "Account has not been activated yet" }
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.REQUISITION_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.REQUISITION_NOT_FOUND_BY_DATA_PROVIDER,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.DUCHY_NOT_FOUND,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_MEASUREMENT_CONSUMER,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.DATA_PROVIDER_CERTIFICATE_NOT_FOUND,
        ErrorCode.MEASUREMENT_CONSUMER_CERTIFICATE_NOT_FOUND,
        ErrorCode.DUCHY_CERTIFICATE_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_MEASUREMENT,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun authenticateAccount(request: AuthenticateAccountRequest): Account {
    val identityResult =
      OpenIdConnectIdentityReader()
        .readByIssuerAndSubject(
          client.singleUse(),
          request.identity.issuer,
          request.identity.subject
        )
        ?: failGrpc(Status.PERMISSION_DENIED) { "Account not found" }

    return AccountReader()
      .readByInternalAccountId(client.singleUse(), identityResult.accountId)
      ?.account
      ?: failGrpc(Status.PERMISSION_DENIED) { "Account not found" }
  }

  override suspend fun generateOpenIdRequestParams(
    request: GenerateOpenIdRequestParamsRequest
  ): OpenIdRequestParams {
    return GenerateOpenIdRequestParams(VALID_SECONDS).execute(client, idGenerator)
  }

  override suspend fun getOpenIdRequestParams(
    request: GetOpenIdRequestParamsRequest
  ): OpenIdRequestParams {
    val result =
      OpenIdRequestParamsReader().readByState(client.singleUse(), ExternalId(request.state))

    return if (result == null) {
      OpenIdRequestParams.getDefaultInstance()
    } else {
      openIdRequestParams {
        state = result.state.value
        nonce = result.nonce.value
        isExpired = result.isExpired
      }
    }
  }
}
