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
import org.wfanet.measurement.common.identity.StringGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.CreateAccountRequest
import org.wfanet.measurement.internal.kingdom.ReplaceAccountIdentityRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ActivateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceAccountIdentityWithNewUsernameIdentity

class SpannerAccountsService(
  private val idGenerator: IdGenerator,
  private val stringGenerator: StringGenerator,
  private val client: AsyncDatabaseClient
) : AccountsCoroutineImplBase() {

  override suspend fun createAccount(request: CreateAccountRequest): Account {
    // TODO("check if externalCreatorAccountId owner of externalOwnedMeasurementConsumerId")

    try {
      return CreateAccount(
          request.externalCreatorAccountId,
          request.externalOwnedMeasurementConsumerId
        )
        .execute(client, idGenerator, stringGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.ACCOUNT_ALREADY_ACTIVATED,
        KingdomInternalException.Code.USERNAME_ALREADY_EXISTS,
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND,
        KingdomInternalException.Code.ACCOUNT_NOT_ACTIVATED,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun activateAccount(request: ActivateAccountRequest): Account {
    AccountReader(Account.View.FULL)
      .readByExternalAccountId(client.singleUse(), ExternalId(request.externalAccountId))
      ?.let {
        val storedActivationToken = it.account.activationParams.activationToken
        if (request.activationToken != storedActivationToken) {
          failGrpc(Status.PERMISSION_DENIED) { "Activation token is not valid for this account" }
        }
      }
      ?: failGrpc(Status.NOT_FOUND) { "Account to activate has not been found" }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (request.credentialsCase) {
      ActivateAccountRequest.CredentialsCase.OPEN_ID_CONNECT_CREDENTIALS ->
        // TODO("Call the open id connect version of activate account when it is implemented")
        failGrpc(Status.UNIMPLEMENTED) { "OpenId not implemented yet" }
      ActivateAccountRequest.CredentialsCase.USERNAME_CREDENTIALS ->
        try {
          ActivateAccount(request.externalAccountId, request.usernameCredentials)
            .execute(client, idGenerator, stringGenerator)
        } catch (e: KingdomInternalException) {
          when (e.code) {
            KingdomInternalException.Code.USERNAME_ALREADY_EXISTS ->
              failGrpc(Status.INVALID_ARGUMENT) { "Username already exists" }
            KingdomInternalException.Code.ACCOUNT_ALREADY_ACTIVATED ->
              failGrpc(Status.PERMISSION_DENIED) { "Cannot activate an account again" }
            KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
              failGrpc(Status.NOT_FOUND) { "Account to activate has not been found" }
            KingdomInternalException.Code.ACCOUNT_NOT_ACTIVATED,
            KingdomInternalException.Code.REQUISITION_NOT_FOUND,
            KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
            KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
            KingdomInternalException.Code.DUCHY_NOT_FOUND,
            KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
            KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
            KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
            KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
            KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
            KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
            KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND -> throw e
          }
        }
      ActivateAccountRequest.CredentialsCase.CREDENTIALS_NOT_SET ->
        failGrpc(Status.NOT_FOUND) { "Account identity not found in request" }
    }
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (request.credentialsCase) {
      ReplaceAccountIdentityRequest.CredentialsCase.OPEN_ID_CONNECT_CREDENTIALS ->
        // TODO("Call the open id connect version of replace account identity when it is
        // implemented")
        failGrpc(Status.UNIMPLEMENTED) { "OpenId not implemented yet" }
      ReplaceAccountIdentityRequest.CredentialsCase.USERNAME_CREDENTIALS ->
        try {
          ReplaceAccountIdentityWithNewUsernameIdentity(
              request.externalAccountId,
              request.usernameCredentials
            )
            .execute(client, idGenerator, stringGenerator)
        } catch (e: KingdomInternalException) {
          when (e.code) {
            KingdomInternalException.Code.USERNAME_ALREADY_EXISTS ->
              failGrpc(Status.INVALID_ARGUMENT) { "Username already exists" }
            KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
              failGrpc(Status.NOT_FOUND) { "Account was not found" }
            KingdomInternalException.Code.ACCOUNT_NOT_ACTIVATED ->
              failGrpc(Status.PERMISSION_DENIED) { "Account has not been activated yet" }
            KingdomInternalException.Code.ACCOUNT_ALREADY_ACTIVATED,
            KingdomInternalException.Code.REQUISITION_NOT_FOUND,
            KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
            KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
            KingdomInternalException.Code.DUCHY_NOT_FOUND,
            KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
            KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
            KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
            KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
            KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
            KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
            KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND -> throw e
          }
        }
      ReplaceAccountIdentityRequest.CredentialsCase.CREDENTIALS_NOT_SET ->
        failGrpc(Status.NOT_FOUND) { "Account identity not found in request" }
    }
  }
}
