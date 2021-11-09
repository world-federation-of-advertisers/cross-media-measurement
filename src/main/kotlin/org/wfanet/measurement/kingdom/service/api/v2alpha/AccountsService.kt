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

import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.Account.ActivationState
import org.wfanet.measurement.api.v2alpha.Account.OpenIdConnectIdentity
import org.wfanet.measurement.api.v2alpha.AccountConstants
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AccountKt.activationParams
import org.wfanet.measurement.api.v2alpha.AccountKt.openIdConnectIdentity
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ActivateAccountRequest
import org.wfanet.measurement.api.v2alpha.AuthenticateRequest
import org.wfanet.measurement.api.v2alpha.AuthenticateResponse
import org.wfanet.measurement.api.v2alpha.CreateAccountRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ReplaceAccountIdentityRequest
import org.wfanet.measurement.api.v2alpha.account
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.Account.ActivationState as InternalActivationState
import org.wfanet.measurement.internal.kingdom.Account.OpenIdConnectIdentity as InternalOpenIdConnectIdentity
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.activateAccountRequest

class AccountsService(private val internalAccountsStub: AccountsCoroutineStub) :
  AccountsCoroutineImplBase() {

  override suspend fun createAccount(request: CreateAccountRequest): Account {
    TODO("Not yet implemented")
  }

  override suspend fun activateAccount(request: ActivateAccountRequest): Account {
    val key =
      grpcRequireNotNull(AccountKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    grpcRequire(request.activationToken.isNotBlank()) { "Activation token is missing" }

    val internalActivateAccountRequest = activateAccountRequest {
      externalAccountId = apiIdToExternalId(key.accountId)
      activationToken = apiIdToExternalId(request.activationToken)
    }

    val idToken = AccountConstants.CONTEXT_ID_TOKEN_KEY.get()
    val result =
      internalAccountsStub.withIdToken(idToken).activateAccount(internalActivateAccountRequest)

    // method only returns the basic account view so some fields are cleared
    return result.toAccount().copy {
      clearActivationParams()
      clearMeasurementConsumerCreationToken()
    }
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    TODO("Not yet implemented")
  }

  override suspend fun authenticate(request: AuthenticateRequest): AuthenticateResponse {
    TODO("Not yet implemented")
  }

  /** Converts an internal [InternalAccount] to a public [Account]. */
  private fun InternalAccount.toAccount(): Account {
    return account {
      name = AccountKey(externalIdToApiId(externalAccountId)).toName()
      if (externalCreatorAccountId != 0L) {
        creator = AccountKey(externalIdToApiId(externalCreatorAccountId)).toName()
      }

      activationState = this@toAccount.activationState.toActivationState()

      activationParams =
        activationParams {
          activationToken = externalIdToApiId(this@toAccount.activationToken)
          with(this@toAccount.externalOwnedMeasurementConsumerId) {
            if (this != 0L) {
              ownedMeasurementConsumer = MeasurementConsumerKey(externalIdToApiId(this)).toName()
            }
          }
        }

      measurementConsumerCreationToken =
        externalIdToApiId(this@toAccount.measurementConsumerCreationToken)

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (this@toAccount.identityCase) {
        InternalAccount.IdentityCase.OPEN_ID_IDENTITY ->
          openId = this@toAccount.openIdIdentity.toOpenIdConnectIdentity()
        InternalAccount.IdentityCase.IDENTITY_NOT_SET -> {}
      }
    }
  }

  /** Converts an internal [InternalActivationState] to a public [ActivationState]. */
  private fun InternalActivationState.toActivationState(): ActivationState =
    when (this) {
      InternalActivationState.ACTIVATED -> ActivationState.ACTIVATED
      InternalActivationState.UNACTIVATED -> ActivationState.UNACTIVATED
      InternalActivationState.UNRECOGNIZED, InternalActivationState.ACTIVATION_STATE_UNSPECIFIED ->
        ActivationState.ACTIVATION_STATE_UNSPECIFIED
    }

  /** Converts an internal [InternalOpenIdConnectIdentity] to a public [OpenIdConnectIdentity]. */
  private fun InternalOpenIdConnectIdentity.toOpenIdConnectIdentity(): OpenIdConnectIdentity =
      openIdConnectIdentity {
    subject = this@toOpenIdConnectIdentity.subject
    issuer = this@toOpenIdConnectIdentity.issuer
  }
}
