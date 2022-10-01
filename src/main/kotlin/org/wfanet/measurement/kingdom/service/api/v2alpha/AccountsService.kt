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

import com.google.gson.JsonParser
import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import java.security.GeneralSecurityException
import org.wfanet.measurement.api.AccountConstants
import org.wfanet.measurement.api.accountFromCurrentContext
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.Account.ActivationState
import org.wfanet.measurement.api.v2alpha.Account.OpenIdConnectIdentity
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AccountKt
import org.wfanet.measurement.api.v2alpha.AccountKt.activationParams
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ActivateAccountRequest
import org.wfanet.measurement.api.v2alpha.AuthenticateRequest
import org.wfanet.measurement.api.v2alpha.AuthenticateResponse
import org.wfanet.measurement.api.v2alpha.CreateAccountRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ReplaceAccountIdentityRequest
import org.wfanet.measurement.api.v2alpha.account
import org.wfanet.measurement.api.v2alpha.authenticateResponse
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.crypto.tink.PublicJwkHandle
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.openid.createRequestUri
import org.wfanet.measurement.common.toLong
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.Account.ActivationState as InternalActivationState
import org.wfanet.measurement.internal.kingdom.Account.OpenIdConnectIdentity as InternalOpenIdConnectIdentity
import org.wfanet.measurement.internal.kingdom.AccountKt as InternalAccountKt
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.account as internalAccount
import org.wfanet.measurement.internal.kingdom.activateAccountRequest
import org.wfanet.measurement.internal.kingdom.generateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.getOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.replaceAccountIdentityRequest

private const val SELF_ISSUED_ISSUER = "https://self-issued.me"

class AccountsService(
  private val internalAccountsStub: AccountsCoroutineStub,
  private val redirectUri: String
) : AccountsCoroutineImplBase() {

  override suspend fun createAccount(request: CreateAccountRequest): Account {
    val account = accountFromCurrentContext

    val ownedMeasurementConsumer = request.account.activationParams.ownedMeasurementConsumer
    val externalOwnedMeasurementConsumerId =
      if (ownedMeasurementConsumer.isNotBlank()) {
        val measurementConsumerKey =
          grpcRequireNotNull(MeasurementConsumerKey.fromName(ownedMeasurementConsumer)) {
            "Owned Measurement Consumer Resource name invalid"
          }
        apiIdToExternalId(measurementConsumerKey.measurementConsumerId)
      } else {
        0L
      }

    val internalCreateAccountRequest = internalAccount {
      externalCreatorAccountId = account.externalAccountId
      this.externalOwnedMeasurementConsumerId = externalOwnedMeasurementConsumerId
    }

    val result =
      try {
        internalAccountsStub.createAccount(internalCreateAccountRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "Creator's Account not found." }
          Status.Code.PERMISSION_DENIED ->
            failGrpc(Status.PERMISSION_DENIED, ex) {
              "Caller does not own the owned measurement consumer."
            }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    return result.toAccount()
  }

  override suspend fun activateAccount(request: ActivateAccountRequest): Account {
    val key =
      grpcRequireNotNull(AccountKey.fromName(request.name)) {
        "Resource name unspecified or invalid"
      }

    grpcRequire(request.activationToken.isNotBlank()) { "Activation token is missing" }

    val idToken =
      grpcRequireNotNull(AccountConstants.CONTEXT_ID_TOKEN_KEY.get()) { "ID token is missing" }

    val openIdConnectIdentity =
      try {
        validateIdToken(
          idToken = idToken,
          redirectUri = redirectUri,
          internalAccountsStub = internalAccountsStub
        )
      } catch (ex: GeneralSecurityException) {
        failGrpc(Status.INVALID_ARGUMENT.withCause(ex)) { "ID token is invalid" }
      } catch (ex: Exception) {
        failGrpc(Status.UNKNOWN.withCause(ex)) { "ID token is invalid" }
      }

    val internalActivateAccountRequest = activateAccountRequest {
      externalAccountId = apiIdToExternalId(key.accountId)
      activationToken = apiIdToExternalId(request.activationToken)
      identity = openIdConnectIdentity
    }

    val result =
      try {
        internalAccountsStub.activateAccount(internalActivateAccountRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.PERMISSION_DENIED ->
            failGrpc(Status.PERMISSION_DENIED, ex) {
              "Activation token is not valid for this account."
            }
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { ex.message ?: "Failed precondition." }
          Status.Code.NOT_FOUND ->
            failGrpc(Status.NOT_FOUND, ex) { "Account to activate has not been found." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    // method only returns the basic account view so some fields are cleared
    return result.toAccount().copy { clearActivationParams() }
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    val account = accountFromCurrentContext

    grpcRequireNotNull(AccountKey.fromName(request.name)) { "Resource name unspecified or invalid" }

    val newIdToken = request.openId.identityBearerToken
    grpcRequire(newIdToken.isNotBlank()) { "New ID token is missing" }

    val openIdConnectIdentity =
      try {
        validateIdToken(
          idToken = newIdToken,
          redirectUri = redirectUri,
          internalAccountsStub = internalAccountsStub
        )
      } catch (ex: GeneralSecurityException) {
        failGrpc(Status.INVALID_ARGUMENT.withCause(ex)) { "New ID token is invalid" }
      } catch (ex: Exception) {
        failGrpc(Status.UNKNOWN.withCause(ex)) { "ID token is invalid" }
      }

    val internalReplaceAccountIdentityRequest = replaceAccountIdentityRequest {
      externalAccountId = account.externalAccountId
      identity = openIdConnectIdentity
    }

    val result =
      try {
        internalAccountsStub.replaceAccountIdentity(internalReplaceAccountIdentityRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.FAILED_PRECONDITION ->
            failGrpc(Status.FAILED_PRECONDITION, ex) { ex.message ?: "Failed precondition." }
          Status.Code.NOT_FOUND -> failGrpc(Status.NOT_FOUND, ex) { "Account was not found." }
          else -> failGrpc(Status.UNKNOWN, ex) { "Unknown exception." }
        }
      }

    // method only returns the basic account view so some fields are cleared
    return result.toAccount().copy { clearActivationParams() }
  }

  override suspend fun authenticate(request: AuthenticateRequest): AuthenticateResponse {
    grpcRequire(request.issuer.isNotBlank()) { "Issuer unspecified" }

    val openIdRequestParams =
      internalAccountsStub.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})

    var uriString = ""
    if (request.issuer == SELF_ISSUED_ISSUER) {
      uriString =
        createRequestUri(
          state = openIdRequestParams.state,
          nonce = openIdRequestParams.nonce,
          redirectUri = this.redirectUri,
          isSelfIssued = true
        )
    }

    return authenticateResponse { authenticationRequestUri = uriString }
  }

  /** Converts an internal [InternalAccount] to a public [Account]. */
  private fun InternalAccount.toAccount(): Account {
    val source = this

    return account {
      name = AccountKey(externalIdToApiId(externalAccountId)).toName()
      if (externalCreatorAccountId != 0L) {
        creator = AccountKey(externalIdToApiId(externalCreatorAccountId)).toName()
      }

      activationState = source.activationState.toActivationState()

      activationParams = activationParams {
        activationToken = externalIdToApiId(source.activationToken)
        if (source.externalOwnedMeasurementConsumerId != 0L) {
          ownedMeasurementConsumer =
            MeasurementConsumerKey(externalIdToApiId(source.externalOwnedMeasurementConsumerId))
              .toName()
        }
      }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.identityCase) {
        InternalAccount.IdentityCase.OPEN_ID_IDENTITY ->
          openId = source.openIdIdentity.toOpenIdConnectIdentity()
        InternalAccount.IdentityCase.IDENTITY_NOT_SET -> {}
      }
    }
  }

  /** Converts an internal [InternalActivationState] to a public [ActivationState]. */
  private fun InternalActivationState.toActivationState(): ActivationState =
    when (this) {
      InternalActivationState.ACTIVATED -> ActivationState.ACTIVATED
      InternalActivationState.UNACTIVATED -> ActivationState.UNACTIVATED
      InternalActivationState.UNRECOGNIZED,
      InternalActivationState.ACTIVATION_STATE_UNSPECIFIED ->
        ActivationState.ACTIVATION_STATE_UNSPECIFIED
    }

  /** Converts an internal [InternalOpenIdConnectIdentity] to a public [OpenIdConnectIdentity]. */
  private fun InternalOpenIdConnectIdentity.toOpenIdConnectIdentity(): OpenIdConnectIdentity =
    AccountKt.openIdConnectIdentity {
      subject = this@toOpenIdConnectIdentity.subject
      issuer = this@toOpenIdConnectIdentity.issuer
    }

  companion object {
    /**
     * Returns the identity if the ID token is valid.
     *
     * @throws GeneralSecurityException if the ID token validation fails
     */
    suspend fun validateIdToken(
      idToken: String,
      redirectUri: String,
      internalAccountsStub: AccountsCoroutineStub
    ): InternalOpenIdConnectIdentity {
      val tokenParts = idToken.split(".")

      if (tokenParts.size != 3) {
        throw GeneralSecurityException("Id token does not have 3 components")
      }

      val claims =
        JsonParser.parseString(tokenParts[1].base64UrlDecode().toString(Charsets.UTF_8))
          .asJsonObject

      val subJwk = claims.get("sub_jwk")
      if (subJwk.isJsonNull || !subJwk.isJsonObject) {
        throw GeneralSecurityException()
      }

      val jwk = subJwk.asJsonObject
      val publicJwkHandle =
        try {
          PublicJwkHandle.fromJwk(jwk)
        } catch (ex: IOException) {
          throw GeneralSecurityException()
        }

      val verifiedJwt =
        SelfIssuedIdTokens.validateJwt(
          redirectUri = redirectUri,
          idToken = idToken,
          publicJwkHandle = publicJwkHandle
        )

      if (!verifiedJwt.subject.equals(SelfIssuedIdTokens.calculateRsaThumbprint(jwk.toString()))) {
        throw GeneralSecurityException()
      }

      val state = verifiedJwt.getStringClaim("state")
      val nonce = verifiedJwt.getStringClaim("nonce")

      if (state == null || nonce == null) {
        throw GeneralSecurityException()
      }

      val openIdRequestParams =
        internalAccountsStub.getOpenIdRequestParams(
          getOpenIdRequestParamsRequest { this.state = state.base64UrlDecode().toLong() }
        )

      if (
        nonce.base64UrlDecode().toLong() != openIdRequestParams.nonce ||
          openIdRequestParams.isExpired
      ) {
        throw GeneralSecurityException()
      }

      if (verifiedJwt.subject == null) {
        throw GeneralSecurityException()
      }

      return InternalAccountKt.openIdConnectIdentity {
        issuer = requireNotNull(verifiedJwt.issuer)
        subject = requireNotNull(verifiedJwt.subject)
      }
    }
  }
}
