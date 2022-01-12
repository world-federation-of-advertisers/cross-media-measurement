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

import com.google.gson.JsonParser
import io.grpc.Status
import java.io.IOException
import java.security.GeneralSecurityException
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.crypto.tink.PublicJwkHandle.Companion.fromJwk
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens.calculateRsaThumbprint
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens.validateJwt
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toLong
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.AuthenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerCreationTokenResponse
import org.wfanet.measurement.internal.kingdom.GenerateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.OpenIdRequestParams
import org.wfanet.measurement.internal.kingdom.ReplaceAccountIdentityRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenResponse
import org.wfanet.measurement.kingdom.deploy.common.service.getIdToken
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdConnectIdentityReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdRequestParamsReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ActivateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurementConsumerCreationToken
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.GenerateOpenIdRequestParams
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceAccountIdentityWithNewOpenIdConnectIdentity

private const val REDIRECT_URI = "https://localhost:2048"
private const val VALID_SECONDS = 3600L

class SpannerAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : AccountsCoroutineImplBase() {
  data class IdToken(val issuer: String, val subject: String)

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
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Creator account not found" }
        KingdomInternalException.Code.PERMISSION_DENIED ->
          failGrpc(Status.PERMISSION_DENIED) {
            "Caller does not own the owned measurement consumer"
          }
        KingdomInternalException.Code.API_KEY_NOT_FOUND,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.EVENT_GROUP_INVALID_ARGS,
        KingdomInternalException.Code.EVENT_GROUP_NOT_FOUND -> throw e
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
    val idToken = getIdToken() ?: failGrpc(Status.INVALID_ARGUMENT) { "Id token is missing" }

    val parsedValidatedIdToken =
      try {
        validateIdToken(idToken)
      } catch (ex: GeneralSecurityException) {
        throw Status.INVALID_ARGUMENT
          .withCause(ex)
          .withDescription("ID token is invalid")
          .asRuntimeException()
      } catch (ex: Exception) {
        throw Status.UNKNOWN
          .withCause(ex)
          .withDescription("ID token is invalid")
          .asRuntimeException()
      }

    try {
      return ActivateAccount(
          externalAccountId = ExternalId(request.externalAccountId),
          activationToken = ExternalId(request.activationToken),
          issuer = parsedValidatedIdToken.issuer,
          subject = parsedValidatedIdToken.subject
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.PERMISSION_DENIED ->
          failGrpc(Status.PERMISSION_DENIED) { "Activation token is not valid for this account" }
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY ->
          failGrpc(Status.INVALID_ARGUMENT) { "Issuer and subject pair already exists" }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL ->
          failGrpc(Status.PERMISSION_DENIED) { "Cannot activate an account again" }
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Account to activate has not been found" }
        KingdomInternalException.Code.API_KEY_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
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
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.EVENT_GROUP_INVALID_ARGS,
        KingdomInternalException.Code.EVENT_GROUP_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    val idToken = getIdToken() ?: failGrpc(Status.INVALID_ARGUMENT) { "New Id token is missing" }

    val parsedValidatedIdToken =
      try {
        validateIdToken(idToken)
      } catch (ex: GeneralSecurityException) {
        throw Status.INVALID_ARGUMENT
          .withCause(ex)
          .withDescription("ID token is invalid")
          .asRuntimeException()
      } catch (ex: Exception) {
        throw Status.UNKNOWN
          .withCause(ex)
          .withDescription("ID token is invalid")
          .asRuntimeException()
      }

    try {
      return ReplaceAccountIdentityWithNewOpenIdConnectIdentity(
          externalAccountId = ExternalId(request.externalAccountId),
          issuer = parsedValidatedIdToken.issuer,
          subject = parsedValidatedIdToken.subject
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY ->
          failGrpc(Status.INVALID_ARGUMENT) { "Issuer and subject pair already exists" }
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Account was not found" }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) { "Account has not been activated yet" }
        KingdomInternalException.Code.API_KEY_NOT_FOUND,
        KingdomInternalException.Code.PERMISSION_DENIED,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.EVENT_GROUP_INVALID_ARGS,
        KingdomInternalException.Code.EVENT_GROUP_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun authenticateAccount(request: AuthenticateAccountRequest): Account {
    val idToken = getIdToken() ?: failGrpc(Status.PERMISSION_DENIED) { "ID token is missing" }

    val parsedValidatedIdToken =
      try {
        validateIdToken(idToken)
      } catch (ex: GeneralSecurityException) {
        throw Status.PERMISSION_DENIED
          .withCause(ex)
          .withDescription("ID token is invalid")
          .asRuntimeException()
      } catch (ex: Exception) {
        throw Status.UNKNOWN
          .withCause(ex)
          .withDescription("ID token is invalid")
          .asRuntimeException()
      }

    val identityResult =
      OpenIdConnectIdentityReader()
        .readByIssuerAndSubject(
          client.singleUse(),
          parsedValidatedIdToken.issuer,
          parsedValidatedIdToken.subject
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

  /**
   * Returns the issuer and subject if the ID token is valid.
   *
   * @throws GeneralSecurityException if the ID token validation fails
   */
  private suspend fun validateIdToken(idToken: String): IdToken {
    val tokenParts = idToken.split(".")

    if (tokenParts.size != 3) {
      throw GeneralSecurityException("Id token does not have 3 components")
    }

    val claims =
      JsonParser.parseString(tokenParts[1].base64UrlDecode().toString(Charsets.UTF_8)).asJsonObject

    val subJwk = claims.get("sub_jwk")
    if (subJwk.isJsonNull || !subJwk.isJsonObject) {
      throw GeneralSecurityException()
    }

    val jwk = subJwk.asJsonObject
    val publicJwkHandle =
      try {
        fromJwk(jwk)
      } catch (ex: IOException) {
        throw GeneralSecurityException()
      }

    val verifiedJwt =
      validateJwt(redirectUri = REDIRECT_URI, idToken = idToken, publicJwkHandle = publicJwkHandle)

    if (!verifiedJwt.subject.equals(calculateRsaThumbprint(jwk.toString()))) {
      throw GeneralSecurityException()
    }

    val state = verifiedJwt.getStringClaim("state")
    val nonce = verifiedJwt.getStringClaim("nonce")

    if (state == null || nonce == null) {
      throw GeneralSecurityException()
    }

    val result =
      OpenIdRequestParamsReader()
        .readByState(client.singleUse(), ExternalId(state.base64UrlDecode().toLong()))
    if (result != null) {
      if (nonce.base64UrlDecode().toLong() != result.nonce.value || result.isExpired) {
        throw GeneralSecurityException()
      }
    } else {
      throw GeneralSecurityException()
    }

    if (verifiedJwt.issuer == null || verifiedJwt.subject == null) {
      throw GeneralSecurityException()
    } else {
      return IdToken(issuer = verifiedJwt.issuer!!, subject = verifiedJwt.subject!!)
    }
  }
}
