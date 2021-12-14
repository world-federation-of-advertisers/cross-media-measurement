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

import com.google.common.primitives.Longs
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.jwt.JwkSetConverter
import com.google.crypto.tink.jwt.JwtInvalidException
import com.google.crypto.tink.jwt.JwtPublicKeyVerify
import com.google.crypto.tink.jwt.JwtSignatureConfig
import com.google.crypto.tink.jwt.JwtValidator
import com.google.crypto.tink.tinkkey.KeyAccess
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.grpc.Status
import java.io.IOException
import java.security.GeneralSecurityException
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.calculateRSAThumbprint
import org.wfanet.measurement.common.createJwkKeyset
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.AuthenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.GenerateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.OpenIdRequestParams
import org.wfanet.measurement.internal.kingdom.ReplaceAccountIdentityRequest
import org.wfanet.measurement.kingdom.deploy.common.service.getIdToken
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdConnectIdentityReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdRequestParamsReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ActivateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.GenerateOpenIdRequestParams

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

      return CreateAccount(externalCreatorAccountId, externalOwnedMeasurementConsumerId)
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
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun activateAccount(request: ActivateAccountRequest): Account {
    val idToken = getIdToken() ?: failGrpc(Status.INVALID_ARGUMENT) { "Id token is missing" }

    val parsedValidatedIdToken =
      validateIdToken(idToken) ?: failGrpc(Status.INVALID_ARGUMENT) { "Id token is invalid" }

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
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    TODO("Not yet implemented")
  }

  override suspend fun authenticateAccount(request: AuthenticateAccountRequest): Account {
    val idToken = getIdToken() ?: failGrpc(Status.PERMISSION_DENIED) { "Id token is missing" }

    val parsedValidatedIdToken =
      validateIdToken(idToken) ?: failGrpc(Status.PERMISSION_DENIED) { "Id token is invalid" }

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

  /** Returns the issuer and subject if the idToken is valid and null otherwise. */
  private suspend fun validateIdToken(idToken: String): IdToken? {
    val tokenParts = idToken.split(".")

    if (tokenParts.size != 3) {
      return null
    }

    val claims =
      JsonParser.parseString(tokenParts[1].base64UrlDecode().toString(Charsets.UTF_8)).asJsonObject

    val subJwk = claims.get("sub_jwk")
    if (subJwk.isJsonNull || !subJwk.isJsonObject) {
      return null
    }

    JwtSignatureConfig.register()

    val publicKeysetHandle: KeysetHandle?
    val jwkKey = subJwk.asJsonObject
    try {
      publicKeysetHandle =
        JwkSetConverter.toKeysetHandle(createJwkKeyset(jwkKey), KeyAccess.publicAccess())
    } catch (ex: GeneralSecurityException) {
      return null
    } catch (ex: IOException) {
      return null
    }

    val verifier: JwtPublicKeyVerify?
    try {
      verifier = publicKeysetHandle.getPrimitive(JwtPublicKeyVerify::class.java)
    } catch (ex: GeneralSecurityException) {
      return null
    }

    try {
      val expectedHeader = JsonObject()
      expectedHeader.addProperty("typ", "JWT")
      expectedHeader.addProperty("alg", "RS256")

      val validator =
        JwtValidator.newBuilder()
          .expectIssuer("https://self-issued.me")
          .expectAudience(REDIRECT_URI)
          .expectTypeHeader(expectedHeader.toString())
          .build()
      val verifiedJwt = verifier!!.verifyAndDecode(idToken, validator)

      val state = verifiedJwt.getStringClaim("state")
      val nonce = verifiedJwt.getStringClaim("nonce")

      val result =
        OpenIdRequestParamsReader()
          .readByState(client.singleUse(), ExternalId(Longs.fromByteArray(state.base64UrlDecode())))
      if (result != null) {
        if (Longs.fromByteArray(nonce.base64UrlDecode()) != result.nonce.value || result.isExpired
        ) {
          return null
        }
      } else {
        return null
      }

      if (!verifiedJwt.subject.equals(calculateRSAThumbprint(jwkKey.toString()))) {
        return null
      }

      return IdToken(issuer = verifiedJwt.issuer, subject = verifiedJwt.subject)
    } catch (ex: GeneralSecurityException) {
      return null
    } catch (ex: JwtInvalidException) {
      return null
    }
  }
}
