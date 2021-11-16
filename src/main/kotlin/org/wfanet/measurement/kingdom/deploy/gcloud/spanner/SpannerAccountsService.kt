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
import com.google.gson.JsonParser
import io.grpc.Status
import java.math.BigInteger
import java.security.KeyFactory
import java.security.Signature
import java.security.spec.RSAPublicKeySpec
import org.wfanet.measurement.common.base64UrlDecode
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
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.GenerateOpenIdRequestParams
import org.wfanet.measurement.tools.calculateRSAThumbprint

private const val REDIRECT_URI = "https://localhost:2048"
private const val MAX_AGE = 3600L

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
    TODO("Not yet implemented")
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
    return GenerateOpenIdRequestParams(MAX_AGE).execute(client, idGenerator)
  }

  /**
   * Returns the issuer and subject if the idToken is valid and null otherwise.
   * TODO(https://github.com/google/tink/issues/541): Replace with Tink Java JWT
   */
  private suspend fun validateIdToken(idToken: String): IdToken? {
    val tokenParts = idToken.split(".")

    if (tokenParts.size != 3) {
      return null
    }

    val header =
      JsonParser.parseString(tokenParts[0].base64UrlDecode().toString(Charsets.UTF_8)).asJsonObject

    val alg = header.get("alg")
    if (alg.isJsonNull || !alg.isJsonPrimitive || alg.asString != "RS256") {
      return null
    }

    val claims =
      JsonParser.parseString(tokenParts[1].base64UrlDecode().toString(Charsets.UTF_8)).asJsonObject

    val iss = claims.get("iss")
    if (iss.isJsonNull || !iss.isJsonPrimitive || iss.asString != "https://self-issued.me") {
      return null
    }

    val aud = claims.get("aud")
    if (aud.isJsonNull || !aud.isJsonPrimitive || aud.asString != REDIRECT_URI) {
      return null
    }

    val state = claims.get("state")
    val nonce = claims.get("nonce")

    if (state.isJsonNull || !state.isJsonPrimitive || nonce.isJsonNull || !nonce.isJsonPrimitive) {
      return null
    }

    val result =
      OpenIdRequestParamsReader()
        .readByState(
          client.singleUse(),
          ExternalId(Longs.fromByteArray(state.asString.base64UrlDecode()))
        )
    if (result != null) {
      if (Longs.fromByteArray(nonce.asString.base64UrlDecode()) != result.nonce.value ||
          result.isExpired
      ) {
        return null
      }
    } else {
      return null
    }

    val subJwk = claims.get("sub_jwk")
    if (subJwk.isJsonNull || !subJwk.isJsonObject) {
      return null
    }

    val modulus = subJwk.asJsonObject.get("n")
    val exponent = subJwk.asJsonObject.get("e")

    if (modulus.isJsonNull ||
        !modulus.isJsonPrimitive ||
        exponent.isJsonNull ||
        !modulus.isJsonPrimitive
    ) {
      return null
    }

    val sub = claims.get("sub")
    if (sub.isJsonNull ||
        !sub.isJsonPrimitive ||
        !sub.asString.equals(calculateRSAThumbprint(modulus.asString, exponent.asString))
    ) {
      return null
    }

    val publicKeySpec =
      RSAPublicKeySpec(
        BigInteger(modulus.asString.base64UrlDecode()),
        BigInteger(exponent.asString.base64UrlDecode())
      )
    val keyFactory = KeyFactory.getInstance("RSA")
    val publicKey = keyFactory.generatePublic(publicKeySpec)
    val verifier = Signature.getInstance("SHA256withRSA")
    verifier.initVerify(publicKey)
    verifier.update((tokenParts[0] + "." + tokenParts[1]).toByteArray(Charsets.US_ASCII))
    if (!verifier.verify(tokenParts[2].base64UrlDecode())) {
      return null
    }

    return IdToken(issuer = iss.asString, subject = sub.asString)
  }
}
