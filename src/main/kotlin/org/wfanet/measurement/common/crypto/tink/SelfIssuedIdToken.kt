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

/*
 * Contains methods for working with self-issued Id tokens.
 */

package org.wfanet.measurement.common.crypto.tink

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.jwt.JwkSetConverter
import com.google.crypto.tink.jwt.JwtPublicKeySign
import com.google.crypto.tink.jwt.JwtPublicKeyVerify
import com.google.crypto.tink.jwt.JwtSignatureConfig
import com.google.crypto.tink.jwt.JwtValidator
import com.google.crypto.tink.jwt.RawJwt
import com.google.crypto.tink.jwt.VerifiedJwt
import com.google.crypto.tink.tinkkey.KeyAccess
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.protobuf.ByteString
import java.net.URI
import java.time.Clock
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.hashSha256

private const val EXP_TIME = 1000L

class SelfIssuedIdToken private constructor() {
  companion object {
    private val HEADER: String
    private const val STATE = "state"
    private const val NONCE = "nonce"
    private const val SELF_ISSUED_ISSUER = "https://self-issued.me"

    init {
      JwtSignatureConfig.register()

      val headerObject = JsonObject()
      headerObject.addProperty("typ", "JWT")
      headerObject.addProperty("alg", "RS256")
      HEADER = headerObject.toString()
    }

    /**
     * Returns a self-issued id token using a generated private key.
     *
     * @throws IllegalArgumentException if the uriString doesn't match the open id connect
     * requirements for self-issued, or doesn't include state and nonce.
     */
    fun generateIdToken(uriString: String, clock: Clock): String {
      val privateKeysetHandle = KeysetHandle.generateNew(KeyTemplates.get("JWT_RS256_2048_F4_RAW"))
      return generateIdToken(privateKeysetHandle, uriString, clock)
    }

    /**
     * Returns a self-issued id token using a provided private key.
     *
     * @throws IllegalArgumentException if the uriString doesn't match the open id connect
     * requirements for self-issued, or doesn't include state and nonce.
     */
    fun generateIdToken(
      privateKeysetHandle: KeysetHandle,
      uriString: String,
      clock: Clock
    ): String {
      val uri = URI.create(uriString)

      if (uri.scheme.equals("openid")) {
        val queryParamMap = buildQueryParamMap(uri)
        if (!isQueryValid(queryParamMap)) {
          throw IllegalArgumentException("URI query parameters are invalid")
        }

        val signer = privateKeysetHandle.getPrimitive(JwtPublicKeySign::class.java)
        val jwkKey = getJwkKey(privateKeysetHandle)
        val now = clock.instant()

        val rawJwtBuilder =
          RawJwt.newBuilder()
            .setIssuer(SELF_ISSUED_ISSUER)
            .setSubject(calculateRSAThumbprint(jwkKey.toString()))
            .addAudience(queryParamMap["client_id"])
            .setTypeHeader(HEADER)
            .setExpiration(now.plusSeconds(EXP_TIME))
            .setIssuedAt(now)
            .addJsonObjectClaim("sub_jwk", jwkKey.toString())

        rawJwtBuilder.addStringClaim(STATE, queryParamMap[STATE])
        rawJwtBuilder.addStringClaim(NONCE, queryParamMap[NONCE])

        return signer!!.signAndEncode(rawJwtBuilder.build())
      } else {
        throw IllegalArgumentException()
      }
    }

    private fun buildQueryParamMap(uri: URI): Map<String, String> {
      val queryParamMap = mutableMapOf<String, String>()

      for (queryParam in uri.query.split("&")) {
        val keyValue = queryParam.split("=")
        queryParamMap[keyValue[0]] = keyValue[1]
      }

      return queryParamMap
    }

    private fun isQueryValid(queryParamMap: Map<String, String>): Boolean {
      queryParamMap["scope"]?.contains("openid") ?: return false
      queryParamMap["response_type"]?.equals("id_token") ?: return false
      queryParamMap[STATE] ?: return false
      queryParamMap[NONCE] ?: return false
      return true
    }

    private fun getJwkKey(privateKeysetHandle: KeysetHandle): JsonObject {
      val jwkKeyset =
        JsonParser.parseString(
            JwkSetConverter.fromKeysetHandle(
              privateKeysetHandle.publicKeysetHandle,
              KeyAccess.publicAccess()
            )
          )
          .asJsonObject
      return jwkKeyset.getAsJsonArray("keys").get(0).asJsonObject
    }

    fun calculateRSAThumbprint(jwtKey: String): String {
      val hash = hashSha256(ByteString.copyFromUtf8(jwtKey))

      return hash.toByteArray().base64UrlEncode()
    }

    fun getPublicKeysetHandle(jwkKey: JsonObject): KeysetHandle {
      val keyset = JsonObject()
      val keys = JsonArray()
      keys.add(jwkKey)
      keyset.add("keys", keys)

      return JwkSetConverter.toKeysetHandle(keyset.toString(), KeyAccess.publicAccess())
    }

    fun getPublicKeyVerifier(publicKeysetHandle: KeysetHandle): JwtPublicKeyVerify {
      return publicKeysetHandle.getPrimitive(JwtPublicKeyVerify::class.java)
    }

    /** Validates the signature, the header, and the following claims: issuer and audience. */
    fun validateJwt(
      redirectUri: String,
      idToken: String,
      verifier: JwtPublicKeyVerify
    ): VerifiedJwt {
      val validator =
        JwtValidator.newBuilder()
          .expectIssuer(SELF_ISSUED_ISSUER)
          .expectAudience(redirectUri)
          .expectTypeHeader(HEADER)
          .build()

      return verifier.verifyAndDecode(idToken, validator)
    }
  }
}
