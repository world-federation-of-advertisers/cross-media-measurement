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

package org.wfanet.measurement.common

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.jwt.JwkSetConverter
import com.google.crypto.tink.jwt.JwtPublicKeySign
import com.google.crypto.tink.jwt.JwtSignatureConfig
import com.google.crypto.tink.jwt.RawJwt
import com.google.crypto.tink.tinkkey.KeyAccess
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.protobuf.ByteString
import java.net.URI
import java.security.GeneralSecurityException
import java.time.Clock
import org.wfanet.measurement.common.crypto.hashSha256

private const val EXP_TIME = 1000L

/**
 * Returns a self-issued Id token. Throws [IllegalArgumentException] if the uriString doesn't match
 * the open id connect requirements for self-issued, or doesn't include state and nonce.
 */
fun generateIdToken(uriString: String, clock: Clock): String {
  val uri = URI.create(uriString)

  if (uri.scheme.equals("openid")) {
    val queryParamMap = mutableMapOf<String, String>()

    for (queryParam in uri.query.split("&")) {
      val keyValue = queryParam.split("=")
      queryParamMap[keyValue[0]] = keyValue[1]
    }

    queryParamMap["scope"]?.contains("openid") ?: throw IllegalArgumentException()
    queryParamMap["response_type"]?.equals("id_token") ?: throw IllegalArgumentException()

    JwtSignatureConfig.register()

    var privateKeysetHandle: KeysetHandle? = null
    try {
      privateKeysetHandle = KeysetHandle.generateNew(KeyTemplates.get("JWT_RS256_2048_F4_RAW"))
    } catch (ex: GeneralSecurityException) {
      val stack = ex.stackTrace
      println(stack)
    }

    var signer: JwtPublicKeySign? = null
    try {
      signer = privateKeysetHandle!!.getPrimitive(JwtPublicKeySign::class.java)
    } catch (ex: GeneralSecurityException) {
      val stack = ex.stackTrace
      println(stack)
    }

    val header = JsonObject()
    header.addProperty("typ", "JWT")
    header.addProperty("alg", "RS256")

    val jwkKeyset =
      JsonParser.parseString(
          JwkSetConverter.fromKeysetHandle(
            privateKeysetHandle!!.publicKeysetHandle,
            KeyAccess.publicAccess()
          )
        )
        .asJsonObject
    val jwkKey = jwkKeyset.getAsJsonArray("keys").get(0).asJsonObject

    val now = clock.instant()
    val rawJwtBuilder =
      RawJwt.newBuilder()
        .setIssuer("https://self-issued.me")
        .setSubject(calculateRSAThumbprint(jwkKey.toString()))
        .addAudience(queryParamMap["client_id"])
        .setTypeHeader(header.toString())
        .setExpiration(now.plusSeconds(EXP_TIME))
        .setIssuedAt(now)
        .addJsonObjectClaim("sub_jwk", jwkKey.toString())

    queryParamMap["state"]?.let { rawJwtBuilder.addStringClaim("state", it) }
    queryParamMap["nonce"]?.let { rawJwtBuilder.addStringClaim("nonce", it) }
      ?: throw IllegalArgumentException()

    return signer!!.signAndEncode(rawJwtBuilder.build())
  } else {
    throw IllegalArgumentException()
  }
}

fun calculateRSAThumbprint(jwtKey: String): String {
  val hash = hashSha256(ByteString.copyFromUtf8(jwtKey))

  return hash.toByteArray().base64UrlEncode()
}

fun createJwkKeyset(jwkKey: JsonObject): String {
  val keyset = JsonObject()
  val keys = JsonArray()
  keys.add(jwkKey)
  keyset.add("keys", keys)

  return keyset.toString()
}
