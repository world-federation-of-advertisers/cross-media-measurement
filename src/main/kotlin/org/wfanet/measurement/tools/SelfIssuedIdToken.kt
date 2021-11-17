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
 * TODO(https://github.com/google/tink/issues/541): Replace with Tink Java JWT
 */

package org.wfanet.measurement.tools

import com.google.gson.JsonObject
import com.google.protobuf.ByteString
import java.net.URI
import java.security.KeyPairGenerator
import java.security.Signature
import java.security.interfaces.RSAPublicKey
import java.time.Clock
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.hashSha256

private const val EXP_TIME = 1000L

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

    val header = JsonObject()
    header.addProperty("typ", "JWT")
    header.addProperty("alg", "RS256")

    val claims = JsonObject()

    claims.addProperty("iss", "https://self-issued.me")

    val jwk = JsonObject()

    val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
    keyPairGenerator.initialize(1024)
    val keyPair = keyPairGenerator.genKeyPair()

    when (val publicKey = keyPair.public) {
      is RSAPublicKey -> {
        jwk.addProperty("kty", "RSA")
        jwk.addProperty("n", publicKey.modulus.toByteArray().base64UrlEncode())
        jwk.addProperty("e", publicKey.publicExponent.toByteArray().base64UrlEncode())

        claims.addProperty(
          "sub",
          calculateRSAThumbprint(jwk.get("n").asString, jwk.get("e").asString)
        )
      }
    }
    claims.addProperty("aud", queryParamMap["client_id"])

    queryParamMap["state"]?.let { claims.addProperty("state", it) }

    queryParamMap["nonce"]?.let { claims.addProperty("nonce", it) }
      ?: throw IllegalArgumentException()

    val now = clock.instant().epochSecond
    claims.addProperty("exp", now + EXP_TIME)
    claims.addProperty("iat", now)

    claims.add("sub_jwk", jwk)

    val encodedHeader = header.toString().toByteArray(Charsets.UTF_8).base64UrlEncode()
    val encodedClaims = claims.toString().toByteArray().base64UrlEncode()

    val signature = Signature.getInstance("SHA256WithRSA")
    signature.initSign(keyPair.private)

    val signingInput = "$encodedHeader.$encodedClaims".toByteArray(Charsets.US_ASCII)
    signature.update(signingInput)
    val signedData = signature.sign().base64UrlEncode()

    return "$encodedHeader.$encodedClaims.$signedData"
  } else {
    throw IllegalArgumentException()
  }
}

fun calculateRSAThumbprint(modulus: String, exponent: String): String {
  val requiredMembers = JsonObject()
  requiredMembers.addProperty("e", exponent)
  requiredMembers.addProperty("kty", "RSA")
  requiredMembers.addProperty("n", modulus)

  val hash = hashSha256(ByteString.copyFromUtf8(requiredMembers.toString()))

  return hash.toByteArray().base64UrlEncode()
}
