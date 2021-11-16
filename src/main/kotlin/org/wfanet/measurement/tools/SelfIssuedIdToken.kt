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

// TODO: "Replace implementation with Tink Java JWT when it is included in a public release"
/** Contains methods for working with self-issued Id tokens. */
private const val EXP_TIME = 1000L

fun generateIdToken(uriString: String, clock: Clock): String {
  val uri = URI.create(uriString)

  if (uri.scheme.equals("openid")) {
    val queryParamMap = mutableMapOf<String, String>()

    for (queryParam in uri.query.split("&")) {
      val keyValue = queryParam.split("=")
      queryParamMap[keyValue[0]] = keyValue[1]
    }

    queryParamMap["scope"]?.contains("openid") ?: return ""
    queryParamMap["response_type"]?.equals("id_token") ?: return ""

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

    queryParamMap["nonce"]?.let { claims.addProperty("nonce", it) } ?: return ""

    val now = clock.instant().epochSecond
    claims.addProperty("exp", now + EXP_TIME)
    claims.addProperty("iat", now)
    queryParamMap["max_age"]?.let { claims.addProperty("auth_time", now) }

    claims.add("sub_jwk", jwk)

    val encodedHeader = header.toString().toByteArray(Charsets.UTF_8).base64UrlEncode()
    val encodedClaims = claims.toString().toByteArray().base64UrlEncode()

    val signature = Signature.getInstance("SHA256WithRSA")
    signature.initSign(keyPair.private)

    val signingInput = "$encodedHeader.$encodedClaims".toByteArray(Charsets.US_ASCII)
    signature.update(signingInput)
    val signedData = signature.sign().base64UrlEncode()

    return "$encodedHeader.$encodedClaims.$signedData"
  }

  return ""
}

fun calculateRSAThumbprint(modulus: String, exponent: String): String {
  val requiredMembers = JsonObject()
  requiredMembers.addProperty("e", exponent)
  requiredMembers.addProperty("kty", "RSA")
  requiredMembers.addProperty("n", modulus)

  val hash = hashSha256(ByteString.copyFromUtf8(requiredMembers.toString()))

  return hash.toByteArray().base64UrlEncode()
}
