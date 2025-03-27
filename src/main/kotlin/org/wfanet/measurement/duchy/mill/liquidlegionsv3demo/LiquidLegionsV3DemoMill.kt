package org.wfanet.measurement.duchy.mill.liquidlegionsv3demo

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.kms.v1.KeyManagementServiceClient
import com.google.cloud.kms.v1.KeyManagementServiceSettings
import com.google.protobuf.ByteString
import java.io.File // For reading the token file
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Base64 // For decoding ciphertext
import java.util.Date
import org.wfanet.measurement.storage.StorageClient

class LiquidLegionsV3DemoMill(
  private val storageClient: StorageClient,
) {
  fun run() {
    val blobBucket = "demo-tee"
    val blobKey = "encrypted_edp_data"
    val kmsKeyName = "projects/halo-cmm-dev/locations/us-central1/keyRings/tee-demo-key-ring/cryptoKeys/tee-demo-key-1"
      // e.g., projects/.../locations/.../keyRings/.../cryptoKeys/...
    val wifAudience = "https://iam.googleapis.com/projects/462363635192/locations/global/workloadIdentityPools/tee-demo-pool/providers/tee-demo-pool-provider"
    // e.g., iam.googleapis.com/projects/[NUM]/locations/global/workloadIdentityPools/[POOL]/providers/[PROV]
    val targetSaEmail = "tee-demo-decrypter@halo-cmm-dev.iam.gserviceaccount.com"
    // e.g., service-account@project-id.iam.gserviceaccount.com
    val tokenFilePath = "/run/container_launcher/attestation_verifier_claims_token"

    println("Configuration:")
    println("- KMS Key Name: $kmsKeyName")
    println("- WIF Audience: $wifAudience")
    println("- Target SA Email: $targetSaEmail")
    println("- Token File Path: $tokenFilePath")
    println("---")

    // --- Step 0: Read ciphertext from GCS ---
    val ciphertextBase64 = ""

    // --- Step 1: Read OIDC Attestation Token from File ---
    println("Reading OIDC token from file: $tokenFilePath")
    val oidcToken = File(tokenFilePath).readText(StandardCharsets.UTF_8).trim()
    if (oidcToken.isEmpty()) {
      throw RuntimeException("Token file is empty: $tokenFilePath")
    }
    println("Successfully read OIDC token (${oidcToken.length} chars).")

    // --- Step 2: Exchange OIDC Token via STS for a Federated Token ---
    println("Exchanging OIDC token for federated token via STS...")
    val federatedToken = exchangeToken(oidcToken, wifAudience)
    println("Federated token obtained (length: ${federatedToken.length}).")

    // --- Step 3: Impersonate Target Service Account via IAM Credentials API ---
    println("Impersonating service account: $targetSaEmail")
    val saAccessToken = impersonateServiceAccount(federatedToken, targetSaEmail)
    println("Obtained access token for service account (length: ${saAccessToken.length}).")

    // --- Step 4: Build GoogleCredentials from the SA Access Token ---
    // Here we assume a 1-hour lifetime; in a production system, parse the expiration from the IAM
    // response.
    val expiration = Date(System.currentTimeMillis() + 3600 * 1000)
    val credentials = GoogleCredentials.create(AccessToken(saAccessToken, expiration))
    println("GoogleCredentials created using impersonated SA token.")

    // --- Step 5: Initialize KMS Client with these Credentials and Decrypt Data ---
    val kmsSettings =
      KeyManagementServiceSettings.newBuilder().setCredentialsProvider { credentials }.build()

    KeyManagementServiceClient.create(kmsSettings).use { kmsClient ->
      println("KMS client initialized successfully.")
      println("Attempting decryption using key: $kmsKeyName")

      // Decode the Base64 ciphertext
      val ciphertextBytes =
        try {
          Base64.getDecoder().decode(ciphertextBase64)
        } catch (e: IllegalArgumentException) {
          throw RuntimeException(
            "Invalid Base64 format for CIPHERTEXT_BASE64 environment variable.",
            e,
          )
        }
      if (ciphertextBytes.isEmpty()) {
        throw RuntimeException(
          "Decoded ciphertext is empty. Check CIPHERTEXT_BASE64 environment variable."
        )
      }
      println("Decoded ciphertext: ${ciphertextBytes.size} bytes.")

      val decryptResponse = kmsClient.decrypt(kmsKeyName, ByteString.copyFrom(ciphertextBytes))
      val decryptedData = decryptResponse.plaintext.toString(StandardCharsets.UTF_8)
      println("Decryption successful!")
      println("--- DECRYPTED DATA ---")
      println(decryptedData)
      println("--- END DECRYPTED DATA ---")
    }
  }

  /**
   * Exchanges the provided OIDC token for a federated access token using the Google STS endpoint.
   */
  fun exchangeToken(oidcToken: String, audience: String): String {
    val url = URL("https://sts.googleapis.com/v1/token")
    // Build form parameters for token exchange
    val params =
      mapOf(
        "grant_type" to "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token_type" to "urn:ietf:params:oauth:token-type:id_token",
        "subject_token" to oidcToken,
        "audience" to audience,
        "scope" to "https://www.googleapis.com/auth/cloud-platform",
        "requested_token_type" to "urn:ietf:params:oauth:token-type:access_token",
      )
    val postData =
      params.entries.joinToString("&") {
        "${URLEncoder.encode(it.key, "UTF-8")}=${URLEncoder.encode(it.value, "UTF-8")}"
      }

    with(url.openConnection() as HttpURLConnection) {
      requestMethod = "POST"
      doOutput = true
      setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
      outputStream.use { os -> os.write(postData.toByteArray(StandardCharsets.UTF_8)) }
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw RuntimeException("STS token exchange failed with HTTP code $responseCode")
      }
      val response = inputStream.bufferedReader().readText()
      // Extract the access token from the JSON response (a simple regex-based extraction)
      val tokenRegex = """"access_token"\s*:\s*"([^"]+)"""".toRegex()
      val matchResult =
        tokenRegex.find(response)
          ?: throw RuntimeException("access_token not found in STS response: $response")
      return matchResult.groupValues[1]
    }
  }

  /**
   * Impersonates the target service account by calling the IAM Credentials API. Uses the federated
   * token as the source credential.
   */
  fun impersonateServiceAccount(federatedToken: String, targetSaEmail: String): String {
    val url =
      URL(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/$targetSaEmail:generateAccessToken"
      )
    // Construct the JSON body for the impersonation request.
    val jsonBody =
      """
        {
            "scope": ["https://www.googleapis.com/auth/cloud-platform"],
            "lifetime": "3600s"
        }
    """
        .trimIndent()

    with(url.openConnection() as HttpURLConnection) {
      requestMethod = "POST"
      doOutput = true
      setRequestProperty("Content-Type", "application/json")
      setRequestProperty("Authorization", "Bearer $federatedToken")
      outputStream.use { os -> os.write(jsonBody.toByteArray(StandardCharsets.UTF_8)) }
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw RuntimeException("Service account impersonation failed with HTTP code $responseCode")
      }
      val response = inputStream.bufferedReader().readText()
      // Extract the access token from the response JSON.
      val tokenRegex = """"accessToken"\s*:\s*"([^"]+)"""".toRegex()
      val matchResult =
        tokenRegex.find(response)
          ?: throw RuntimeException("accessToken not found in impersonation response: $response")
      return matchResult.groupValues[1]
    }
  }
}
