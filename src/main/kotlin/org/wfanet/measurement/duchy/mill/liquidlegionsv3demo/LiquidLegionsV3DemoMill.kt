package org.wfanet.measurement.duchy.mill.liquidlegionsv3demo

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.kms.v1.DecryptRequest
import com.google.cloud.kms.v1.KeyManagementServiceClient
import com.google.cloud.kms.v1.KeyManagementServiceSettings
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.StorageOptions
import com.google.protobuf.ByteString
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

class LiquidLegionsV3DemoMill() {
  fun run() {
    val blobBucket = "demo-tee"
    val blobKey = "encrypted_edp_data"
    val kmsKeyName =
      "projects/halo-cmm-dev/locations/us-central1/keyRings/tee-demo-key-ring/cryptoKeys/tee-demo-key-1"
    val wifProviderResourceName =
      "//iam.googleapis.com/projects/462363635192/locations/global/workloadIdentityPools/tee-demo-pool/providers/tee-demo-pool-provider"
    val targetSaEmail = "tee-demo-decrypter@halo-cmm-dev.iam.gserviceaccount.com"
    val oidcTokenFilePath = "/run/container_launcher/attestation_verifier_claims_token"

    println("Configuration:")
    println("- KMS Key Name: $kmsKeyName")
    println("- WIF Provider: $wifProviderResourceName")
    println("- Target SA Email: $targetSaEmail")
    println("- OIDC Token File Path: $oidcTokenFilePath")
    println("---")

    // --- Step 1: Read ciphertext from GCS ---
    val storage = StorageOptions.getDefaultInstance().service
    println("GCS client initialized successfully.")
    val blobId = BlobId.of(blobBucket, blobKey)
    val blob = storage.get(blobId)
    val ciphertextBytes = blob.getContent()
    println("ciphertext length:${ciphertextBytes.size}")

    // --- Steps 2: Configure Credentials using JSON ---
    val credentialConfigJson =
      """
      {
        "type": "external_account",
        "audience": "$wifProviderResourceName",
        "subject_token_type": "urn:ietf:params:oauth:token-type:id_token",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {
          "file": "$oidcTokenFilePath"
        },
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/$targetSaEmail:generateAccessToken"
      }
      """
        .trimIndent()

    println("Using External Account JSON configuration.")
    println(credentialConfigJson)

    // --- Step 3: Create GoogleCredentials from the JSON configuration
    val credentials =
      try {
        GoogleCredentials.fromStream(
          ByteArrayInputStream(credentialConfigJson.toByteArray(StandardCharsets.UTF_8))
        )
        // Optionally add scopes if needed, though often inferred or added during impersonation
        // .createScoped("https://www.googleapis.com/auth/cloud-platform")
      } catch (e: Exception) {
        println("Error creating GoogleCredentials from JSON: ${e.message}")
        throw RuntimeException("Failed to create GoogleCredentials", e)
      }
    println("GoogleCredentials created successfully .")

    // --- Step 4: Initialize KMS Client with these Credentials and Decrypt Data ---
    val kmsSettings =
      KeyManagementServiceSettings.newBuilder()
        .setCredentialsProvider { credentials } // Pass the credentials object here
        .build()
    try {
      KeyManagementServiceClient.create(kmsSettings).use { kmsClient ->
        println("KMS client initialized successfully with external account credentials.")
        println("Attempting decryption using key: $kmsKeyName")

        val decryptRequest =
          DecryptRequest.newBuilder()
            .setName(kmsKeyName)
            .setCiphertext(ByteString.copyFrom(ciphertextBytes))
            .build()

        val decryptResponse = kmsClient.decrypt(decryptRequest)
        val decryptedData = decryptResponse.plaintext.toStringUtf8() // Use toStringUtf8()

        println("Decryption successful!")
        println("--- DECRYPTED DATA ---")
        println(decryptedData)
        println("--- END DECRYPTED DATA ---")
      }
    } catch (e: Exception) {
      println("Error during KMS client creation or decryption: ${e.message}")
      e.printStackTrace()
      throw RuntimeException("KMS operation failed", e)
    }
  }
}
