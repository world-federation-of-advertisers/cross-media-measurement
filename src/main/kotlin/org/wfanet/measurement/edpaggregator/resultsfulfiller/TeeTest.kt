package org.wfanet.measurement.edpaggregator.resultsfulfiller

import java.util.logging.Logger
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretVersionName
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.common.flatten
import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.util.zip.CRC32C
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import com.google.auth.oauth2.ExternalAccountCredentials
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.StorageOptions
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.kms.v1.DecryptRequest
import com.google.cloud.kms.v1.KeyManagementServiceClient
import com.google.cloud.kms.v1.KeyManagementServiceSettings
import com.google.cloud.storage.BlobInfo
import com.google.protobuf.ByteString
import java.nio.file.Files
import java.util.*

@CommandLine.Command(name = "tee_test")
class TeeTest : Runnable {

  @CommandLine.ArgGroup(
    exclusive = false,
    multiplicity = "1..*",
    heading = "Single EDP certs\n"
  )
  lateinit var edpCerts: List<EdpFlags>
    private set

  class EdpFlags {
    @CommandLine.Option(names = ["--edp-name"], required = true, description = ["Name of the EDP"])
    lateinit var edpName: String

    @CommandLine.Option(
      names = ["--edp-cert-der"],
      required = true,
      description = ["Secret ID for the EDP cert"]
    )
    lateinit var certDerSecretId: String

    @CommandLine.Option(
      names = ["--edp-private-der"],
      required = true,
      description = ["Secret ID for the EDP private key"]
    )
    lateinit var privateDerSecretId: String
  }

  @CommandLine.Option(
    names = ["--test-flag"],
    description = ["A test flag to demonstrate Picocli input."],
    required = true
  )
  lateinit var testFlag: String
    private set

  val projectId = "halo-cmm-dev"
  val secretId = "edpa-tee-app-tls-key"
  val secretVersion = "latest"
  val outputPath = "/tmp/certs/edpa_tee_app_tls.key"

  override fun run(){
    writeToBucket()
    logger.info("TeeTest.mainFunction")
    access_kms()
    logger.info("TeeTest.run called with --test-flag=$testFlag")

    edpCerts.forEachIndexed { index, edp ->
      logger.info("EDP #$index:")
      logger.info("  edpName: ${edp.edpName}")
      logger.info("  certDerSecretId: ${edp.certDerSecretId}")
      logger.info("  privateDerSecretId: ${edp.privateDerSecretId}")
    }

    val secretValue = accessSecret(projectId, secretId, secretVersion)
    println("Secret value: $secretValue")

    saveToFile(secretValue, outputPath)

    if (checkFile(outputPath)) {
      println("File $outputPath exists and has non-zero size.")
    } else {
      println("File $outputPath does not exist or is empty.")
    }

    logger.info("PRE runBlocking")
    runBlocking {
      logger.info("INSIDE runBlocking")
      saveBytesToFile(
        getConfig("halo-cmm-dev", "gs://edpa-configs-storage-dev-bucket/test.txt"),
        "/tmp/config.txt"
      )

      if (checkFile("/tmp/config.txt")) {
        println("File CONFIG exists and has non-zero size.")
      } else {
        println("File CONFIG does not exist or is empty.")
      }
    }

    logger.info("POST runBlocking")

  }

  fun crc32c(data: ByteArray): Long {
    val crc = CRC32C()
    crc.update(data)
    return crc.value
  }

  fun access_kms() {
    val logs = mutableListOf<String>()
    val blobBucket = "edp-integration-storage-bucket-test"
    val blobKey = "primus_enc_customer_list.csv"
//    val kmsKeyName =
//      "projects/halo-cmm-dev/locations/us-central1/keyRings/tee-demo-key-ring/cryptoKeys/tee-demo-key-1"
    val keyName = "projects/halo-cmm-dev-edp/locations/global/keyRings/halo-cmm-dev-edp-enc-kr/cryptoKeys/halo-cmm-dev-edp-enc-key-"
    val workloadIdentityProvider = "//iam.googleapis.com/projects/472172784441/locations/global/workloadIdentityPools/edp-workload-identity-pool/providers/edp-wip-provider"

//    val wifProviderResourceName =
//      "//iam.googleapis.com/projects/462363635192/locations/global/workloadIdentityPools/tee-demo-pool/providers/tee-demo-pool-provider"
//    val targetSaEmail = "tee-demo-decrypter@halo-cmm-dev.iam.gserviceaccount.com"
    val targetServiceAccount = "primus-sa@halo-cmm-dev-edp.iam.gserviceaccount.com"
    val oidcTokenFilePath = "/run/container_launcher/attestation_verifier_claims_token"

    logger.info("Configuration:")
    logger.info("- KMS Key Name: $keyName")
    logger.info("- WIF Provider: $workloadIdentityProvider")
    logger.info("- Target SA Email: $targetServiceAccount")
    logger.info("- OIDC Token File Path: $oidcTokenFilePath")
    logger.info("---")

    logs += "Configuration:"
    logs += "- KMS Key Name: $keyName"
    logs += "- WIF Provider: $workloadIdentityProvider"
    logs += "- Target SA Email: $targetServiceAccount"
    logs += "- OIDC Token File Path: $oidcTokenFilePath"
    logs += "---"

    logger.info("--- Attestation Token Debug ---")
    try {
      val tokenFile = File(oidcTokenFilePath)
      if (tokenFile.exists()) {
        val jwtTokenString = tokenFile.readText(StandardCharsets.UTF_8)
        logger.info("Raw JWT Token String: $jwtTokenString")
        logs += "Raw JWT Token String: $jwtTokenString"
        logger.info("---")

        val parts = jwtTokenString.split('.')
        if (parts.size >= 2) { // A JWT has 3 parts, but we only need the payload (index 1)
          val payloadBase64Url = parts[1]
          // Base64Url requires a decoder that handles URL and filename safe alphabet.
          val decodedPayloadBytes = Base64.getUrlDecoder().decode(payloadBase64Url)
          val decodedPayloadJson = String(decodedPayloadBytes, StandardCharsets.UTF_8)

          logger.info("Decoded Attestation Token Payload (JSON):")
          // For more structured pretty printing, you could use a JSON library like Klaxon, Gson, or Jackson
          // Example: org.json.JSONObject(decodedPayloadJson).toString(2)
          logger.info(decodedPayloadJson)
          logs += "Decoded Attestation Token Payload (JSON): $decodedPayloadJson"
        } else {
          logs += "ERROR 1"
          logger.info("Error: OIDC token at '$oidcTokenFilePath' is not a valid JWT (expected at least 2 parts, found ${parts.size}).")
        }
      } else {
        logs += "ERROR 2"
        logger.info("Error: OIDC token file not found at '$oidcTokenFilePath'")
      }
    } catch (e: Exception) {
      logs += "ERROR 3"
      logger.info("Error reading or parsing OIDC token: ${e.message}")
      e.printStackTrace()
    }
    logger.info("--- End Attestation Token Debug ---")

    // --- Step 1: Read ciphertext from GCS ---
    val storage = StorageOptions.getDefaultInstance().service
    logger.info("GCS client initialized successfully.")
    val blobId = BlobId.of(blobBucket, blobKey)
    val blob = storage.get(blobId)
    val ciphertextBytes = blob.getContent()
    logger.info("ciphertext length:${ciphertextBytes.size}")

    // --- Steps 2: Configure Credentials using JSON ---
    val credentialConfigJson =
      """
      {
        "type": "external_account",
        "audience": "$workloadIdentityProvider",
        "subject_token_type": "urn:ietf:params:oauth:token-type:id_token",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {
          "file": "$oidcTokenFilePath"
        },
        "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/$targetServiceAccount:generateAccessToken"
      }
      """
        .trimIndent()

    logger.info("Using External Account JSON configuration.")
    logger.info(credentialConfigJson)
    logs += "CREDENTIALS: $credentialConfigJson"

    // --- Step 3: Create GoogleCredentials from the JSON configuration
    val credentials =
      try {
        GoogleCredentials.fromStream(
          ByteArrayInputStream(credentialConfigJson.toByteArray(StandardCharsets.UTF_8))
        )
        // Optionally add scopes if needed, though often inferred or added during impersonation
        // .createScoped("https://www.googleapis.com/auth/cloud-platform")
      } catch (e: Exception) {
        logs += "Error 4"
        logger.info("Error creating GoogleCredentials from JSON: ${e.message}")
        throw RuntimeException("Failed to create GoogleCredentials", e)
      }
    logger.info("GoogleCredentials created successfully .")

    // --- Step 4: Initialize KMS Client with these Credentials and Decrypt Data ---
    val kmsSettings =
      KeyManagementServiceSettings.newBuilder()
        .setCredentialsProvider { credentials } // Pass the credentials object here
        .build()
    try {
      KeyManagementServiceClient.create(kmsSettings).use { kmsClient ->
        logger.info("KMS client initialized successfully with external account credentials.")
        logger.info("Attempting decryption using key: $keyName")
        logs += "Attempting decryption using key: $keyName"

        val decryptRequest =
          DecryptRequest.newBuilder()
            .setName(keyName)
            .setCiphertext(ByteString.copyFrom(ciphertextBytes))
            .build()

        val decryptResponse = kmsClient.decrypt(decryptRequest)
        val decryptedData = decryptResponse.plaintext.toStringUtf8() // Use toStringUtf8()

        logs += "Description succesful!: $decryptedData"
        logger.info("Decryption successful!")
        logger.info("--- DECRYPTED DATA ---")
        logger.info(decryptedData)
        logger.info("--- END DECRYPTED DATA ---")
      }


      val content = logs.joinToString(separator = "\n") { it }.toByteArray(Charsets.UTF_8)

      // Define the blob in GCS
      val blobInfo = BlobInfo.newBuilder(blobBucket, "my-logs.txt")
        .setContentType("text/plain")
        .build()

      // Upload (this will overwrite existing object)
      storage.create(blobInfo, content)

    } catch (e: Exception) {
      logger.info("Error during KMS client creation or decryption: ${e.message}")
      e.printStackTrace()
      throw RuntimeException("KMS operation failed", e)
    }

    logger.info("ACCESSING KMS")


  }

  fun writeToBucket() {
    val bucketName = "edp-integration-storage-bucket-test"
    val objectName = "hello.txt"               // 🔁 Name of the object in GCS
    val localFilePath = "hello.txt"            // File to be created and uploaded

    val content = "Hello world".toByteArray(Charsets.UTF_8)

    // Initialize GCS client (uses default credentials)
    val storage = StorageOptions.getDefaultInstance().service


    // Define metadata and target location in GCS
    val blobInfo = BlobInfo.newBuilder(bucketName, objectName)
      .setContentType("text/plain")
      .build()

    // Upload to bucket
    storage.create(blobInfo, content)

    println("✅ Uploaded '$objectName' to bucket '$bucketName'")
  }

  suspend fun getConfig(
    projectId: String,
    blobUri: String,
  ): ByteArray {

    val uri = URI(blobUri)
    val rootDirectory = if (uri.scheme == "file") {
      val path = Paths.get(uri)
      path.parent?.toFile()
    } else {
      null
    }

    val storageClient = SelectedStorageClient(url = blobUri, rootDirectory = rootDirectory, projectId = projectId)
    val blobKey = uri.path.removePrefix("/")

    val blob = checkNotNull(storageClient.getBlob(blobKey)) {
      "Blob '$blobKey' not found at '$blobUri'"
    }

    return blob.read().flatten().toByteArray()
  }

  fun accessSecret(projectId: String, secretId: String, version: String): String {
    return SecretManagerServiceClient.create().use { client ->
      val secretVersionName = SecretVersionName.of(projectId, secretId, version)
      val request = AccessSecretVersionRequest.newBuilder()
        .setName(secretVersionName.toString())
        .build()

      val response = client.accessSecretVersion(request)
      response.payload.data.toStringUtf8()
    }
  }

  fun saveBytesToFile(content: ByteArray, path: String) {
    val file = File(path)
    file.parentFile?.mkdirs()
    File(path).writeBytes(content)
  }

  fun saveToFile(content: String, path: String) {
    val file = File(path)
    file.parentFile?.mkdirs()
    File(path).writeText(content)
  }

  fun checkFile(path: String): Boolean {
    val file = File(path)
    return file.exists() && file.length() > 0
  }

  companion object {

    private val logger = Logger.getLogger(this::class.java.name)
    @JvmStatic fun main(args: Array<String>){
      CommandLine(TeeTest()).execute(*args)
    }

  }

}
