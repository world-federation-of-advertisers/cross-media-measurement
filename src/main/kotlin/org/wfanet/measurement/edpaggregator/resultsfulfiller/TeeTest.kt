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
import kotlinx.coroutines.runBlocking
import picocli.CommandLine

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
    logger.info("TeeTest.mainFunction")
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
