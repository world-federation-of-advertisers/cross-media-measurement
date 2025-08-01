package org.wfanet.measurement.edpaggregator.resultsfulfiller

import java.util.logging.Logger
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest
import com.google.cloud.secretmanager.v1.SecretVersionName
import java.io.File
import picocli.CommandLine

@CommandLine.Command(name = "tee_test")
class TeeTest : Runnable {

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

    val secretValue = accessSecret(projectId, secretId, secretVersion)
    println("Secret value: $secretValue")

    saveToFile(secretValue, outputPath)

    if (checkFile(outputPath)) {
      println("File $outputPath exists and has non-zero size.")
    } else {
      println("File $outputPath does not exist or is empty.")
    }

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
