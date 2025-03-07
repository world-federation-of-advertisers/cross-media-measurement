package org.wfanet.measurement.edpaggregator.requisitionfetcher
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Paths
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import org.wfanet.measurement.common.k8s.testing.Processes
import org.wfanet.measurement.common.testing.chainRulesSequentially


class RequisitionFetcherFunctionTest {
  private class Images : TestRule {
    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          pushImages()
          base.evaluate()
        }
      }
    }
    private fun pushImages() {
      val pusherRuntimePath = checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(Paths.get("wfa_measurement_system").resolve(IMAGE_PUSHER_PATH))
      )
      Processes.runCommand(pusherRuntimePath.toString())
    }
  }
  val host: String
    get() = container.host
  val port: Int
    get() = container.getMappedPort(8085)
  @Before
  fun setUp() {
    container.start()
    container.isHostAccessible = true
  }
  @After
  fun cleanUp() {
    container.stop()
  }
  @Test
  fun `use RequisitionFetcherFunction in Docker container`() {
    val logs = container.logs
    println("Container logs: $logs")

    val execResult = container.execInContainer("ls", "-la", "/")
    println("Container file structure: ${execResult.stdout}")

    // Check if your class exists in the image
    val classCheck = container.execInContainer("find", "/", "-name", "*.class")
    println("Class files in container: ${classCheck.stdout}")

    val url = "http://$host:$port"
//    val client = HttpClient.newHttpClient()
//    val getRequest = HttpRequest.newBuilder().uri(URI.create(url)).GET().build()
//
//    // Send the sendHttpRequest using the client
//    val getResponse = client.send(getRequest, BodyHandlers.ofString())


    println("JOJI URL: ${url}")
    val connection = URL(url).openConnection() as HttpURLConnection
    // how do i call the google cloud function using http?
    try {
      connection.requestMethod = "GET" // or "POST", "PUT", etc.
      connection.connect()
      runBlocking {
        delay(1000000)
      }
      connection.connectTimeout = 30000 // 30 seconds
      connection.readTimeout = 30000 // 30 seconds
      val responseCode = connection.responseCode
      println("Response Code: $responseCode")

      val response = connection.inputStream.bufferedReader().use { it.readText() }
      println("Response: $response")
    } finally {
      connection.disconnect()
    }
  }
  companion object {
    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_local_images.bash")
    val imageName = "localhost:5000/halo/requisitions/requisition-fetcher:latest"
    val container = GenericContainer(DockerImageName.parse(imageName)).apply {
      withExposedPorts(8085)
      withCommand("java",
        "-Dcom.google.cloud.functions.invoker.runner.function=org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcherFunction",
        "com.google.cloud.functions.invoker.runner.Invoker")
      withEnv("TARGET", "test-target")
      withEnv("CERT_HOST", "localhost")
      withEnv("REQUISITIONS_GCS_PROJECT_ID", "test-gcs-project-id")
      withEnv("REQUISITIONS_GCS_BUCKET", "test-gcs-bucket")
      withEnv("DATAPROVIDER_NAME", "your-dataprovider-name")
      withEnv("PAGE_SIZE", "10") // Example value
      withEnv("STORAGE_PATH_PREFIX", "your-storage-path-prefix")
      withEnv("CERT_FILE_PATH", "/path/to/cert.pem")
      withEnv("PRIVATE_KEY_FILE_PATH", "/path/to/private-key.pem")
      withEnv("CERT_COLLECTION_FILE_PATH", "/path/to/cert-collection.pem")
      // how do i get the absolute path of these files dynamically?
      withCopyFileToContainer(MountableFile.forHostPath("/home/jojijacob/XMM/cross-media-measurement/src/main/k8s/testing/secretfiles/edp1_root.pem"), "/path/to/cert.pem")
      withCopyFileToContainer(MountableFile.forHostPath("/home/jojijacob/XMM/cross-media-measurement/src/main/k8s/testing/secretfiles/edp1_root.key"), "/path/to/private-key.pem")
      withCopyFileToContainer(MountableFile.forHostPath("/home/jojijacob/XMM/cross-media-measurement/src/main/k8s/testing/secretfiles/kingdom_root.pem"), "/path/to/cert-collection.pem")

    }
    private val tempDir = TemporaryFolder()
    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(tempDir, Images())
  }
}
