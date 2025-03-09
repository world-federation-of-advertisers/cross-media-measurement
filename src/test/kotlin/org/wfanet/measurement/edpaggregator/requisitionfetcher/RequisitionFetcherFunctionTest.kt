package org.wfanet.measurement.edpaggregator.requisitionfetcher
import io.grpc.Server
import io.grpc.ServerInterceptors
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
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.common.grpc.testing.mockService
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.requisition
import io.grpc.netty.NettyServerBuilder
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import org.junit.Rule
import org.testcontainers.Testcontainers
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.toServerTlsContext


class RequisitionFetcherFunctionTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("edp1_root.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("edp1_root.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("kingdom_root.pem").toFile(),
    )

  private val grpcServerPort = 8090


  private val grpcServer: Server =
    NettyServerBuilder
      .forAddress(InetSocketAddress("0.0.0.0", grpcServerPort))  // Bind to all interfaces
      .addService(ServerInterceptors.intercept(requisitionsServiceMock.bindService(), HeaderCapturingInterceptor()))
      .sslContext(serverCerts.toServerTlsContext())
      .build()
      .start()

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
    get() = container.getMappedPort(8080)
  @Before
  fun setUp() {
    println("Started mock gRPC server on port ${grpcServer.port}")
    println("Started mock gRPC server on socket ${grpcServer.listenSockets[0]}")
    println("gRPC server bound to addresses: ${grpcServer.listenSockets.joinToString { it.toString() }}")

    container = GenericContainer(DockerImageName.parse(imageName)).apply {
      // Google Cloud Function will listen on port 8080 by default
      withExposedPorts(8080)
      withAccessToHost(true)
//      withNetwork(network)
      withEnv("FUNCTION_TARGET", "org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcherFunction")

      // We'll set the TARGET environment variable in setUp() when we know the hostIpAddress and grpcPort
      withEnv("TARGET", "host.testcontainers.internal:${grpcServer.port}")
      withEnv("CERT_HOST", "host.testcontainers.internal")
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
    Testcontainers.exposeHostPorts(grpcServer.port)
    container.start()
    container.isHostAccessible = true
  }
  @After
  fun cleanUp() {
    grpcServer.shutdown()
    grpcServer.awaitTermination(5, TimeUnit.SECONDS)
    container.stop()
  }
  @Test
  fun `use RequisitionFetcherFunction in Docker container`() {
    // Give the container a bit of time to start up
    runBlocking {
      delay(2000)
    }

    // Test the HTTP endpoint of your function
    val url = "http://${container.host}:${container.getMappedPort(8080)}"
    println("Testing Cloud Function at: $url")

    // Create a simple client without TLS expectations
    val client = HttpClient.newHttpClient()
    val getRequest = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET()
      .build()

    // Wrap in try-catch to better handle errors
    try {
      val getResponse = client.send(getRequest, BodyHandlers.ofString())
      println("Response status: ${getResponse.statusCode()}")
      println("Response body: ${getResponse.body()}")

      // Verify the function worked
      assert(getResponse.statusCode() == 200)
    } catch (e: Exception) {
      println("Error calling cloud function: ${e.message}")
      e.printStackTrace()
      throw e
    }

    // Print container logs for debugging
    println("Container logs:\n${container.logs}")
  }

  @Test
  fun `test container connectivity basics`() {
    // Start with a simple container setup
    val simpleContainer = GenericContainer(DockerImageName.parse("alpine:latest")).apply {
      withExposedPorts(80)
      withCommand("sh", "-c", "echo 'HTTP/1.1 200 OK\r\n\r\nHello World' | nc -l -p 80")
    }

    try {
      simpleContainer.start()

      // Test connection to the simple container
      val url = "http://${simpleContainer.host}:${simpleContainer.getMappedPort(80)}"
      println("Testing simple HTTP container at: $url")

      val client = HttpClient.newHttpClient()
      val getRequest = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .GET()
        .build()

      val response = client.send(getRequest, BodyHandlers.ofString())
      println("Simple container response: ${response.body()}")

      // If this works, the basic container connectivity is fine
    } finally {
      simpleContainer.stop()
    }
  }

  @Test
  fun `debug cloud function container`() {
    // Add more debug environment variables
    container = GenericContainer(DockerImageName.parse(imageName)).apply {
      withExposedPorts(8080)
      withAccessToHost(true)

      // Enable more verbose logging
      withEnv("FUNCTION_TARGET", "org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcherFunction")
      withEnv("DEBUG", "true") // Add if your function framework supports debug mode

      // Try with plaintext first, to isolate TLS issues
      withEnv("USE_PLAINTEXT", "true") // Add this to your function code to conditionally use plaintext

      // Other environment variables as before
      // ...
    }

    Testcontainers.exposeHostPorts(grpcServerPort)
    container.start()

    // Check if container actually started
    assert(container.isRunning) { "Container failed to start" }

    // Print container logs immediately
    println("Container startup logs:\n${container.logs}")

    // Run a few diagnostic commands inside the container
    val envResult = container.execInContainer("env")
    println("Container environment:\n${envResult.stdout}")

    val netstatResult = container.execInContainer("netstat", "-tuln")
    println("Container network status:\n${netstatResult.stdout}")

    val checkHostResult = container.execInContainer("ping", "-c", "1", "host.testcontainers.internal")
    println("Container host ping:\n${checkHostResult.stdout}")

    // Try to directly call the function inside container
    val curlResult = container.execInContainer("curl", "-v", "http://localhost:8080")
    println("Container self-curl:\n${curlResult.stdout}\n${curlResult.stderr}")

    // Wait a bit then check for more logs
    runBlocking { delay(5000) }
    println("Container logs after waiting:\n${container.logs}")
  }

  companion object {
    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_edp_aggregator_images.bash")
    val imageName = "localhost:5000/halo/requisitions/requisition-fetcher:latest"

    private val tempDir = TemporaryFolder()

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(tempDir, Images())

    // Late initialize container so we can use grpcPort and hostIpAddress from the test instance
    lateinit var container: GenericContainer<*>


    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private val REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = "MEASUREMENT_NAME"
      state = Requisition.State.UNFULFILLED

    }

    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("/home/jojijacob/XMM/cross-media-measurement/src/main/k8s/testing/secretfiles")
      )!!
  }

  init {
  }
}
