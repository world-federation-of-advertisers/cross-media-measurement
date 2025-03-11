package org.wfanet.measurement.edpaggregator.requisitionfetcher

import io.grpc.Server
import io.grpc.ServerInterceptors
import java.net.URI
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
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import org.testcontainers.Testcontainers
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.toServerTlsContext

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

  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }

  @Before
  fun setUp() {
    // Push GCF image to docker registry
    Images()

    grpcServer = NettyServerBuilder
      .forAddress(InetSocketAddress("0.0.0.0", GRPC_SERVER_PORT))  // Bind to all interfaces
      .addService(ServerInterceptors.intercept(requisitionsServiceMock.bindService(), HeaderCapturingInterceptor()))
      .sslContext(serverCerts.toServerTlsContext())
      .build()
      .start()

    println("Started mock gRPC server on port ${grpcServer.port}")
    println("gRPC server bound to addresses: ${grpcServer.listenSockets.joinToString { it.toString() }}")

    // Expose the gRPC server port to containers
    Testcontainers.exposeHostPorts(grpcServer.port)

    // Configure the function container
    gcfContainer = GenericContainer(DockerImageName.parse(IMAGE_NAME)).apply {
      //TODO(): Figure out better way to emulate GCS
      withEnv("IS_TEST_CALL", "true")

      withExposedPorts(GCF_CONTAINER_PORT)
      withAccessToHost(true)
      withEnv("FUNCTION_TARGET", "org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcherFunction")

      // Configure gRPC connection to the mock server
      withEnv("TARGET", "host.testcontainers.internal:${grpcServer.port}")
      withEnv("CERT_HOST", "localhost")


      // Configure GCS
      withEnv("REQUISITIONS_GCS_PROJECT_ID", "test-project-id")
      withEnv("REQUISITIONS_GCS_BUCKET", "test-bucket")

        // Other configuration parameters
        withEnv("DATAPROVIDER_NAME", EDP_NAME)
        withEnv("PAGE_SIZE", "10")
        withEnv("STORAGE_PATH_PREFIX", "storage-path-prefix")

        // Certificate files
        withEnv("CERT_FILE_PATH", "/path/to/cert.pem")
        withEnv("PRIVATE_KEY_FILE_PATH", "/path/to/private-key.pem")
        withEnv("CERT_COLLECTION_FILE_PATH", "/path/to/cert-collection.pem")

        // Copy certificate files into the container
        withCopyFileToContainer(
          MountableFile.forHostPath(SECRETS_DIR.resolve("edp1_tls.pem")),
          "/path/to/cert.pem"
        )
        withCopyFileToContainer(
          MountableFile.forHostPath(SECRETS_DIR.resolve("edp1_tls.key")),
          "/path/to/private-key.pem"
        )
        withCopyFileToContainer(
          MountableFile.forHostPath(SECRETS_DIR.resolve("kingdom_root.pem")),
          "/path/to/cert-collection.pem"
        )
      }

    gcfContainer.start()
    println("Started function container at ${gcfContainer.host}:${gcfContainer.getMappedPort(GCF_CONTAINER_PORT)}")
  }

  @After
  fun cleanUp() {
    grpcServer.shutdown()
    grpcServer.awaitTermination(5, TimeUnit.SECONDS)
    gcfContainer.stop()
  }

  @Test
  fun `use RequisitionFetcherFunction in Docker container`() {
    // Give the container a bit of time to start up
    runBlocking {
      delay(60000)
    }

    val url = "http://${gcfContainer.host}:${gcfContainer.getMappedPort(GCF_CONTAINER_PORT)}"
    println("Testing Cloud Function at: $url")

    val client = HttpClient.newHttpClient()
    val getRequest = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET()
      .build()

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
    println("Container logs:\n${gcfContainer.logs}")
  }

  companion object {
    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_edp_aggregator_images.bash")
    private const val IMAGE_NAME = "localhost:5000/halo/requisitions/requisition-fetcher:latest"

    // Late initialize container so we can use grpcPort and hostIpAddress from the test instance
    lateinit var gcfContainer: GenericContainer<*>
    lateinit var grpcServer: Server
    private const val GRPC_SERVER_PORT = 8090
    // Google Cloud Function will listen on port 8080 by default
    private const val GCF_CONTAINER_PORT = 8080


    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private val REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = "MEASUREMENT_NAME"
      state = Requisition.State.UNFULFILLED
    }

    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!

    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp1_root.pem").toFile(),
      )
  }
}
