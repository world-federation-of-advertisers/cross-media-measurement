package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.properties.Delegates
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.securecomputation.storage.requisitionBatch
import com.google.protobuf.Any
import io.netty.handler.ssl.ClientAuth
import java.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.TimeSource
import kotlin.time.toDuration
import kotlinx.coroutines.time.withTimeout
import org.junit.Rule
import org.wfanet.measurement.common.grpc.CommonServer

class RequisitionFetcherFunctionTest {
  // Temp folder to store Requisitions in test
  @Rule
  @JvmField val tempFolder = TemporaryFolder()
  // Mock of RequisitionsService
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }
  // Mock grpc server to handle calls tp RequisitionService
  private lateinit var grpcServer: CommonServer
  // Process for RequisitionFetcher Google cloud function
  private lateinit var functionProcess: RequisitionFetcherProcess

  @Before
  fun setUp() {
    // Start gRPC server with mock Requisitions service
    grpcServer = CommonServer.fromParameters(
      verboseGrpcLogging = true,
      certs = serverCerts,
      clientAuth = ClientAuth.REQUIRE,
      nameForLogging = "RequisitionFetcherServer",
      services = listOf(requisitionsServiceMock.bindService())
    ).start()

    println("Started mock gRPC server on port ${grpcServer.port}")

    // Start the RequisitionFetcherFunction process
    functionProcess = RequisitionFetcherProcess()
    runBlocking {
      val port = functionProcess.start(
        mapOf(
          "FUNCTION_TARGET" to "org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcherFunction",
          "REQUISITION_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "TARGET" to "localhost:${grpcServer.port}",
          "CERT_HOST" to "localhost",
          "REQUISITIONS_GCS_PROJECT_ID" to "test-project-id",
          "REQUISITIONS_GCS_BUCKET" to "test-bucket",
          "DATAPROVIDER_NAME" to DATA_PROVIDER_NAME,
          "PAGE_SIZE" to "10",
          "STORAGE_PATH_PREFIX" to STORAGE_PATH_PREFIX,
          "CERT_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.pem").toString(),
          "PRIVATE_KEY_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.key").toString(),
          "CERT_COLLECTION_FILE_PATH" to SECRETS_DIR.resolve("kingdom_root.pem").toString()
        )
      )
      println("Started RequisitionFetcher process on port $port")
    }
  }

  @After
  fun cleanUp() {
    functionProcess.close()
    grpcServer.shutdown()
  }

  @Test
  fun `test RequisitionFetcherFunction as local process`() {
    runBlocking {
      withTimeout(Duration.ofSeconds(10)) {
        while (functionProcess.started == false);
      }
    }
    val url = "http://localhost:${functionProcess.port}"
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

    val storedRequisitionPath = Paths.get(STORAGE_PATH_PREFIX, REQUISITION.name)
    val requisitionFile = tempFolder.root.toPath().resolve(storedRequisitionPath).toFile()
    assertThat(requisitionFile.exists()).isTrue()
    assertThat(requisitionFile.readByteString()).isEqualTo(REQUISITION_BATCH.toByteString())
  }

  /**
   * Wrapper for the RequisitionFetcher java_binary process.
   */
  class RequisitionFetcherProcess(
    private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO
  ) : AutoCloseable {

    private val startMutex = Mutex()
    @Volatile private lateinit var process: Process

    private var localPort by Delegates.notNull<Int>()

    val started: Boolean
      get() = this::process.isInitialized

    val port: Int
      get() {
        check(started) { "RequisitionFetcher process not started" }
        return localPort
      }

    /**
     * Starts the RequisitionFetcher process if it has not already been started.
     *
     * This suspends until the process is ready.
     *
     * @param env Environment variables to pass to the process
     * @returns The HTTP port the process is listening on
     */
    suspend fun start(env: Map<String, String>): Int {
      if (started) {
        return port
      }

      return startMutex.withLock {
        // Double-checked locking.
        if (started) {
          return@withLock port
        }

        return withContext(coroutineContext) {
          // Open a socket on `port`. This should reduce the likelihood that the port
          // is in use. Additionally, this will allocate a port if `port` is 0.
          localPort = ServerSocket(0).use { it.localPort }
          val runtimePath = getRuntimePath(FETCHER_BINARY_PATH)
          check(runtimePath != null && Files.exists(runtimePath)) {
            "$FETCHER_BINARY_PATH not found in runfiles"
          }

          val processBuilder = ProcessBuilder(
              runtimePath.toString(),
            // Add HTTP port configuration
            "--port",
            localPort.toString(),

            )
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.PIPE)

          // Set environment variables
          processBuilder.environment().putAll(env)

          // Start the process
          process = processBuilder.start()

          val reader = process.inputStream.bufferedReader()
          val readyPattern = "Serving function..."
          var isReady = false

          // Start a thread to read output
          Thread {
            try {
              while (true) {
                val line = reader.readLine() ?: break
                println("Process output: $line")

                // Check if the ready message is in the output
                if (line.contains(readyPattern)) {
                  isReady = true
                }
              }
            } catch (e: Exception) {
              if (process.isAlive) {
                println("Error reading process output: ${e.message}")
              }
            }
          }.start()

          // Wait for the ready message or timeout
          val timeoutMs = 10000L // 10 seconds timeout
          val startTime = TimeSource.Monotonic.markNow()

          while (!isReady) {
            yield()
            check(process.isAlive) { "Google Cloud Function stopped unexpectedly" }

            if (TimeSource.Monotonic.markNow().minus(startTime) > timeoutMs.toDuration(DurationUnit.SECONDS)) {
              throw IllegalStateException("Timeout waiting for Google Cloud Function to start")
            }
          }
          localPort
        }
      }
    }

    override fun close() {
      if (started) {
        process.destroy()
        try {
          if (!process.waitFor(5, TimeUnit.SECONDS)) {
            process.destroyForcibly()
          }
        } catch (e: InterruptedException) {
          process.destroyForcibly()
          Thread.currentThread().interrupt()
        }
      }
    }
  }

  companion object {
    private val FETCHER_BINARY_PATH = Paths.get(
      "wfa_measurement_system", "src", "test", "kotlin", "org", "wfanet", "measurement",
      "edpaggregator", "requisitionfetcher", "InvokeRequisitionFetcherFunction"
    )
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "${DATA_PROVIDER_NAME}/requisitions/foo"
    private val REQUISITION = requisition {
      name = REQUISITION_NAME
    }
    private val REQUISITION_BATCH = requisitionBatch {
      requisitions += listOf(Any.pack(REQUISITION))
    }
    private val STORAGE_PATH_PREFIX = "storage-path-prefix"
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
