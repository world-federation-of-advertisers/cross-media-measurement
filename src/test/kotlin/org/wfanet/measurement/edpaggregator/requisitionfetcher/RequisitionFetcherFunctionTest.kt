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
import org.wfanet.measurement.api.v2alpha.Requisition
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
import org.wfanet.measurement.common.grpc.CommonServer

class RequisitionFetcherFunctionTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }
  private lateinit var grpcServer: CommonServer
  private lateinit var functionProcess: RequisitionFetcherProcess
  private lateinit var tempFolder: TemporaryFolder

  @Before
  fun setUp() {
    // Set up temp folder to emulate GCS
    tempFolder = TemporaryFolder()
    tempFolder.create()

    // Start gRPC server with mock service
    grpcServer = CommonServer.fromParameters(
      verboseGrpcLogging = true,
      certs = serverCerts,
      clientAuth = ClientAuth.REQUIRE,
      nameForLogging = "RequisitionFetcherServer",
      services = listOf(requisitionsServiceMock.bindService())
    ).start()

    println("Started mock gRPC server on port ${grpcServer.port}")

    // Start the RequisitionFetcher process
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
          "DATAPROVIDER_NAME" to EDP_NAME,
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
    while(true) {
      if (functionProcess.started == true) {
        val url = "http://localhost:${functionProcess.port}"
        break
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
    val requistionFile = tempFolder.root.toPath().resolve(storedRequisitionPath).toFile()
    assertThat(requistionFile.exists()).isTrue()
    assertThat(requistionFile.readByteString()).isEqualTo(REQUISITION_BATCH.toByteString())
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
            "java",
            "-jar",
            runtimePath.toString()
          )
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.PIPE)

          // Set environment variables
          processBuilder.environment().putAll(env)

          // Add HTTP port configuration
          processBuilder.environment()["PORT"] = localPort.toString()

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
          val timeoutMs = 30000L // 30 seconds timeout
          val startTime = System.currentTimeMillis()
          while (!isReady) {
            yield()
            check(process.isAlive) { "Google Cloud Function stopped unexpectedly" }

            if (System.currentTimeMillis() - startTime > timeoutMs) {
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
      "wfa_measurement_system", "src", "main", "kotlin", "org", "wfanet", "measurement",
      "edpaggregator", "requisitionfetcher", "run_requisition_fetcher_function_deploy.jar"
    )
    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private val REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = "MEASUREMENT_NAME"
      state = Requisition.State.UNFULFILLED
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
