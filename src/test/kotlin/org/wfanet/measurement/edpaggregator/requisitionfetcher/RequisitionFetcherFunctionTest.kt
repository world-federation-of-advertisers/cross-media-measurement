/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import io.netty.handler.ssl.ClientAuth
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.properties.Delegates
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.jetbrains.annotations.BlockingExecutor
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.readByteString

/** Test class for the RequisitionFetcherFunction. */
class RequisitionFetcherFunctionTest {
  /** Temp folder to store Requisitions in test. */
  @Rule
  @JvmField
  val tempFolder = TemporaryFolder()

  /** Mock of RequisitionsService. */
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }

  /** Grpc server to handle calls to RequisitionService. */
  private lateinit var grpcServer: CommonServer

  /** Process for RequisitionFetcher Google cloud function. */
  private lateinit var functionProcess: RequisitionFetcherProcess

  /** Sets up the infrastructure before each test. */
  @Before
  fun startInfra() {
    /** Start gRPC server with mock Requisitions service */
    grpcServer =
      CommonServer.fromParameters(
        verboseGrpcLogging = true,
        certs = serverCerts,
        clientAuth = ClientAuth.REQUIRE,
        nameForLogging = "RequisitionFetcherServer",
        services = listOf(requisitionsServiceMock.bindService()),
      )
        .start()
    logger.info("Started gRPC server on port ${grpcServer.port}")

    /** Start the RequisitionFetcherFunction process */
    functionProcess = RequisitionFetcherProcess()
    runBlocking {
      val port =
        functionProcess.start(
          mapOf(
            "REQUISITION_FILE_SYSTEM_PATH" to tempFolder.root.path,
            "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
            "KINGDOM_CERT_HOST" to "localhost",
            "REQUISITIONS_GCS_PROJECT_ID" to "test-project-id",
            "REQUISITIONS_GCS_BUCKET" to "test-bucket",
            "DATA_PROVIDER_NAME" to DATA_PROVIDER_NAME,
            "PAGE_SIZE" to "10",
            "STORAGE_PATH_PREFIX" to STORAGE_PATH_PREFIX,
            "CERT_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.pem").toString(),
            "PRIVATE_KEY_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.key").toString(),
            "CERT_COLLECTION_FILE_PATH" to SECRETS_DIR.resolve("kingdom_root.pem").toString(),
          )
        )
      logger.info("Started RequisitionFetcher process on port $port")
    }
  }

  /** Cleans up resources after each test. */
  @After
  fun cleanUp() {
    functionProcess.close()
    grpcServer.shutdown()
  }

  /** Tests the RequisitionFetcherFunction as a local process. */
  @Test
  fun `test RequisitionFetcherFunction as local process`() {
    val url = "http://localhost:${functionProcess.port}"
    logger.info("Testing Cloud Function at: $url")

    val client = HttpClient.newHttpClient()
    val getRequest = HttpRequest.newBuilder().uri(URI.create(url)).GET().build()
    val getResponse = client.send(getRequest, BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")
    // Verify the function worked
    assertThat(getResponse.statusCode()).isEqualTo(200)

    val storedRequisitionPath = Paths.get(STORAGE_PATH_PREFIX, REQUISITION.name)
    val requisitionFile = tempFolder.root.toPath().resolve(storedRequisitionPath).toFile()
    assertThat(requisitionFile.exists()).isTrue()
    assertThat(requisitionFile.readByteString()).isEqualTo(PACKED_REQUISITION.toByteString())
  }

  /** Wrapper for the RequisitionFetcher java_binary process. */
  class RequisitionFetcherProcess(
    private val coroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO
  ) : AutoCloseable {
    private val startMutex = Mutex()
    @Volatile
    private lateinit var process: Process
    private var localPort by Delegates.notNull<Int>()

    /** Indicates whether the process has started. */
    val started: Boolean
      get() = this::process.isInitialized

    /** Returns the port the process is listening on. */
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
     * @return The HTTP port the process is listening on
     */
    suspend fun start(env: Map<String, String>): Int {
      if (started) {
        return port
      }
      return startMutex.withLock {
        /** Double-checked locking. */
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

          val processBuilder =
            ProcessBuilder(
              runtimePath.toString(),
              /** Add HTTP port configuration */
              "--port",
              localPort.toString(),
              "--target",
              GCF_TARGET,
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
                logger.info("Process output: $line")
                /** Check if the ready message is in the output */
                if (line.contains(readyPattern)) {
                  isReady = true
                }
              }
            } catch (e: Exception) {
              if (process.isAlive) {
                logger.log(Level.WARNING, "Error reading process output: ${e.message}")
              }
            }
          }
            .start()

          // Wait for the ready message or timeout
          val timeout: Duration = 10.seconds
          val startTime = TimeSource.Monotonic.markNow()
          while (!isReady) {
            yield()
            check(process.isAlive) { "Google Cloud Function stopped unexpectedly" }
            if (startTime.elapsedNow() >= timeout) {
              throw IllegalStateException("Timeout waiting for Google Cloud Function to start")
            }
          }
          localPort
        }
      }
    }

    /** Closes the process if it has been started. */
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
    private val FETCHER_BINARY_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "test",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "requisitionfetcher",
        "InvokeRequisitionFetcherFunction",
      )
    private const val GCF_TARGET =
      "org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcherFunction"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "${DATA_PROVIDER_NAME}/requisitions/foo"
    private val REQUISITION = requisition { name = REQUISITION_NAME }
    private val PACKED_REQUISITION = Any.pack(REQUISITION)
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
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
