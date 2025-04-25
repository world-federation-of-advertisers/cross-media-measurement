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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import io.netty.handler.ssl.ClientAuth
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess

/** Test class for the RequisitionFetcherFunction. */
class RequisitionFetcherFunctionTest {
  /** Temp folder to store Requisitions in test. */
  @Rule @JvmField val tempFolder = TemporaryFolder()

  /** Mock of RequisitionsService. */
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }

  /** Grpc server to handle calls to RequisitionService. */
  private lateinit var grpcServer: CommonServer

  /** Process for RequisitionFetcher Google cloud function. */
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  /** Sets up the infrastructure before each test. */
  @Before
  fun startInfra() {
    /** Start gRPC server with mock Requisitions service */
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "RequisitionsServiceServer",
          services = listOf(requisitionsServiceMock.bindService()),
        )
        .start()
    logger.info("Started gRPC server on port ${grpcServer.port}")

    /** Start the RequisitionFetcherFunction process */
    functionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = FETCHER_BINARY_PATH,
        classTarget = GCF_TARGET,
      )
    runBlocking {
      val port =
        functionProcess.start(
          mapOf(
            "REQUISITION_FETCHER_CONFIG_RESOURCE_PATH" to
              Paths.get(
                  "main",
                  "kotlin",
                  "org",
                  "wfanet",
                  "measurement",
                  "edpaggregator",
                  "deploy",
                  "gcloud",
                  "requisitionfetcher",
                  "testing",
                  "requisition_fetcher_config.textproto",
                )
                .toString(),
            "REQUISITION_FILE_SYSTEM_PATH" to tempFolder.root.path,
            "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
            "KINGDOM_CERT_HOST" to "localhost",
            "PAGE_SIZE" to "10",
            "STORAGE_PATH_PREFIX" to STORAGE_PATH_PREFIX,
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

  companion object {
    private val FETCHER_BINARY_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "deploy",
        "gcloud",
        "requisitionfetcher",
        "testing",
        "InvokeRequisitionFetcherFunction",
      )
    private const val GCF_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher.RequisitionFetcherFunction"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"
    private val REQUISITION = requisition { name = REQUISITION_NAME }
    private val PACKED_REQUISITION = Any.pack(REQUISITION)
    private val STORAGE_PATH_PREFIX = "edp1"
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
