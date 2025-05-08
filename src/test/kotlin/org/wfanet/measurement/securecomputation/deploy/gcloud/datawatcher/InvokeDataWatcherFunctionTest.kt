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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

@RunWith(JUnit4::class)
class InvokeDataWatcherFunctionTest() {

  val functionBinaryPath =
    Paths.get(
      "wfa_measurement_system",
      "src",
      "main",
      "kotlin",
      "org",
      "wfanet",
      "measurement",
      "securecomputation",
      "deploy",
      "gcloud",
      "datawatcher",
      "testing",
      "InvokeDataWatcherFunction",
    )
  val gcfTarget =
    "org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction"
  val additionalFlags = emptyMap<String, String>()
  val projectId = "some-project-id"
  val topicId = "some-topic-id"

  private lateinit var storageClient: GcsStorageClient
  private lateinit var grpcServer: CommonServer
  /** Process for Google cloud function. */
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  private val workItemsServiceMock: WorkItemsCoroutineImplBase = mockService {
    onBlocking { createWorkItem(any()) }.thenReturn(workItem { name = "some-work-item-name" })
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(workItemsServiceMock) }

  @Before
  fun initStorageClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Before
  fun startInfra() {
    /** Start gRPC server with mock Work Items service */
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "WorkItemsServer",
          services = listOf(workItemsServiceMock.bindService()),
        )
        .start()
    logger.info("Started gRPC server on port ${grpcServer.port}")
    /** Start the DataWatcherFunction process */
    functionProcess =
      FunctionsFrameworkInvokerProcess(javaBinaryPath = functionBinaryPath, classTarget = gcfTarget)
    runBlocking {
      val port =
        functionProcess.start(
          mapOf(
            "DATA_WATCHER_CONFIG_JAR_RESOURCE_PATH" to
              Paths.get("securecomputation", "datawatcher", "data_watcher_config.textproto")
                .toString(),
            "CONTROL_PLANE_PROJECT_ID" to projectId,
            "CONTROL_PLANE_TARGET" to "localhost:${grpcServer.port}",
            "CONTROL_PLANE_CERT_HOST" to "localhost",
            "CONTROL_PLANE_CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
            "CERT_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.pem").toString(),
            "PRIVATE_KEY_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.key").toString(),
            "CERT_COLLECTION_FILE_PATH" to SECRETS_DIR.resolve("kingdom_root.pem").toString(),
          ) + additionalFlags
        )
      logger.info("Started DataWatcher process on port $port")
    }
  }

  /** Cleans up resources after each test. */
  @After
  fun cleanUp() {
    functionProcess.close()
    grpcServer.shutdown()
  }

  /** Tests the DataWatcherFunction as a local process. */
  @Test
  fun `verify DataWatcherFunction returns a 200 and creates work item`() {
    val url = "http://localhost:${functionProcess.port}"
    logger.info("Testing Cloud Function at: $url")
    val client = HttpClient.newHttpClient()
    val jsonData =
      """
      {
        "bucket": "$BUCKET",
        "contentType": "text/plain",
        "kind": "storage#object",
        "md5Hash": "...",
        "metageneration": "1",
        "name": "control-plane-sink-path-to-watch/some-blob",
        "size": "352",
        "storageClass": "MULTI_REGIONAL",
        "timeCreated": "2020-04-23T07:38:57.230Z",
        "timeStorageClassUpdated": "2020-04-23T07:38:57.230Z",
        "updated": "2020-04-23T07:38:57.230Z"
      }
    """
        .trimIndent()
    val getRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Content-Type", "application/json")
        .header("ce-id", "123451234512345")
        .header("ce-specversion", "1.0")
        .header("ce-time", "2020-01-02T12:34:56.789Z")
        .header("ce-type", "google.cloud.storage.object.v1.finalized")
        .header("ce-source", "//storage.googleapis.com/projects/_/buckets/$BUCKET")
        .header("ce-subject", "objects/control-plane-sink-path-to-watch/some-blob")
        .POST(HttpRequest.BodyPublishers.ofString(jsonData))
        .build()
    val getResponse = client.send(getRequest, BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    // Verify the function worked
    // Note that this always returns 200 in spite of the documentation saying that it will return
    // a 500 if the cloud function throws an exception.
    assertThat(getResponse.statusCode()).isEqualTo(200)

    val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()

    verifyBlocking(workItemsServiceMock, times(1)) {
      createWorkItem(createWorkItemRequestCaptor.capture())
    }
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()
    private const val BUCKET = "test-bucket"
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem"),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key"),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp1_root.pem"),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
