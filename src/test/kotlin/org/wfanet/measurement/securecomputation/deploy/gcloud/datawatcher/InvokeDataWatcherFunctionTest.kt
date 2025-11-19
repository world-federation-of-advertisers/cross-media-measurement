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

import com.google.common.truth.Truth.assertThat
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Paths
import java.util.*
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
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
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule
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
    storageEmulator.createBucket(BUCKET)
    GcsStorageClient(storageEmulator.storage, BUCKET)
  }

  @After
  fun deleteBucket() {
    storageEmulator.deleteBucketRecursive(BUCKET)
  }

  val tmpCreds =
    File.createTempFile("test-sa", ".json").apply {
      writeText(
        String(Base64.getDecoder().decode(GOOGLE_FAKE_CREDENTIAL), Charsets.UTF_8).trimIndent()
      )
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
            "GOOGLE_APPLICATION_CREDENTIALS" to tmpCreds.absolutePath,
            "CONTROL_PLANE_PROJECT_ID" to projectId,
            "CONTROL_PLANE_TARGET" to "localhost:${grpcServer.port}",
            "CONTROL_PLANE_CERT_HOST" to "localhost",
            "CONTROL_PLANE_CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
            "CERT_FILE_PATH" to SECRETS_DIR.resolve("edp7_tls.pem").toString(),
            "PRIVATE_KEY_FILE_PATH" to SECRETS_DIR.resolve("edp7_tls.key").toString(),
            "CERT_COLLECTION_FILE_PATH" to SECRETS_DIR.resolve("kingdom_root.pem").toString(),
            "EDPA_CONFIG_STORAGE_BUCKET" to DATA_WATCHER_CONFIG_FILE_SYSTEM_PATH,
            "OTEL_METRICS_EXPORTER" to "none",
            "OTEL_TRACES_EXPORTER" to "none",
            "OTEL_LOGS_EXPORTER" to "none",
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
    private val DATA_WATCHER_CONFIG_FILE_SYSTEM_PATH =
      "file://" +
        getRuntimePath(
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
          )
        )!!
    private const val BUCKET = "test-bucket"
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem"),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key"),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp7_root.pem"),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val GOOGLE_FAKE_CREDENTIAL =
      "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAidGVzdC1wcm9qZWN0LWlkIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAidGVzdC1rZXktaWQiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS0KTUlJRXZBSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS1l3Z2dTaUFnRUFBb0lCQVFDeWxvdmUwb1JiVDVZQQpoTVBoUkU4dG5nS2gwT1dtbUJ1bTNhdk1BVTlXMkVhb3BReFRPaGJCc2dWRS9GRDkrY1Z0dEpHVVhNL1dKYmdXCmNlSm1DQ2ExVGZSNWpEZWRPMFNPZHhqenJlSVNoNzB2UzNycjVkT210U0lJUk5ZSXhuNGFHRllHVzZjZXBrWWkKWTBlMVgxT0NjcStBdDg0bWUxWWVldXByb0VnL01SSDA1V0hkVEtDbHc5aWNhcHpPQjIrVUF2TzdvdEJpbFA1YQpyYzNWNHB3dzBQSWdIem91M1d3cytHeU5qL3ZFRjMzc0RCTS9EMCsrOEh6MTRzakVxRTV3T0Z4d2tGelcxa0VkClZVbThWaEFocEVPcVBUbjZIQ0FmcUEveTNscVpIYzhQNHZMMTJHc2p3WnBOQ0pEcjZYcjRMSHdvaXNuQWFicUMKTTh0YWg4cFhBZ01CQUFFQ2dnRUFRUmZsZUh5eitKSk1JdmxCYW1qYkVVNEFPSm5yTXV0TFhPbDhWbm45d0xKSgpJdXd4ejE0amNFdGlaMUF1ZHp3a2pZV2M4SDVaMVB6Zm1mSzlxaUg3ZGVjcG5tb1ExVk1HZklVRmg4QlA0Q0F3CndUM2FXb1JsUG1UVU9ENWE0MHp5SnJITEhUc244V0I2dk1zQ0ZxWmR5blRoNm1GVWx5c3FheWF6TGpKNFV1dUUKU2ZwV0o4Ly9jTThnY1RZSndEQjZaenFsaFZuYWVmaElmY3MyOXJ0aW4valBLZFN0RW14R0w4bUNxMWVySWRTNwpCKzRRSm0zMTNOUTA4cVpyaHQvdkhCVTBqY1NnZS85cW1wUjV0Y2VaYWl6L3VKL2VsSWsvaUlWS1hPMmxMd3FICjNKNnJNZUNyaG5DczR5UkF2UmFRWTBtRCtkeVlza2tWOFpraFZSM3g3UUtCZ1FEZ1ltTXYzVVNBS1NtVEFCUEYKc2pmNUNkSXUyQzExNzcxelExNEswS0ZEN09WMVhES2t3TThRcmYzSjJmemRsYkJxZDNYa3c0c3ZQM0V2bjY5NAp6MUU0bTRHYnI4aVBvWDhYUzd2ZmZrTXpkdUZwSzA0UEZxaU1vT1NlZW9NU09odFZKbEFpM2dUeFBVVHVMamUyCmZwdlNjVDRWWEw4OGR1NE1hR1JQODd2MnhRS0JnUURMd0VZVk1RVzlXNElUYUx0d3Myb2puRFhmZmtWUkQvOUkKTWdGU2NLYWtlWWdwWXVhQXZzY0dSNmdDT21mZ0hxTzVkQUhGNXRRaGZ6a20wbHR2UE9MNmdLeUZyb2h6aHlwRAppTE82TFZkZUlTOFFLNHpvVE01SjQzdVgwQmlENUF3SUNIRjFYNzVJMTdDdTdoOUJCdVVPWmU5WTRFcFpLcW5rCjdvL0dzaU51YXdLQmdEb3ZDUTFHVVJieWxZYzZ3K1hGdXVIbS9BdU5udXd3Q0c0MUQ5TzZHYmNsWExLNy81M2QKS1ZSbjZhRkgxMXdXRHJMczJ6TkF5WHlzOU1xbW03ZTErcGUxS3p5VnJtb2dOSFp6K3ZtUElobmNQOE1ucVl3YwpFZ1MxUzlNVWJaeHlXTmdSb1VJSlZEckI2bmZnb0MzQVV1T1UzY1pvVUdaN2FHcnJQdWZFaWY4SkFvR0FUeEt0Cm9DZklUSGFwQXBOUXV5cEY5TS80OEdWMnpVRGlGOGlnVHJnOVUvTitibUZkaDNXQ2srTlhScFlZSGhpRi9jRVIKdHhZZ0dXZmdiRHFURlphUm1CbzcyaDJrQXdIZjJ4bkFkbTZHUzVlaFJpdEFvaDY4cUZ4S2FONXZ4Uy9KbzR4egpTOVArYXhLYUZTbUFvNkhqWHpVY01HZkxNdE9sMzV6ZDI0VGl6MFVDZ1lCV1ZkdzhlR1VjMTRxOUVaR0R6alRmCmIzWDY5Zit3OVAwMEhYVzU1SnEwTzZxL1c1bndLZHNtQk9aQ0JpeGJtOUczSGo1WFhVV1pqMDJ2UmVmdGhpbjcKNU1Kd0VuZWtkL0lSc3ZESnRBVGloM0JmRTVXS294RjV5UjRQeGI2VzB4OHlIb0J5MHAvYzlhRGxQWDhGVFo3UApqYVVXd2hMenpPa3BVMWhkQlVEKzBnPT0KLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLSIsCiAgImNsaWVudF9lbWFpbCI6ICJ0ZXN0LXNhQHRlc3QtcHJvamVjdC1pZC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgImNsaWVudF9pZCI6ICIxMjM0NTY3ODkwIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS90ZXN0LXNhQHRlc3QtcHJvamVjdC1pZC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIKfQ=="

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()
  }
}
