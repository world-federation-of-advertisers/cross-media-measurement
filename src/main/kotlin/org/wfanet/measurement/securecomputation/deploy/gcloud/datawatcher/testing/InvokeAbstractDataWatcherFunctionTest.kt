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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.testing

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Path
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfigs
import org.wfanet.measurement.securecomputation.deploy.gcloud.testing.CloudFunctionProcess

@RunWith(JUnit4::class)
abstract class InvokeAbstractDataWatcherFunctionTest() {

  abstract val functionBinaryPath: Path
  abstract val gcfTarget: String
  abstract val additionalFlags: Map<String, String>
  abstract val projectId: String
  abstract val topicId: String

  private lateinit var storageClient: GcsStorageClient
  /** Process for RequisitionFetcher Google cloud function. */
  private lateinit var functionProcess: CloudFunctionProcess

  @Before
  fun initStorageClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Before
  fun startInfra() {
    val dataWatcherConfigs = dataWatcherConfigs {
      configs += dataWatcherConfig {
        sourcePathRegex = "gs://$BUCKET/path-to-watch/(.*)"
        this.controlPlaneConfig = controlPlaneConfig {
          queueName = topicId
          appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }
    }
    /** Start the DataWatcherFunction process */
    functionProcess =
      CloudFunctionProcess(
        functionBinaryPath = functionBinaryPath,
        gcfTarget = gcfTarget,
        logger = logger,
      )
    runBlocking {
      val port =
        functionProcess.start(
          mapOf(
            "DATA_WATCHER_CONFIGS" to dataWatcherConfigs.toString(),
            "CONTROL_PLANE_PROJECT_ID" to projectId,
          ) + additionalFlags
        )
      logger.info("Started DataWatcher process on port $port")
    }
  }

  /** Cleans up resources after each test. */
  @After
  fun cleanUp() {
    functionProcess.close()
  }

  /** Tests the DataWatcherFunction as a local process. */
  @Test
  fun `verify DataWatcherFunction returns a 200`() {
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
        "name": "path-to-watch/some-blob",
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
        .header("ce-subject", "objects/path-to-watc1h/some-blob")
        .POST(HttpRequest.BodyPublishers.ofString(jsonData))
        .build()
    val getResponse = client.send(getRequest, BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")
    // Verify the function worked
    // Note that this always returns 200 in spite of the documentation saying that it will return
    // a 500 if the cloud function throws an exception.
    assertThat(getResponse.statusCode()).isEqualTo(200)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val BUCKET = "test-bucket"
  }
}
