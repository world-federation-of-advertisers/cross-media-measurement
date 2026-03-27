/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.fileSystemStorage
import org.wfanet.measurement.config.edpaggregator.dataAvailabilityMonitorConfig
import org.wfanet.measurement.config.edpaggregator.dataAvailabilityMonitorConfigs
import org.wfanet.measurement.config.edpaggregator.modelLineConfig
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class DataAvailabilityMonitorFunctionTest {

  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  @get:Rule val tempFolder = TemporaryFolder()

  private fun createDoneBlob(
    storageClient: FileSystemStorageClient,
    edpPath: String,
    modelLine: String,
    date: String,
  ): Unit = runBlocking {
    val path = "$edpPath/model-line/$modelLine/$date/done"
    File(tempFolder.root, path).parentFile.mkdirs()
    storageClient.writeBlob(path, ByteString.copyFromUtf8("done"))
  }

  private fun startFunction(configText: String): Int {
    val configBucketDir = File(tempFolder.root, "configbucket")
    configBucketDir.mkdirs()
    File(configBucketDir, "monitor_config.textproto").writeText(configText)

    functionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = FUNCTION_BINARY_PATH,
        classTarget = GCF_TARGET,
      )

    return runBlocking {
      functionProcess.start(
        mapOf(
          "DATA_AVAILABILITY_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configBucketDir.absolutePath}",
          "CONFIG_BLOB_KEY" to "monitor_config.textproto",
          "OTEL_METRICS_EXPORTER" to "none",
          "OTEL_TRACES_EXPORTER" to "none",
          "OTEL_LOGS_EXPORTER" to "none",
        )
      )
    }
  }

  @Test
  fun `returns 200 when all model lines are healthy`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"

    // Create recent done blobs and data files (today-ish dates)
    for (day in 22..24) {
      createDoneBlob(storageClient, edpPath, "modelLineA", "2026-03-%02d".format(day))
      val dataPath = "$edpPath/model-line/modelLineA/2026-03-%02d/data_campaign_1".format(day)
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      runBlocking { storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data")) }
    }

    val config = dataAvailabilityMonitorConfigs {
      configs += dataAvailabilityMonitorConfig {
        storage = storageParams { fileSystem = fileSystemStorage {} }
        edpImpressionPath = edpPath
        modelLineConfigs += modelLineConfig {
          modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"
        }
        timeZone = "UTC"
        maxStaleDays = 3
      }
    }
    val port = startFunction(TextFormat.printer().printToString(config))

    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder().uri(URI.create("http://localhost:$port")).GET().build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(200)
    assertThat(response.body()).contains("healthy")
    functionProcess.close()
  }

  @Test
  fun `returns 500 when model line is stale`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"

    // Create old done blobs and data files (more than 3 days ago from a reasonable date)
    for (day in 1..3) {
      createDoneBlob(storageClient, edpPath, "modelLineA", "2026-03-%02d".format(day))
      val dataPath = "$edpPath/model-line/modelLineA/2026-03-%02d/data_campaign_1".format(day)
      File(tempFolder.root, dataPath).parentFile.mkdirs()
      runBlocking { storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data")) }
    }

    val config = dataAvailabilityMonitorConfigs {
      configs += dataAvailabilityMonitorConfig {
        storage = storageParams { fileSystem = fileSystemStorage {} }
        edpImpressionPath = edpPath
        modelLineConfigs += modelLineConfig {
          modelLine = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"
        }
        timeZone = "UTC"
        maxStaleDays = 3
      }
    }
    val port = startFunction(TextFormat.printer().printToString(config))

    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder().uri(URI.create("http://localhost:$port")).GET().build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("issues detected")
    functionProcess.close()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val FUNCTION_BINARY_PATH =
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
        "dataavailability",
        "testing",
        "InvokeDataAvailabilityMonitorFunction",
      )
    private const val GCF_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilityMonitorFunction"
  }
}
