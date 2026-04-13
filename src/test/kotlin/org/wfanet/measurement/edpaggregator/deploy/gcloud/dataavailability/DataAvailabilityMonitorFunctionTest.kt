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
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.runBlocking
import org.junit.After
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

  @After
  fun tearDown() {
    if (::functionProcess.isInitialized) {
      functionProcess.close()
    }
  }

  private fun writeCompletedDate(
    storageClient: FileSystemStorageClient,
    edpPath: String,
    modelLine: String,
    date: LocalDate,
  ): Unit = runBlocking {
    val dateString = date.toString()
    val dataPath = "$edpPath/model-line/$modelLine/$dateString/data_campaign_1"
    File(tempFolder.root, dataPath).parentFile.mkdirs()
    storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))

    val donePath = "$edpPath/model-line/$modelLine/$dateString/done"
    storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
  }

  private fun writeDoneOnlyDate(
    storageClient: FileSystemStorageClient,
    edpPath: String,
    modelLine: String,
    date: LocalDate,
  ): Unit = runBlocking {
    val dateString = date.toString()
    val donePath = "$edpPath/model-line/$modelLine/$dateString/done"
    File(tempFolder.root, donePath).parentFile.mkdirs()
    storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))
  }

  private fun writeDataOnlyDate(
    storageClient: FileSystemStorageClient,
    edpPath: String,
    modelLine: String,
    date: LocalDate,
  ): Unit = runBlocking {
    val dateString = date.toString()
    val dataPath = "$edpPath/model-line/$modelLine/$dateString/data_campaign_1"
    File(tempFolder.root, dataPath).parentFile.mkdirs()
    storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
  }

  private fun writeLateArrivingDate(
    storageClient: FileSystemStorageClient,
    edpPath: String,
    modelLine: String,
    date: LocalDate,
  ): Unit = runBlocking {
    val dateString = date.toString()
    val donePath = "$edpPath/model-line/$modelLine/$dateString/done"
    File(tempFolder.root, donePath).parentFile.mkdirs()
    storageClient.writeBlob(donePath, ByteString.copyFromUtf8("done"))

    Thread.sleep(1100)

    val dataPath = "$edpPath/model-line/$modelLine/$dateString/data_campaign_1"
    storageClient.writeBlob(dataPath, ByteString.copyFromUtf8("data"))
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

  private fun createConfig(
    edpPath: String,
    modelLines: List<String> = listOf(MODEL_LINE_RESOURCE_NAME),
    maxStaleDays: Int? = 3,
  ) = dataAvailabilityMonitorConfigs {
    configs += dataAvailabilityMonitorConfig {
      storage = storageParams { fileSystem = fileSystemStorage {} }
      edpImpressionPath = edpPath
      modelLines.forEach { modelLineName ->
        modelLineConfigs += modelLineConfig { modelLine = modelLineName }
      }
      timeZone = "UTC"
      if (maxStaleDays != null) {
        this.maxStaleDays = maxStaleDays
      }
    }
  }

  private fun invokeFunction(port: Int): HttpResponse<String> {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder().uri(URI.create("http://localhost:$port")).GET().build()
    return client.send(request, HttpResponse.BodyHandlers.ofString())
  }

  @Test
  fun `returns 200 when all model lines are healthy`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"
    val today = LocalDate.now(ZoneId.of("UTC"))

    // Keep the latest upload comfortably within the staleness threshold.
    for (daysAgo in 2 downTo 0) {
      writeCompletedDate(storageClient, edpPath, MODEL_LINE_ID, today.minusDays(daysAgo.toLong()))
    }

    val config = createConfig(edpPath)
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(200)
    assertThat(response.body()).contains("healthy")
  }

  @Test
  fun `returns 500 when model line is stale`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"
    val today = LocalDate.now(ZoneId.of("UTC"))

    // Leave a large margin so the assertion stays valid even if the date changes mid-test.
    for (daysAgo in 10 downTo 8) {
      writeCompletedDate(storageClient, edpPath, MODEL_LINE_ID, today.minusDays(daysAgo.toLong()))
    }

    val config = createConfig(edpPath)
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("issues detected")
  }

  @Test
  fun `returns 500 when model line has gap`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"
    val today = LocalDate.now(ZoneId.of("UTC"))

    writeCompletedDate(storageClient, edpPath, MODEL_LINE_ID, today.minusDays(2))
    writeCompletedDate(storageClient, edpPath, MODEL_LINE_ID, today)

    val config = createConfig(edpPath = edpPath)
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("issues detected")
  }

  @Test
  fun `returns 500 when date folder is missing done blob`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"
    val today = LocalDate.now(ZoneId.of("UTC"))

    writeDataOnlyDate(storageClient, edpPath, MODEL_LINE_ID, today)

    val config = createConfig(edpPath = edpPath)
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("issues detected")
  }

  @Test
  fun `returns 500 when done blob has no corresponding data`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"
    val today = LocalDate.now(ZoneId.of("UTC"))

    writeDoneOnlyDate(storageClient, edpPath, MODEL_LINE_ID, today)

    val config = createConfig(edpPath = edpPath)
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("issues detected")
  }

  @Test
  fun `returns 500 when late arriving files are detected`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"
    val today = LocalDate.now(ZoneId.of("UTC"))

    writeLateArrivingDate(storageClient, edpPath, MODEL_LINE_ID, today)

    val config = createConfig(edpPath = edpPath)
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("issues detected")
  }

  @Test
  fun `uses default max stale days when config does not set it`() {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    val edpPath = "edp/test/impressions"
    val today = LocalDate.now(ZoneId.of("UTC"))

    writeCompletedDate(storageClient, edpPath, MODEL_LINE_ID, today.minusDays(4))

    val config = createConfig(edpPath = edpPath, maxStaleDays = null)
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("issues detected")
  }

  @Test
  fun `returns 500 when no model lines are configured`() {
    val edpPath = "edp/test/impressions"
    val config = createConfig(edpPath = edpPath, modelLines = emptyList())
    val port = startFunction(TextFormat.printer().printToString(config))
    val response = invokeFunction(port)

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("No active model lines configured")
  }

  companion object {
    private const val MODEL_LINE_ID = "modelLineA"
    private const val MODEL_LINE_RESOURCE_NAME =
      "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"

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
