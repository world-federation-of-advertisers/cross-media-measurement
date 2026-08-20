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
import io.cloudevents.CloudEvent
import io.cloudevents.CloudEventData
import io.cloudevents.SpecVersion
import java.net.URI
import java.time.OffsetDateTime
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

@RunWith(JUnit4::class)
class DataWatcherFunctionTest {

  @Test
  fun `stashes the object generation in metadata for DataWatcher`() {
    val capturedMetadata = mutableListOf<Map<String, String>>()
    val pathReceiver: suspend (String, Map<String, String>) -> Unit = { _, metadata ->
      capturedMetadata += metadata
    }

    val cloudEventData =
      """
      {
        "bucket": "test-bucket",
        "name": "path/to/blob",
        "size": "1",
        "generation": "42",
        "metadata": { "impression-metadata-resource-id": "res-123" }
      }
      """
        .trimIndent()

    DataWatcherFunction(pathReceiver).accept(TestCloudEvent(cloudEventData.toByteArray()))

    assertThat(capturedMetadata).hasSize(1)
    // The GCS object generation is stashed under the reserved key so DataWatcher can forward it as
    // the X-DataWatcher-Generation header.
    assertThat(capturedMetadata.single()).containsEntry(DataWatcher.GENERATION_METADATA_KEY, "42")
    // Existing custom object metadata is preserved alongside the generation.
    assertThat(capturedMetadata.single())
      .containsEntry("impression-metadata-resource-id", "res-123")
  }

  private class TestCloudEvent(private val dataBytes: ByteArray) : CloudEvent {
    private val data =
      object : CloudEventData {
        override fun toBytes(): ByteArray = dataBytes
      }

    override fun getSpecVersion(): SpecVersion = SpecVersion.V1

    override fun getId(): String = EVENT_ID

    override fun getSource(): URI = URI.create(EVENT_SOURCE)

    override fun getType(): String = EVENT_TYPE

    override fun getDataContentType(): String? = "application/json"

    override fun getDataSchema(): URI? = null

    override fun getSubject(): String? = null

    override fun getTime(): OffsetDateTime? = null

    override fun getData(): CloudEventData = data

    override fun getExtension(name: String): Any? = null

    override fun getExtensionNames(): Set<String> = emptySet()

    override fun getAttribute(name: String): Any? =
      when (name.lowercase()) {
        "id" -> EVENT_ID
        "source" -> URI.create(EVENT_SOURCE)
        "type" -> EVENT_TYPE
        "datacontenttype" -> "application/json"
        else -> null
      }
  }

  companion object {
    init {
      // Disable OTLP exporters for testing before EdpaTelemetry initializes.
      System.setProperty("otel.metrics.exporter", "none")
      System.setProperty("otel.traces.exporter", "none")
      System.setProperty("otel.logs.exporter", "none")

      EdpaTelemetry.ensureInitialized()
    }

    private const val EVENT_ID = "generation-test"
    private const val EVENT_SOURCE = "//storage.googleapis.com/projects/_/buckets/test-bucket"
    private const val EVENT_TYPE = "google.cloud.storage.object.v1.finalized"
  }
}
