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
import io.opentelemetry.api.trace.Span
import java.net.URI
import java.time.OffsetDateTime
import kotlin.text.Charsets
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

@RunWith(JUnit4::class)
class DataWatcherFunctionTracingTest {

  @Test
  fun `handles CloudEvent with unknown fields like customTime`() {
    var receivedPath: String? = null

    val pathReceiver: suspend (String, Map<String, String>) -> Unit = { path, _ ->
      receivedPath = path
    }

    val cloudEventData =
      """
      {
        "bucket": "test-bucket",
        "name": "path/to/blob",
        "size": "1",
        "customTime": "2026-02-11T10:00:00.000Z"
      }
      """
        .trimIndent()

    val cloudEvent =
      TestCloudEvent(
        dataBytes = cloudEventData.toByteArray(Charsets.UTF_8),
        extensions = emptyMap(),
      )

    DataWatcherFunction(pathReceiver).accept(cloudEvent)

    assertThat(receivedPath).isEqualTo("gs://test-bucket/path/to/blob")
  }

  @Test
  fun `propagates traceparent from CloudEvent to DataWatcher work`() {
    val expectedTraceId = "4bf92f3577b34da6a3ce929d0e0e4736"
    val traceParent = "00-$expectedTraceId-00f067aa0ba902b7-01"
    val recordedTraceIds = mutableListOf<String>()

    val pathReceiver: suspend (String, Map<String, String>) -> Unit = { _, _ ->
      recordedTraceIds += Span.current().spanContext.traceId
    }

    val cloudEventData =
      """
      {
        "bucket": "test-bucket",
        "name": "path/to/blob",
        "size": "1"
      }
      """
        .trimIndent()

    val cloudEvent =
      TestCloudEvent(
        dataBytes = cloudEventData.toByteArray(Charsets.UTF_8),
        extensions = mapOf("traceparent" to traceParent),
      )

    DataWatcherFunction(pathReceiver).accept(cloudEvent)

    assertThat(recordedTraceIds).containsExactly(expectedTraceId)
  }

  @Test
  fun `persisted done metadata does not override CloudEvent trace context`() {
    val expectedTraceId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    val persistedTraceParent = "00-11111111111111111111111111111111-2222222222222222-01"
    var receivedMetadata: Map<String, String> = emptyMap()
    var receivedTraceId = ""
    val cloudEventData =
      """
      {
        "bucket": "test-bucket",
        "name": "path/to/done",
        "size": "0",
        "generation": "123",
        "metadata": {
          "xmm-trace-context-source": "vid-labeler",
          "xmm-traceparent": "$persistedTraceParent",
          "xmm-raw-impression-upload": "dataProviders/dp/rawImpressionUploads/up",
          "xmm-model-line": "modelProviders/mp/modelSuites/ms/modelLines/ml",
          "xmm-vid-labeling-job": "dataProviders/dp/rawImpressionUploads/up/vidLabelingJobs/job"
        }
      }
      """
        .trimIndent()
    val cloudEvent =
      TestCloudEvent(
        dataBytes = cloudEventData.toByteArray(Charsets.UTF_8),
        extensions = mapOf("traceparent" to "00-$expectedTraceId-bbbbbbbbbbbbbbbb-01"),
      )

    DataWatcherFunction { _, metadata ->
        receivedMetadata = metadata
        receivedTraceId = Span.current().spanContext.traceId
      }
      .accept(cloudEvent)

    assertThat(receivedTraceId).isEqualTo(expectedTraceId)
    assertThat(receivedMetadata).doesNotContainKey("xmm-traceparent")
    assertThat(receivedMetadata).doesNotContainKey("xmm-raw-impression-upload")
    assertThat(receivedMetadata).doesNotContainKey("xmm-model-line")
    assertThat(receivedMetadata).doesNotContainKey("xmm-vid-labeling-job")
    assertThat(receivedMetadata[DataWatcher.GENERATION_METADATA_KEY]).isEqualTo("123")
  }

  @Test
  fun `untrusted persisted trace context does not replace CloudEvent parent`() {
    val expectedTraceId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    var receivedMetadata: Map<String, String> = emptyMap()
    var receivedTraceId = ""
    val cloudEventData =
      """
      {
        "bucket": "test-bucket",
        "name": "path/to/done",
        "size": "0",
        "generation": "123",
        "metadata": {
          "xmm-traceparent": "00-11111111111111111111111111111111-2222222222222222-01",
          "xmm-raw-impression-upload": "dataProviders/dp/rawImpressionUploads/up",
          "xmm-model-line": "modelProviders/mp/modelSuites/ms/modelLines/ml",
          "xmm-vid-labeling-job": "dataProviders/dp/rawImpressionUploads/up/vidLabelingJobs/job"
        }
      }
      """
        .trimIndent()
    val cloudEvent =
      TestCloudEvent(
        dataBytes = cloudEventData.toByteArray(Charsets.UTF_8),
        extensions = mapOf("traceparent" to "00-$expectedTraceId-bbbbbbbbbbbbbbbb-01"),
      )

    DataWatcherFunction { _, metadata ->
        receivedMetadata = metadata
        receivedTraceId = Span.current().spanContext.traceId
      }
      .accept(cloudEvent)

    assertThat(receivedTraceId).isEqualTo(expectedTraceId)
    assertThat(receivedMetadata).doesNotContainKey("xmm-traceparent")
    assertThat(receivedMetadata).doesNotContainKey("xmm-raw-impression-upload")
    assertThat(receivedMetadata).doesNotContainKey("xmm-model-line")
    assertThat(receivedMetadata).doesNotContainKey("xmm-vid-labeling-job")
    assertThat(receivedMetadata[DataWatcher.GENERATION_METADATA_KEY]).isEqualTo("123")
  }

  @Test
  fun `malformed persisted trace context does not replace CloudEvent parent`() {
    val expectedTraceId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    var receivedTraceId = ""
    val cloudEventData =
      """
      {
        "bucket": "test-bucket",
        "name": "path/to/done",
        "size": "0",
        "metadata": {
          "xmm-trace-context-source": "vid-labeler",
          "xmm-traceparent": "malformed",
          "xmm-raw-impression-upload": "dataProviders/dp/rawImpressionUploads/up",
          "xmm-model-line": "modelProviders/mp/modelSuites/ms/modelLines/ml",
          "xmm-vid-labeling-job": "dataProviders/dp/rawImpressionUploads/up/vidLabelingJobs/job"
        }
      }
      """
        .trimIndent()
    val cloudEvent =
      TestCloudEvent(
        dataBytes = cloudEventData.toByteArray(Charsets.UTF_8),
        extensions = mapOf("traceparent" to "00-$expectedTraceId-bbbbbbbbbbbbbbbb-01"),
      )

    DataWatcherFunction { _, _ -> receivedTraceId = Span.current().spanContext.traceId }
      .accept(cloudEvent)

    assertThat(receivedTraceId).isEqualTo(expectedTraceId)
  }

  private class TestCloudEvent(
    private val dataBytes: ByteArray,
    private val extensions: Map<String, Any?>,
  ) : CloudEvent {
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

    override fun getExtension(name: String): Any? = extensions[name]

    override fun getExtensionNames(): Set<String> = extensions.keys

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
      // Disable OTLP exporters for testing before EdpaTelemetry initializes
      System.setProperty("otel.metrics.exporter", "none")
      System.setProperty("otel.traces.exporter", "none")
      System.setProperty("otel.logs.exporter", "none")

      EdpaTelemetry.ensureInitialized()
    }

    private const val EVENT_ID = "trace-test"
    private const val EVENT_SOURCE = "//storage.googleapis.com/projects/_/buckets/test-bucket"
    private const val EVENT_TYPE = "google.cloud.storage.object.v1.finalized"
  }
}
