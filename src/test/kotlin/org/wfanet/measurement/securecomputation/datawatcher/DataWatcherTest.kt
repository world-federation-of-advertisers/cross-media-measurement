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

package org.wfanet.measurement.securecomputation.datawatcher

import com.google.auth.oauth2.IdTokenProvider
import com.google.common.truth.Truth.assertThat
import com.google.gson.Gson
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import com.google.protobuf.Struct
import com.google.protobuf.Value
import com.google.protobuf.kotlin.unpack
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.net.InetSocketAddress
import java.net.ServerSocket
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.controlPlaneQueueSink
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.httpEndpointSink
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.deploy.gcloud.testing.TestIdTokenProvider

@RunWith(JUnit4::class)
class DataWatcherTest() {

  private val workItemsServiceMock: WorkItemsCoroutineImplBase = mockService {}
  val mockIdTokenProvider: IdTokenProvider = TestIdTokenProvider()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(workItemsServiceMock) }

  private val workItemsStub: WorkItemsCoroutineStub by lazy {
    WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
        .buildAndRegisterGlobal()
  }

  @After
  fun cleanupTelemetry() {
    openTelemetry.close()
  }

  private fun getMetrics(): List<MetricData> {
    metricReader.forceFlush()
    return metricExporter.finishedMetricItems
  }

  @Test
  fun `sends to control plane sink when path matches`() {
    runBlocking {
      val topicId = "test-topic-id"
      val appParams = Int32Value.newBuilder().setValue(5).build()

      val config = watchedPath {
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          queue = topicId
          this.appParams = Any.pack(appParams)
        }
      }

      val dataWatcher =
        DataWatcher(
          workItemsStub,
          listOf(config),
          workItemIdGenerator = { "some-work-item-id" },
          idTokenProvider = mockIdTokenProvider,
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(1)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
      assertThat(createWorkItemRequestCaptor.allValues.single().workItemId)
        .isEqualTo("some-work-item-id")
      assertThat(createWorkItemRequestCaptor.allValues.single().workItem.queue).isEqualTo(topicId)
      val workItemParams =
        createWorkItemRequestCaptor.allValues
          .single()
          .workItem
          .workItemParams
          .unpack<WorkItemParams>()
      assertThat(workItemParams.dataPathParams.dataPath)
        .isEqualTo("test-schema://test-bucket/path-to-watch/some-data")
      val workItemAppConfig = workItemParams.appParams.unpack<Int32Value>()
      assertThat(workItemAppConfig).isEqualTo(appParams)
    }
  }

  @Test
  fun `sends to webhook sink when path matches`() {
    runBlocking {
      val appParams =
        Struct.newBuilder()
          .putFields("some-key", Value.newBuilder().setStringValue("some-value").build())
          .build()
      val localPort = ServerSocket(0).use { it.localPort }
      val config = watchedPath {
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.httpEndpointSink = httpEndpointSink {
          endpointUri = "http://localhost:$localPort"
          this.appParams = appParams
        }
      }
      val server = TestServer()
      server.start(localPort)

      val dataWatcher =
        DataWatcher(
          workItemsStub = workItemsStub,
          dataWatcherConfigs = listOf(config),
          idTokenProvider = mockIdTokenProvider,
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(0)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
      val gson = Gson()
      assertThat(gson.fromJson(server.getLastRequest(), Map::class.java))
        .isEqualTo(mapOf("some-key" to "some-value"))
      server.stop()
    }
  }

  @Test
  fun `does not create WorkItem with path does not match`() {
    runBlocking {
      val topicId = "test-topic-id"

      val config = watchedPath {
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          queue = topicId
          appParams = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }

      val dataWatcher =
        DataWatcher(workItemsStub, listOf(config), idTokenProvider = mockIdTokenProvider)
      dataWatcher.receivePath("test-schema://test-bucket/some-other-path/some-data")

      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(0)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
    }
  }

  @Test
  fun `records processing_duration metric for control plane sink`() {
    runBlocking {
      val topicId = "test-topic-id"
      val configIdentifier = "test-config-1"
      val appParams = Int32Value.newBuilder().setValue(5).build()

      val config = watchedPath {
        identifier = configIdentifier
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          queue = topicId
          this.appParams = Any.pack(appParams)
        }
      }

      val dataWatcher =
        DataWatcher(
          workItemsStub,
          listOf(config),
          workItemIdGenerator = { "test-work-item-id" },
          idTokenProvider = mockIdTokenProvider,
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")

      val metrics = getMetrics()
      val processingDurationMetric =
        metrics.find { it.name == "edpa.data_watcher.processing_duration" }
      assertThat(processingDurationMetric).isNotNull()
      assertThat(processingDurationMetric!!.description)
        .isEqualTo("Time from regex match to successful sink submission")
      assertThat(processingDurationMetric.unit).isEqualTo("s")

      val histogramData = processingDurationMetric.histogramData
      assertThat(histogramData.points).hasSize(1)
      val point = histogramData.points.single()
      assertThat(point.attributes.get(AttributeKey.stringKey("edpa.data_watcher.sink_type")))
        .isEqualTo("CONTROL_PLANE_QUEUE_SINK")
      // Verify that some duration was recorded (should be very small)
      assertThat(point.sum).isGreaterThan(0.0)
    }
  }

  @Test
  fun `records queue_writes counter metric`() {
    runBlocking {
      val topicId = "test-topic-id"
      val configIdentifier = "test-config-2"
      val workItemId = "test-work-item-123"
      val appParams = Int32Value.newBuilder().setValue(5).build()

      val config = watchedPath {
        identifier = configIdentifier
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          queue = topicId
          this.appParams = Any.pack(appParams)
        }
      }

      val dataWatcher =
        DataWatcher(
          workItemsStub,
          listOf(config),
          workItemIdGenerator = { workItemId },
          idTokenProvider = mockIdTokenProvider,
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")

      val metrics = getMetrics()
      val queueWritesMetric = metrics.find { it.name == "edpa.data_watcher.queue_writes" }
      assertThat(queueWritesMetric).isNotNull()
      assertThat(queueWritesMetric!!.description)
        .isEqualTo("Number of work items submitted to control plane queue")

      val sumData = queueWritesMetric.longSumData
      assertThat(sumData.points).hasSize(1)
      val point = sumData.points.single()
      assertThat(point.value).isEqualTo(1)
      assertThat(point.attributes.get(AttributeKey.stringKey("edpa.data_watcher.queue")))
        .isEqualTo(topicId)
    }
  }

  @Test
  fun `records multiple queue_writes for multiple paths`() {
    runBlocking {
      val topicId = "test-topic-id"
      val configIdentifier = "test-config-3"
      var workItemCounter = 0
      val appParams = Int32Value.newBuilder().setValue(5).build()

      val config = watchedPath {
        identifier = configIdentifier
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          queue = topicId
          this.appParams = Any.pack(appParams)
        }
      }

      val dataWatcher =
        DataWatcher(
          workItemsStub,
          listOf(config),
          workItemIdGenerator = { "work-item-${workItemCounter++}" },
          idTokenProvider = mockIdTokenProvider,
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/data-1")
      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/data-2")
      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/data-3")

      val metrics = getMetrics()
      val queueWritesMetric = metrics.find { it.name == "edpa.data_watcher.queue_writes" }
      assertThat(queueWritesMetric).isNotNull()

      val sumData = queueWritesMetric!!.longSumData
      // All 3 writes have the same attributes, so they get aggregated into 1 point
      assertThat(sumData.points).hasSize(1)
      val point = sumData.points.single()
      assertThat(point.value).isEqualTo(3)
      assertThat(point.attributes.get(AttributeKey.stringKey("edpa.data_watcher.queue")))
        .isEqualTo(topicId)
    }
  }

  @Test
  fun `does not record metrics when path does not match`() {
    runBlocking {
      val topicId = "test-topic-id"

      val config = watchedPath {
        identifier = "test-config-4"
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          queue = topicId
          appParams = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }

      val dataWatcher =
        DataWatcher(workItemsStub, listOf(config), idTokenProvider = mockIdTokenProvider)
      dataWatcher.receivePath("test-schema://test-bucket/some-other-path/some-data")

      val metrics = getMetrics()
      // No metrics should be recorded since path didn't match
      assertThat(metrics.filter { it.name == "edpa.data_watcher.processing_duration" }).isEmpty()
      assertThat(metrics.filter { it.name == "edpa.data_watcher.queue_writes" }).isEmpty()
    }
  }
}

private class TestServer() {
  private lateinit var handler: ServerHandler
  private lateinit var httpServer: HttpServer

  fun start(port: Int): HttpServer {
    httpServer = HttpServer.create(InetSocketAddress(port), 0)
    handler = ServerHandler()
    httpServer.createContext("/", handler)
    httpServer.setExecutor(null)
    httpServer.start()
    return httpServer
  }

  fun stop() {
    httpServer.stop(0)
  }

  fun getLastRequest(): String {
    return handler.requestBody
  }

  class ServerHandler : HttpHandler {
    lateinit var requestBody: String

    override fun handle(t: HttpExchange) {
      val requestBodyBytes = t.requestBody.readBytes()
      requestBody = requestBodyBytes.toString(Charsets.UTF_8)
      val response = "Success"
      t.sendResponseHeaders(200, response.length.toLong())
      val os = t.responseBody
      os.write(response.toByteArray())
      os.close()
    }
  }
}
