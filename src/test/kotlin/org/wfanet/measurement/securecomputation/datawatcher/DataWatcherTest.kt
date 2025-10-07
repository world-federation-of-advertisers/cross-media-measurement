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
import com.sun.net.httpserver.Headers
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
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
  private lateinit var metricReader: InMemoryMetricReader
  private lateinit var meterProvider: SdkMeterProvider
  private var meterCounter: Int = 0

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricReader = InMemoryMetricReader.create()
    meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal()
  }

  @After
  fun cleanupTelemetry() {
    if (this::openTelemetry.isInitialized) {
      openTelemetry.close()
    }
  }

  private fun getMetrics(): List<MetricData> {
    meterProvider.forceFlush().join(10, TimeUnit.SECONDS)
    return metricReader.collectAllMetrics().toList()
  }

  private fun newMetrics(): DataWatcherMetrics {
    val meter = openTelemetry.getMeter("data-watcher-test-${meterCounter++}")
    return DataWatcherMetrics(meter)
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
          metrics = newMetrics(),
        )

      val blobCreateTime = Instant.now().minusSeconds(12)
      dataWatcher.receivePath(
        path = "test-schema://test-bucket/path-to-watch/some-data",
        blobCreateTime = blobCreateTime,
      )
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
          metrics = newMetrics(),
        )

      dataWatcher.receivePath(
        path = "test-schema://test-bucket/path-to-watch/some-data",
        blobCreateTime = Instant.now(),
      )
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(0)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
      val gson = Gson()
      assertThat(gson.fromJson(server.getLastRequest(), Map::class.java))
        .isEqualTo(mapOf("some-key" to "some-value"))
      assertThat(server.getLastHeader("X-DataWatcher-Done-Timestamp")).isNull()
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
        DataWatcher(
          workItemsStub,
          listOf(config),
          idTokenProvider = mockIdTokenProvider,
          metrics = newMetrics(),
        )
      dataWatcher.receivePath("test-schema://test-bucket/some-other-path/some-data")

      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(0)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
    }
  }

  @Test
  fun `records match_latency metric for control plane sink`() {
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
          workItemIdGenerator = { "test-work-item-id" },
          idTokenProvider = mockIdTokenProvider,
          metrics = newMetrics(),
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")

      val metrics = getMetrics()
      val matchLatencyMetric =
        metrics.find { it.name == "edpa.data_watcher.match_latency" }
      assertThat(matchLatencyMetric).isNotNull()
      assertThat(matchLatencyMetric!!.description)
        .isEqualTo("Time from blob match to sink completion")
      assertThat(matchLatencyMetric.unit).isEqualTo("s")

      val histogramData = matchLatencyMetric.histogramData
      val points = histogramData.points
      assertThat(points.size).isEqualTo(1)
      val point = points.single()
      val sinkTypeKey: AttributeKey<String> = AttributeKey.stringKey("sink_type")
      assertThat(point.attributes.get(sinkTypeKey)).isEqualTo("CONTROL_PLANE_QUEUE_SINK")
      assertThat(point.sum).isGreaterThan(10.0)
    }
  }

  @Test
  fun `records queue_writes counter metric`() {
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
          idTokenProvider = mockIdTokenProvider,
          metrics = newMetrics(),
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")

      val metrics = getMetrics()
      val queueWritesMetric = metrics.find { it.name == "edpa.data_watcher.queue_writes" }
      assertThat(queueWritesMetric).isNotNull()
      assertThat(queueWritesMetric!!.description)
        .contains("work items submitted to the control plane queue")

      val sumData = queueWritesMetric.longSumData
      val points = sumData.points
      assertThat(points.size).isEqualTo(1)
      val point = points.single()
      val queueKey: AttributeKey<String> = AttributeKey.stringKey("queue")
      assertThat(point.value).isEqualTo(1)
      assertThat(point.attributes.get(queueKey)).isEqualTo(topicId)
      assertThat(point.attributes.size()).isEqualTo(1)
    }
  }

  @Test
  fun `records multiple queue_writes for multiple paths`() {
    runBlocking {
      val topicId = "test-topic-id"
      var workItemCounter = 0
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
          workItemIdGenerator = { "work-item-${workItemCounter++}" },
          idTokenProvider = mockIdTokenProvider,
          metrics = newMetrics(),
        )

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/data-1")
      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/data-2")
      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/data-3")

      val metrics = getMetrics()
      val queueWritesMetric = metrics.find { it.name == "edpa.data_watcher.queue_writes" }
      assertThat(queueWritesMetric).isNotNull()

      val sumData = queueWritesMetric!!.longSumData
      val points = sumData.points
      assertThat(points.size).isEqualTo(1)
      val point = points.single()
      assertThat(point.value).isEqualTo(3)
    }
  }

  @Test
  fun `adds done timestamp header for http sink done marker`() {
    runBlocking {
      val localPort = ServerSocket(0).use { it.localPort }
      val server = TestServer()
      server.start(localPort)

      val config = watchedPath {
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.httpEndpointSink = httpEndpointSink {
          endpointUri = "http://localhost:$localPort"
          this.appParams =
            Struct.newBuilder()
              .putFields("some-key", Value.newBuilder().setStringValue("value").build())
              .build()
        }
      }

      val dataWatcher =
        DataWatcher(
          workItemsStub = workItemsStub,
          dataWatcherConfigs = listOf(config),
          idTokenProvider = mockIdTokenProvider,
          metrics = newMetrics(),
        )

      val blobCreateTime = Instant.now().minusSeconds(15)
      dataWatcher.receivePath(
        path = "test-schema://test-bucket/path-to-watch/done",
        blobCreateTime = blobCreateTime,
      )

      assertThat(server.getLastHeader("X-DataWatcher-Done-Timestamp"))
        .isEqualTo(blobCreateTime.toEpochMilli().toString())

      val metrics = getMetrics()
      val matchLatencyMetric = metrics.find { it.name == "edpa.data_watcher.match_latency" }
      assertThat(matchLatencyMetric).isNotNull()
      val point = matchLatencyMetric!!.histogramData.points.single()
      assertThat(point.sum).isGreaterThan(10.0)
      assertThat(point.sum).isLessThan(30.0)

      server.stop()
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
        DataWatcher(
          workItemsStub,
          listOf(config),
          idTokenProvider = mockIdTokenProvider,
          metrics = newMetrics(),
        )
      dataWatcher.receivePath("test-schema://test-bucket/some-other-path/some-data")

      val metrics = getMetrics()
      assertThat(metrics.filter { it.name == "edpa.data_watcher.match_latency" }).isEmpty()
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

  fun getLastHeader(name: String): String? {
    return handler.requestHeaders?.getFirst(name)
  }

  class ServerHandler : HttpHandler {
    lateinit var requestBody: String
    var requestHeaders: Headers? = null

    override fun handle(t: HttpExchange) {
      val requestBodyBytes = t.requestBody.readBytes()
      requestBody = requestBodyBytes.toString(Charsets.UTF_8)
      requestHeaders = t.requestHeaders
      val response = "Success"
      t.sendResponseHeaders(200, response.length.toLong())
      val os = t.responseBody
      os.write(response.toByteArray())
      os.close()
    }
  }
}
