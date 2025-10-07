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

package org.wfanet.measurement.common.grpc

import com.google.common.truth.Truth.assertThat
import com.google.longrunning.CancelOperationRequest
import com.google.longrunning.OperationsGrpc
import com.google.longrunning.OperationsGrpcKt
import com.google.protobuf.Empty
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.HistogramPointData
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import kotlin.test.assertFailsWith
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService

@RunWith(JUnit4::class)
class OpenTelemetryClientInterceptorTest {
  private lateinit var metricReader: InMemoryMetricReader
  private lateinit var meterProvider: SdkMeterProvider
  private lateinit var openTelemetry: OpenTelemetry
  private lateinit var meter: Meter

  private val serviceMock = mockService<OperationsGrpcKt.OperationsCoroutineImplBase> {
    onBlocking { cancelOperation(any()) } doReturn Empty.getDefaultInstance()
  }

  @get:Rule val testServer = GrpcTestServerRule { addService(serviceMock) }

  @Before
  fun setup() {
    metricReader = InMemoryMetricReader.create()
    meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build()
    meter = openTelemetry.getMeter("test")
  }

  @Test
  fun `records rpc client duration metric on successful call`() {
    val interceptor = OpenTelemetryClientInterceptor(meter)
    val channel = ClientInterceptors.intercept(testServer.channel, interceptor)
    val stub = OperationsGrpc.newBlockingStub(channel)

    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())

    val metrics = metricReader.collectAllMetrics()
    val durationMetric = metrics.find { it.name == "rpc.client.duration" }
    assertThat(durationMetric).isNotNull()

    val histogramData = durationMetric!!.histogramData.points.first()
    assertThat(histogramData.attributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("rpc.service")))
      .isEqualTo("google.longrunning.Operations")
    assertThat(histogramData.attributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("rpc.method")))
      .isEqualTo("CancelOperation")
    assertThat(histogramData.attributes.get(io.opentelemetry.api.common.AttributeKey.longKey("rpc.grpc.status_code")))
      .isEqualTo(0L) // Status.OK
  }

  @Test
  fun `records rpc client duration metric with status code`() {
    val interceptor = OpenTelemetryClientInterceptor(meter)
    val channel = ClientInterceptors.intercept(testServer.channel, interceptor)
    val stub = OperationsGrpc.newBlockingStub(channel)

    // Make a successful call
    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())

    val metrics = metricReader.collectAllMetrics()
    val durationMetric = metrics.find { it.name == "rpc.client.duration" }
    assertThat(durationMetric).isNotNull()

    val histogramData = durationMetric!!.histogramData.points.first()
    // Verify the status code attribute is recorded (0 for Status.OK)
    assertThat(histogramData.attributes.get(io.opentelemetry.api.common.AttributeKey.longKey("rpc.grpc.status_code")))
      .isEqualTo(0L)
  }

  @Test
  fun `records duration in seconds`() {
    val interceptor = OpenTelemetryClientInterceptor(meter)
    val channel = ClientInterceptors.intercept(testServer.channel, interceptor)
    val stub = OperationsGrpc.newBlockingStub(channel)

    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())

    val metrics = metricReader.collectAllMetrics()
    val durationMetric = metrics.find { it.name == "rpc.client.duration" }
    assertThat(durationMetric).isNotNull()
    assertThat(durationMetric!!.unit).isEqualTo("s")

    val histogramData = durationMetric.histogramData.points.first()
    // Duration should be a small non-negative number (in seconds)
    assertThat(histogramData.sum).isAtLeast(0.0)
    assertThat(histogramData.sum).isLessThan(10.0) // Should complete in less than 10 seconds
  }

  @Test
  fun `records multiple calls separately`() {
    val interceptor = OpenTelemetryClientInterceptor(meter)
    val channel = ClientInterceptors.intercept(testServer.channel, interceptor)
    val stub = OperationsGrpc.newBlockingStub(channel)

    // Make multiple calls
    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())
    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())
    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())

    val metrics = metricReader.collectAllMetrics()
    val durationMetric = metrics.find { it.name == "rpc.client.duration" }
    assertThat(durationMetric).isNotNull()

    val histogramData = durationMetric!!.histogramData.points.first()
    assertThat(histogramData.count).isEqualTo(3)
  }

  @Test
  fun `metric has correct description and unit`() {
    val interceptor = OpenTelemetryClientInterceptor(meter)
    val channel = ClientInterceptors.intercept(testServer.channel, interceptor)
    val stub = OperationsGrpc.newBlockingStub(channel)

    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())

    val metrics = metricReader.collectAllMetrics()
    val durationMetric = metrics.find { it.name == "rpc.client.duration" }
    assertThat(durationMetric).isNotNull()
    assertThat(durationMetric!!.description).isEqualTo("Duration of gRPC client calls")
    assertThat(durationMetric.unit).isEqualTo("s")
  }
}
