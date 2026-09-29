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

package org.wfanet.measurement.kingdom.batch

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata
import org.wfanet.measurement.api.v2alpha.measurementSpec as apiMeasurementSpec
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.batchCancelMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementDetails

private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 1L
private const val EXTERNAL_MEASUREMENT_ID = 2L
private const val BASIC_REPORT_NAME =
  "measurementConsumers/AAAAAAAAAAE/basicReports/basic-report"
private const val REPORT_NAME = "measurementConsumers/AAAAAAAAAAE/reports/report"
private const val METRIC_NAME = "measurementConsumers/AAAAAAAAAAE/metrics/metric"
private val MEASUREMENT_NAME =
  "measurementConsumers/${externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID)}/" +
    "measurements/${externalIdToApiId(EXTERNAL_MEASUREMENT_ID)}"
private val NOW = Instant.parse("2026-09-29T12:00:00Z")
private val PENDING_MEASUREMENT = measurement {
  externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
  externalMeasurementId = EXTERNAL_MEASUREMENT_ID
  state = Measurement.State.PENDING_REQUISITION_FULFILLMENT
  createTime = NOW.minus(Duration.ofDays(3)).toProtoTime()
  etag = "etag"
  details = measurementDetails {
    measurementSpec =
      apiMeasurementSpec {
          reportingMetadata = reportingMetadata {
            basicReport = BASIC_REPORT_NAME
            report = REPORT_NAME
            metric = METRIC_NAME
          }
        }
        .toByteString()
  }
}

@RunWith(JUnit4::class)
class PendingMeasurementsCancellationTest {
  private val measurementsServiceMock: MeasurementsCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(measurementsServiceMock) }

  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var spanExporter: InMemorySpanExporter

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    spanExporter = InMemorySpanExporter.create()
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setTracerProvider(
          SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        )
        .buildAndRegisterGlobal()
  }

  @After
  fun cleanupTelemetry() {
    openTelemetry.close()
  }

  @Test
  fun `run emits accepted lifecycle span for each cancelled Measurement`() {
    whenever(measurementsServiceMock.streamMeasurements(any()))
      .thenReturn(flowOf(PENDING_MEASUREMENT), emptyFlow())
    whenever(measurementsServiceMock.batchCancelMeasurements(any())).thenReturn(
      batchCancelMeasurementsResponse {
        measurements += PENDING_MEASUREMENT.copy { state = Measurement.State.CANCELLED }
      }
    )
    val cancellation =
      PendingMeasurementsCancellation(
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        Duration.ofDays(2),
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
      )

    cancellation.run()

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.name).isEqualTo("kingdom.measurement.retention_cancel")
    assertThat(span.attributes.get(ReportTraceAttributes.BASIC_REPORT_NAME))
      .isEqualTo(BASIC_REPORT_NAME)
    assertThat(span.attributes.get(ReportTraceAttributes.MEASUREMENT_NAME))
      .isEqualTo(MEASUREMENT_NAME)
    assertThat(span.attributes.get(ReportTraceAttributes.MEASUREMENT_STATE))
      .isEqualTo("CANCELLED")
    assertThat(span.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE))
      .isEqualTo("measurement_cancellation")
    assertThat(span.attributes.get(ReportTraceAttributes.CANCELLATION_ORIGIN))
      .isEqualTo("retention_policy")
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("accepted")
  }

  @Test
  fun `run emits failed lifecycle span when cancellation RPC fails`() {
    whenever(measurementsServiceMock.streamMeasurements(any()))
      .thenReturn(flowOf(PENDING_MEASUREMENT))
    whenever(measurementsServiceMock.batchCancelMeasurements(any()))
      .thenThrow(Status.UNAVAILABLE.asRuntimeException())
    val cancellation =
      PendingMeasurementsCancellation(
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        Duration.ofDays(2),
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
      )

    assertFailsWith<StatusException> { cancellation.run() }

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.BASIC_REPORT_NAME))
      .isEqualTo(BASIC_REPORT_NAME)
    assertThat(span.attributes.get(ReportTraceAttributes.MEASUREMENT_NAME))
      .isEqualTo(MEASUREMENT_NAME)
    assertThat(span.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE))
      .isEqualTo("measurement_cancellation")
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("failed")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_CODE))
      .isEqualTo("grpc.UNAVAILABLE")
  }
}
