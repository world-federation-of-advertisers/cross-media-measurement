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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse

@RunWith(JUnit4::class)
class DataAvailabilityCleanupTest {

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/dataProvider123"
    private const val RESOURCE_ID = "$DATA_PROVIDER_NAME/impressionMetadata/im-123"
    private const val BLOB_URI = "gs://bucket/path/to/blob"

    private const val CLEANUP_DURATION_METRIC = "edpa.data_availability.cleanup_duration"
    private const val RECORDS_DELETED_METRIC = "edpa.data_availability.records_deleted"
    private const val CLEANUP_ERRORS_METRIC = "edpa.data_availability.cleanup_errors"
  }

  private val cleanupStatusAttributeKey =
    io.opentelemetry.api.common.AttributeKey.stringKey(
      "edpa.data_availability_cleanup.cleanup_status"
    )
  private val dataProviderKeyAttributeKey =
    io.opentelemetry.api.common.AttributeKey.stringKey(
      "edpa.data_availability_cleanup.data_provider_key"
    )
  private val errorTypeAttributeKey =
    io.opentelemetry.api.common.AttributeKey.stringKey("edpa.data_availability_cleanup.error_type")

  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            impressionMetadata +=
              listOf(
                impressionMetadata {
                  name = RESOURCE_ID
                  modelLine = "modelLine1"
                  blobUri = request.filter.blobUriPrefix
                  interval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
                  state = ImpressionMetadata.State.ACTIVE
                }
              )
          }
        }
      onBlocking { deleteImpressionMetadata(any<DeleteImpressionMetadataRequest>()) }
        .thenAnswer { ImpressionMetadata.getDefaultInstance() }
    }

  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  private data class MetricsTestEnvironment(
    val metrics: DataAvailabilityCleanupMetrics,
    val metricExporter: InMemoryMetricExporter,
    val metricReader: PeriodicMetricReader,
    val meterProvider: SdkMeterProvider,
  ) {
    fun close() {
      meterProvider.close()
    }
  }

  private fun createMetricsEnvironment(): MetricsTestEnvironment {
    val metricExporter = InMemoryMetricExporter.create()
    val metricReader = PeriodicMetricReader.create(metricExporter)
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    val meter = meterProvider.get("data-availability-cleanup-test")
    return MetricsTestEnvironment(
      DataAvailabilityCleanupMetrics(meter),
      metricExporter,
      metricReader,
      meterProvider,
    )
  }

  @Test
  fun `cleanup with resource ID deletes directly without lookup`() = runBlocking {
    val dataAvailabilityCleanup =
      DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME)

    val result = dataAvailabilityCleanup.cleanup(BLOB_URI, RESOURCE_ID)

    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)

    // Verify delete was called with the provided resource ID
    val deleteCaptor = argumentCaptor<DeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      deleteImpressionMetadata(deleteCaptor.capture())
    }
    assertThat(deleteCaptor.firstValue.name).isEqualTo(RESOURCE_ID)

    // Verify list was NOT called since we had a resource ID
    verifyBlocking(impressionMetadataServiceMock, never()) { listImpressionMetadata(any()) }
  }

  @Test
  fun `cleanup without resource ID looks up by blob URI then deletes`() = runBlocking {
    val dataAvailabilityCleanup =
      DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME)

    val result = dataAvailabilityCleanup.cleanup(BLOB_URI, null)

    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)

    // Verify list was called with the blob URI
    val listCaptor = argumentCaptor<ListImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      listImpressionMetadata(listCaptor.capture())
    }
    assertThat(listCaptor.firstValue.parent).isEqualTo(DATA_PROVIDER_NAME)
    assertThat(listCaptor.firstValue.filter.blobUriPrefix).isEqualTo(BLOB_URI)

    // Verify delete was called with the looked-up resource ID
    val deleteCaptor = argumentCaptor<DeleteImpressionMetadataRequest>()
    verifyBlocking(impressionMetadataServiceMock, times(1)) {
      deleteImpressionMetadata(deleteCaptor.capture())
    }
    assertThat(deleteCaptor.firstValue.name).isEqualTo(RESOURCE_ID)
  }

  @Test
  fun `cleanup returns SKIPPED when record not found during lookup`() = runBlocking {
    // Mock empty list response
    wheneverBlocking { impressionMetadataServiceMock.listImpressionMetadata(any()) }
      .thenReturn(listImpressionMetadataResponse {})

    val dataAvailabilityCleanup =
      DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME)

    val result = dataAvailabilityCleanup.cleanup(BLOB_URI, null)

    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SKIPPED)

    // Verify delete was NOT called
    verifyBlocking(impressionMetadataServiceMock, never()) { deleteImpressionMetadata(any()) }
  }

  @Test
  fun `cleanup uses first match and emits metric when multiple records found`() = runBlocking {
    val firstResourceId = "$DATA_PROVIDER_NAME/impressionMetadata/im-first"
    val secondResourceId = "$DATA_PROVIDER_NAME/impressionMetadata/im-second"

    // Mock list response with multiple records
    wheneverBlocking { impressionMetadataServiceMock.listImpressionMetadata(any()) }
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata +=
            listOf(
              impressionMetadata {
                name = firstResourceId
                modelLine = "modelLine1"
                blobUri = BLOB_URI
                interval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                state = ImpressionMetadata.State.ACTIVE
              },
              impressionMetadata {
                name = secondResourceId
                modelLine = "modelLine1"
                blobUri = "$BLOB_URI-other"
                interval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
                state = ImpressionMetadata.State.ACTIVE
              },
            )
        }
      )

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilityCleanup =
        DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME, metricsEnv.metrics)

      val result = dataAvailabilityCleanup.cleanup(BLOB_URI, null)

      // Should still succeed using first match
      assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)

      // Verify delete was called with the FIRST resource ID
      val deleteCaptor = argumentCaptor<DeleteImpressionMetadataRequest>()
      verifyBlocking(impressionMetadataServiceMock, times(1)) {
        deleteImpressionMetadata(deleteCaptor.capture())
      }
      assertThat(deleteCaptor.firstValue.name).isEqualTo(firstResourceId)

      // Verify multiple_matches error metric was emitted
      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      val errorPoint = metricByName.getValue(CLEANUP_ERRORS_METRIC).longSumData.points.single()
      assertThat(errorPoint.value).isEqualTo(1)
      assertThat(errorPoint.attributes.get(errorTypeAttributeKey)).isEqualTo("multiple_matches")
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `cleanup returns SKIPPED when delete returns NOT_FOUND`() = runBlocking {
    // Mock NOT_FOUND response from delete
    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenAnswer { throw StatusException(Status.NOT_FOUND) }

    val dataAvailabilityCleanup =
      DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME)

    val result = dataAvailabilityCleanup.cleanup(BLOB_URI, RESOURCE_ID)

    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SKIPPED)
  }

  @Test
  fun `cleanup throws exception on non-NOT_FOUND error from delete`(): Unit = runBlocking {
    // Mock UNAVAILABLE response from delete
    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenAnswer { throw StatusException(Status.UNAVAILABLE) }

    val dataAvailabilityCleanup =
      DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME)

    assertFailsWith<StatusException> { dataAvailabilityCleanup.cleanup(BLOB_URI, RESOURCE_ID) }
    Unit
  }

  @Test
  fun `cleanup with empty resource ID string looks up by blob URI`() = runBlocking {
    val dataAvailabilityCleanup =
      DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME)

    val result = dataAvailabilityCleanup.cleanup(BLOB_URI, "")

    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)

    // Verify list was called (empty string treated as no resource ID)
    verifyBlocking(impressionMetadataServiceMock, times(1)) { listImpressionMetadata(any()) }
    verifyBlocking(impressionMetadataServiceMock, times(1)) { deleteImpressionMetadata(any()) }
  }

  @Test
  fun `cleanup emits success metrics`() = runBlocking {
    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilityCleanup =
        DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME, metricsEnv.metrics)

      dataAvailabilityCleanup.cleanup(BLOB_URI, RESOURCE_ID)

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      // Verify duration metric
      val durationPoint = metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(cleanupStatusAttributeKey)).isEqualTo("success")
      assertThat(durationPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo(DATA_PROVIDER_NAME)

      // Verify records deleted metric
      val recordsPoint = metricByName.getValue(RECORDS_DELETED_METRIC).longSumData.points.single()
      assertThat(recordsPoint.value).isEqualTo(1)
      assertThat(recordsPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `cleanup emits skipped metrics when record not found during lookup`() = runBlocking {
    // Mock empty list response
    wheneverBlocking { impressionMetadataServiceMock.listImpressionMetadata(any()) }
      .thenReturn(listImpressionMetadataResponse {})

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilityCleanup =
        DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME, metricsEnv.metrics)

      dataAvailabilityCleanup.cleanup(BLOB_URI, null)

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      // Verify duration metric with skipped status
      val durationPoint = metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(cleanupStatusAttributeKey)).isEqualTo("skipped")
      assertThat(durationPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo(DATA_PROVIDER_NAME)

      // Verify error metric for not found
      val errorPoint = metricByName.getValue(CLEANUP_ERRORS_METRIC).longSumData.points.single()
      assertThat(errorPoint.value).isEqualTo(1)
      assertThat(errorPoint.attributes.get(errorTypeAttributeKey)).isEqualTo("not_found")
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `cleanup emits failed metrics and RPC error metric on exception`() = runBlocking {
    // Mock UNAVAILABLE response from delete
    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenAnswer { throw StatusException(Status.UNAVAILABLE) }

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilityCleanup =
        DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME, metricsEnv.metrics)

      try {
        dataAvailabilityCleanup.cleanup(BLOB_URI, RESOURCE_ID)
      } catch (e: StatusException) {
        // Expected
      }

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      // Verify duration metric with failed status
      val durationPoint = metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(cleanupStatusAttributeKey)).isEqualTo("failed")
      assertThat(durationPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo(DATA_PROVIDER_NAME)

      // Verify RPC error metric was emitted
      val errorPoint = metricByName.getValue(CLEANUP_ERRORS_METRIC).longSumData.points.single()
      assertThat(errorPoint.value).isEqualTo(1)
      assertThat(errorPoint.attributes.get(errorTypeAttributeKey)).isEqualTo("rpc_error")
      assertThat(errorPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `cleanup emits skipped metrics when delete returns NOT_FOUND`() = runBlocking {
    // Mock NOT_FOUND response from delete
    wheneverBlocking { impressionMetadataServiceMock.deleteImpressionMetadata(any()) }
      .thenAnswer { throw StatusException(Status.NOT_FOUND) }

    val metricsEnv = createMetricsEnvironment()
    try {
      val dataAvailabilityCleanup =
        DataAvailabilityCleanup(impressionMetadataStub, DATA_PROVIDER_NAME, metricsEnv.metrics)

      dataAvailabilityCleanup.cleanup(BLOB_URI, RESOURCE_ID)

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      // Verify duration metric with skipped status (already deleted is treated as skip)
      val durationPoint = metricByName.getValue(CLEANUP_DURATION_METRIC).histogramData.points.single()
      assertThat(durationPoint.attributes.get(cleanupStatusAttributeKey)).isEqualTo("skipped")
      assertThat(durationPoint.attributes.get(dataProviderKeyAttributeKey))
        .isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }
}
