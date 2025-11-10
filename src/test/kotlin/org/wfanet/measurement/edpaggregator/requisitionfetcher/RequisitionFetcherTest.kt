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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.HistogramPointData
import io.opentelemetry.sdk.metrics.data.LongPointData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class RequisitionFetcherTest {
  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions.add(REQUISITION) })
    }

  private val requisitionMetadataServiceMock:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { fetchLatestCmmsCreateTime(any()) }.thenReturn(timestamp {})
      onBlocking { createRequisitionMetadata(any()) }.thenReturn(requisitionMetadata {})
      onBlocking { refuseRequisitionMetadata(any()) }.thenReturn(requisitionMetadata {})
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
    addService(requisitionMetadataServiceMock)
  }
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val requisitionMetadataStub:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub by lazy {
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }
  private val requisitionGrouper: RequisitionGrouper = mock()

  @Rule @JvmField val tempFolder = TemporaryFolder()

  private lateinit var metricReader: InMemoryMetricReader
  private lateinit var testMetrics: RequisitionFetcherMetrics
  private lateinit var storageClient: FileSystemStorageClient

  @Before
  fun setUp() {
    // Create an in-memory metric reader for testing
    metricReader = InMemoryMetricReader.create()
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()

    // Create test metrics instance with test meter
    testMetrics = RequisitionFetcherMetrics(meterProvider.get("test"))

    // Create storage client
    storageClient = FileSystemStorageClient(tempFolder.root)
  }

  private fun createFetcher(
    storageClient: StorageClient = this.storageClient,
    metrics: RequisitionFetcherMetrics = RequisitionFetcherMetrics.Default,
  ): RequisitionFetcher {
    return RequisitionFetcher(
      requisitionsStub,
      storageClient,
      DATA_PROVIDER_NAME,
      STORAGE_PATH_PREFIX,
      requisitionGrouper,
      metrics = metrics,
    )
  }

  @Test
  fun `fetchAndStoreRequisitions stores single GroupedRequisition`() = runBlocking {
    whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(listOf(GROUPED_REQUISITIONS))

    val fetcher = createFetcher()
    val typeRegistry = TypeRegistry.newBuilder().add(Requisition.getDescriptor()).build()

    fetcher.fetchAndStoreRequisitions()

    // Verify storage
    val blobKey = "$STORAGE_PATH_PREFIX/1234"
    val blob = storageClient.getBlob(blobKey)
    assertThat(blob).isNotNull()
    val blobContent: ByteString = blob!!.read().flatten()
    val parsedBlob = Any.parseFrom(blobContent)
    assertThat(parsedBlob)
      .unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry())
      .isEqualTo(Any.pack(GROUPED_REQUISITIONS))
  }

  @Test
  fun `fetchAndStoreRequisitions stores multiple GroupedRequisitions with same groupId`() =
    runBlocking {
      val groupedRequisitionsList: List<GroupedRequisitions> =
        listOf(GROUPED_REQUISITIONS, GROUPED_REQUISITIONS, GROUPED_REQUISITIONS)
      whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(groupedRequisitionsList)

      val fetcher = createFetcher()

      fetcher.fetchAndStoreRequisitions()

      // Verify storage (all three write to same blob since they have same groupId)
      assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/1234")).isNotNull()
    }

  @Test
  fun `fetchAndStoreRequisitions stores multiple GroupedRequisitions with different groupIds`() =
    runBlocking {
      val groupedRequisitionsList =
        listOf(GROUPED_REQUISITIONS, GROUPED_REQUISITIONS_2, GROUPED_REQUISITIONS_3)
      whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(groupedRequisitionsList)

      val fetcher = createFetcher()

      fetcher.fetchAndStoreRequisitions()

      // Verify storage (all three stored with different groupIds)
      assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/1234")).isNotNull()
      assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/5678")).isNotNull()
      assertThat(storageClient.getBlob("$STORAGE_PATH_PREFIX/9012")).isNotNull()
    }

  // Metric Tests

  @Test
  fun `metrics - requisitionsFetched emitted with correct count`() = runBlocking {
    whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(listOf(GROUPED_REQUISITIONS))

    val fetcher = createFetcher(metrics = testMetrics)

    fetcher.fetchAndStoreRequisitions()

    // Verify requisitionsFetched counter
    val metrics = metricReader.collectAllMetrics()
    val fetchedMetric = metrics.find { it.name == "edpa.requisition_fetcher.requisitions_fetched" }
    assertThat(fetchedMetric).isNotNull()
    val fetchedValue = (fetchedMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(fetchedValue).isEqualTo(1)
  }

  @Test
  fun `metrics - storageWrites emitted for single GroupedRequisition`() = runBlocking {
    whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(listOf(GROUPED_REQUISITIONS))

    val fetcher = createFetcher(metrics = testMetrics)

    fetcher.fetchAndStoreRequisitions()

    // Verify storageWrites counter (1 GroupedRequisition written)
    val metrics = metricReader.collectAllMetrics()
    val writesMetric = metrics.find { it.name == "edpa.requisition_fetcher.storage_writes" }
    assertThat(writesMetric).isNotNull()
    val writesValue = (writesMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(writesValue).isEqualTo(1)
  }

  @Test
  fun `metrics - storageWrites emitted for multiple GroupedRequisitions`() = runBlocking {
    val groupedRequisitionsList =
      listOf(GROUPED_REQUISITIONS, GROUPED_REQUISITIONS_2, GROUPED_REQUISITIONS_3)
    whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(groupedRequisitionsList)

    val fetcher = createFetcher(metrics = testMetrics)

    fetcher.fetchAndStoreRequisitions()

    // Verify storageWrites counter (3 different GroupedRequisitions written)
    val metrics = metricReader.collectAllMetrics()
    val writesMetric = metrics.find { it.name == "edpa.requisition_fetcher.storage_writes" }
    assertThat(writesMetric).isNotNull()
    val writesValue = (writesMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(writesValue).isEqualTo(3)
  }

  @Test
  fun `metrics - fetchLatency histogram recorded`() = runBlocking {
    whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(listOf(GROUPED_REQUISITIONS))

    val fetcher = createFetcher(metrics = testMetrics)

    fetcher.fetchAndStoreRequisitions()

    // Verify fetchLatency histogram was recorded
    val metrics = metricReader.collectAllMetrics()
    val latencyMetric = metrics.find { it.name == "edpa.requisition_fetcher.fetch_latency" }
    assertThat(latencyMetric).isNotNull()
    val latencyPoint = latencyMetric!!.histogramData.points.first() as HistogramPointData
    assertThat(latencyPoint.count).isEqualTo(1)
    assertThat(latencyPoint.sum).isGreaterThan(0.0)
  }

  @Test
  fun `metrics - data_provider_name attribute present in metrics`() = runBlocking {
    whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(listOf(GROUPED_REQUISITIONS))

    val fetcher = createFetcher(metrics = testMetrics)

    fetcher.fetchAndStoreRequisitions()

    // Verify data_provider_name attribute is present
    val metrics = metricReader.collectAllMetrics()
    val latencyMetric = metrics.find { it.name == "edpa.requisition_fetcher.fetch_latency" }
    assertThat(latencyMetric).isNotNull()
    val latencyPoint = latencyMetric!!.histogramData.points.first() as HistogramPointData
    assertThat(latencyPoint.attributes.asMap())
      .containsKey(io.opentelemetry.api.common.AttributeKey.stringKey("edpa.data_provider_name"))
  }

  @Test
  fun `metrics - storageFails emitted on storage error`() = runBlocking {
    val groupedRequisitionsList =
      listOf(GROUPED_REQUISITIONS, GROUPED_REQUISITIONS_2, GROUPED_REQUISITIONS_3)
    whenever(requisitionGrouper.groupRequisitions(any())).thenReturn(groupedRequisitionsList)

    // Create a mock storage client that fails on the second write
    val mockStorageClient: StorageClient = mock()
    var writeCount = 0
    whenever(mockStorageClient.writeBlob(any<String>(), any<ByteString>())).thenAnswer {
      writeCount++
      if (writeCount == 2) {
        throw RuntimeException("Storage write failed")
      }
    }

    val fetcher = createFetcher(storageClient = mockStorageClient, metrics = testMetrics)

    // Expect the exception to be re-raised
    var exceptionThrown = false
    try {
      fetcher.fetchAndStoreRequisitions()
    } catch (e: Exception) {
      exceptionThrown = true
      assertThat(e.message).contains("Storage write failed")
    }
    assertThat(exceptionThrown).isTrue()

    // Verify metrics
    val metrics = metricReader.collectAllMetrics()

    // Verify storageWrites counter (1 successful write before failure)
    val writesMetric = metrics.find { it.name == "edpa.requisition_fetcher.storage_writes" }
    assertThat(writesMetric).isNotNull()
    val writesValue = (writesMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(writesValue).isEqualTo(1)

    // Verify storageFails counter (2 failed writes: the one that threw + the one not attempted)
    val failsMetric = metrics.find { it.name == "edpa.requisition_fetcher.storage_fails" }
    assertThat(failsMetric).isNotNull()
    val failsValue = (failsMetric!!.longSumData.points.first() as LongPointData).value
    assertThat(failsValue).isEqualTo(2)
  }

  companion object {
    init {
      EdpaTelemetry.ensureInitialized()
    }

    private const val STORAGE_PATH_PREFIX = "test-requisitions"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private val REQUISITION = requisition { name = "requisition-name" }
    private val GROUPED_REQUISITIONS = groupedRequisitions {
      requisitions.add(requisitionEntry { requisition = Any.pack(REQUISITION) })
      groupId = "1234"
    }
    private val GROUPED_REQUISITIONS_2 = groupedRequisitions {
      requisitions.add(requisitionEntry { requisition = Any.pack(REQUISITION) })
      groupId = "5678"
    }
    private val GROUPED_REQUISITIONS_3 = groupedRequisitions {
      requisitions.add(requisitionEntry { requisition = Any.pack(REQUISITION) })
      groupId = "9012"
    }
  }
}
