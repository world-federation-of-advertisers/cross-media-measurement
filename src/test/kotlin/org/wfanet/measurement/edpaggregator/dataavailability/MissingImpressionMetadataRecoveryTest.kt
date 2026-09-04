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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class MissingImpressionMetadataRecoveryTest {
  private val registeredMetadata = mutableMapOf<String, ImpressionMetadata>()
  private val listRequests = mutableListOf<ListImpressionMetadataRequest>()
  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            listRequests += request
            impressionMetadata +=
              request.filter.blobUrisList.mapNotNull { blobUri ->
                registeredMetadata[blobUri]?.takeIf {
                  if (request.showDeleted) it.state == ImpressionMetadata.State.DELETED
                  else it.state != ImpressionMetadata.State.DELETED
                }
              }
          }
        }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(impressionMetadataServiceMock) }

  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader
  private lateinit var metrics: MissingImpressionMetadataRecoveryMetrics

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    openTelemetry =
      OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal()
    metrics = MissingImpressionMetadataRecoveryMetrics(meterProvider.get("test"))
  }

  @After
  fun cleanupTelemetry() {
    openTelemetry.close()
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
  }

  @Test
  fun `recover distinguishes missing finalized blobs from deleted records with files`(): Unit =
    runBlocking {
      val storageClient = InMemoryStorageClient()
      val registeredUri = metadataUri("2026-08-01", "metadata-registered.json")
      val deletedUri = metadataUri("2026-08-03", "metadata-deleted.json")
      writeFinalizedMetadata(storageClient, "2026-08-01", "metadata-registered.json")
      writeFinalizedMetadata(storageClient, "2026-08-02", "metadata-missing.json")
      storageClient.writeBlob(
        metadataKey("2026-08-03", "metadata-deleted.json"),
        ByteString.copyFromUtf8("metadata"),
      )
      storageClient.writeBlob(
        "$EDP_IMPRESSION_PATH/model-line/model-line-1/2026-08-02/readme.txt",
        ByteString.copyFromUtf8("not metadata"),
      )
      registeredMetadata[registeredUri] = impressionMetadata {
        blobUri = registeredUri
        state = ImpressionMetadata.State.ACTIVE
      }
      registeredMetadata[deletedUri] = impressionMetadata {
        blobUri = deletedUri
        state = ImpressionMetadata.State.DELETED
      }
      val syncedDoneBlobUris = mutableListOf<String>()

      val result =
        buildRecovery(storageClient, impressionMetadataBatchSize = 100) { doneBlobUri, _ ->
            syncedDoneBlobUris += doneBlobUri
          }
          .recover()

      assertThat(result.finalizedMetadataBlobs).isEqualTo(2)
      assertThat(result.missingBlobs).isEqualTo(1)
      assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
      assertThat(result.recoveredBlobs).isEqualTo(1)
      assertThat(result.failedBlobs).isEqualTo(0)
      assertThat(result.dateFoldersResynced).isEqualTo(1)
      assertThat(syncedDoneBlobUris)
        .containsExactly("$BUCKET_URI/$EDP_IMPRESSION_PATH/model-line/model-line-1/2026-08-02/done")
    }

  @Test
  fun `recover syncs a date folder once for multiple missing blobs`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeFinalizedMetadata(storageClient, "2026-08-01", "metadata-a.json", "metadata-b.binpb")
    val syncedDoneBlobUris = mutableListOf<String>()
    val syncedMetadataBlobKeys = mutableSetOf<String>()

    val result =
      buildRecovery(storageClient, impressionMetadataBatchSize = 100) { doneBlobUri, blobKeys ->
          syncedDoneBlobUris += doneBlobUri
          syncedMetadataBlobKeys += blobKeys
        }
        .recover()

    assertThat(result.missingBlobs).isEqualTo(2)
    assertThat(result.recoveredBlobs).isEqualTo(2)
    assertThat(result.dateFoldersResynced).isEqualTo(1)
    assertThat(syncedDoneBlobUris).hasSize(1)
    assertThat(syncedMetadataBlobKeys)
      .containsExactly(
        metadataKey("2026-08-01", "metadata-a.json"),
        metadataKey("2026-08-01", "metadata-b.binpb"),
      )
    assertThat(metricValue(MISSING_BLOBS_METRIC)).isEqualTo(2)
    assertThat(metricValue(RECOVERED_BLOBS_METRIC)).isEqualTo(2)
    assertThat(metricValue(FAILED_BLOBS_METRIC)).isEqualTo(0)
    assertThat(metricValue(DELETED_RECORDS_WITH_BLOBS_METRIC)).isEqualTo(0)
  }

  @Test
  fun `recover passes only missing in-scope blob keys to sync`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    val deletedUri = metadataUri("2026-08-02", "metadata-deleted.json")
    writeFinalizedMetadata(
      storageClient,
      "2026-08-02",
      "metadata-missing.json",
      "metadata-deleted.json",
    )
    writeFinalizedMetadata(storageClient, "2025-01-01", "metadata-outside-lookback.json")
    writeFinalizedMetadata(storageClient, "2026-09-01", "metadata-future.json")
    registeredMetadata[deletedUri] = impressionMetadata {
      blobUri = deletedUri
      state = ImpressionMetadata.State.DELETED
    }
    val syncedMetadataBlobKeys = mutableSetOf<String>()

    val result =
      buildRecovery(storageClient, impressionMetadataBatchSize = 100) { _, blobKeys ->
          syncedMetadataBlobKeys += blobKeys
        }
        .recover()

    assertThat(result.finalizedMetadataBlobs).isEqualTo(2)
    assertThat(result.missingBlobs).isEqualTo(1)
    assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
    assertThat(syncedMetadataBlobKeys)
      .containsExactly(metadataKey("2026-08-02", "metadata-missing.json"))
  }

  @Test
  fun `recover continues after a date folder fails`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeFinalizedMetadata(storageClient, "2026-08-01", "metadata.json")
    writeFinalizedMetadata(storageClient, "2026-08-02", "metadata.json")

    val result =
      buildRecovery(storageClient, impressionMetadataBatchSize = 100) { doneBlobUri, _ ->
          if (doneBlobUri.contains("2026-08-01")) {
            error("sync failed")
          }
        }
        .recover()

    assertThat(result.missingBlobs).isEqualTo(2)
    assertThat(result.recoveredBlobs).isEqualTo(1)
    assertThat(result.failedBlobs).isEqualTo(1)
    assertThat(result.dateFoldersResynced).isEqualTo(1)
    assertThat(result.errors).hasSize(1)
    assertThat(result.errors.single().message).contains("sync failed")
    assertThat(metricValue(FAILED_BLOBS_METRIC)).isEqualTo(1)
  }

  @Test
  fun `recover lists blobs only from date folders in lookback window`(): Unit = runBlocking {
    val delegate = InMemoryStorageClient()
    writeFinalizedMetadata(delegate, "2026-08-31", "metadata-in-window.json")
    writeFinalizedMetadata(delegate, "2025-01-01", "metadata-outside-window.json")
    val storageClient = RecordingStorageClient(delegate)

    val result = buildRecovery(storageClient, impressionMetadataBatchSize = 100) { _, _ -> }.recover()

    assertThat(result.finalizedMetadataBlobs).isEqualTo(1)
    assertThat(storageClient.listedBlobPrefixes)
      .containsExactly("$EDP_IMPRESSION_PATH/model-line/model-line-1/2026-08-31/")
    assertThat(storageClient.listedBlobPrefixes)
      .doesNotContain("$EDP_IMPRESSION_PATH/model-line/model-line-1/2025-01-01/")
    assertThat(storageClient.listedBlobPrefixes).doesNotContain("$EDP_IMPRESSION_PATH/")
  }

  @Test
  fun `recover lists deleted resources and exact storage blob URIs`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    val firstUri = metadataUri("2026-08-01", "metadata.json")
    val secondUri = metadataUri("2026-08-02", "metadata.json")
    writeFinalizedMetadata(storageClient, "2026-08-01", "metadata.json")
    writeFinalizedMetadata(storageClient, "2026-08-02", "metadata.json")
    registeredMetadata[firstUri] = impressionMetadata { blobUri = firstUri }
    registeredMetadata[secondUri] = impressionMetadata {
      blobUri = secondUri
      state = ImpressionMetadata.State.DELETED
    }

    val result = buildRecovery(storageClient, impressionMetadataBatchSize = 2) { _, _ -> }.recover()

    assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
    assertThat(metricValue(DELETED_RECORDS_WITH_BLOBS_METRIC)).isEqualTo(1)
    assertThat(listRequests.map { it.showDeleted }).containsExactly(false, true).inOrder()
    for (request in listRequests) {
      assertThat(request.pageSize).isEqualTo(2)
      assertThat(request.filter.blobUrisList).containsExactly(firstUri, secondUri)
    }
  }

  private fun buildRecovery(
    storageClient: StorageClient,
    impressionMetadataBatchSize: Int,
    sync: suspend (String, Set<String>) -> Unit,
  ): MissingImpressionMetadataRecovery =
    MissingImpressionMetadataRecovery(
      storageClient = storageClient,
      storageRootUri = BlobUri(scheme = "gs", bucket = BUCKET_NAME, key = ""),
      edpImpressionPath = EDP_IMPRESSION_PATH,
      impressionMetadataStub = impressionMetadataStub,
      dataProviderName = DATA_PROVIDER_NAME,
      throttler =
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T = block()
        },
      impressionMetadataBatchSize = impressionMetadataBatchSize,
      earliestDataDate = LocalDate.parse("2026-06-01"),
      latestDataDate = LocalDate.parse("2026-08-31"),
      sync = sync,
      metrics = metrics,
    )

  private suspend fun writeFinalizedMetadata(
    storageClient: InMemoryStorageClient,
    date: String,
    vararg fileNames: String,
  ) {
    for (fileName in fileNames) {
      storageClient.writeBlob(metadataKey(date, fileName), ByteString.copyFromUtf8("metadata"))
    }
    storageClient.writeBlob(
      "$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/done",
      ByteString.EMPTY,
    )
  }

  private fun metadataKey(date: String, fileName: String): String =
    "$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/$fileName"

  private fun metadataUri(date: String, fileName: String): String =
    "$BUCKET_URI/${metadataKey(date, fileName)}"

  private fun metricValue(name: String): Long {
    metricReader.forceFlush()
    val metric: MetricData = metricExporter.finishedMetricItems.last { it.name == name }
    return metric.longGaugeData.points.single().value
  }

  private class RecordingStorageClient(private val delegate: StorageClient) :
    StorageClient by delegate {
    val listedBlobPrefixes = mutableListOf<String?>()

    override suspend fun listBlobs(prefix: String?) =
      delegate.listBlobs(prefix).also { listedBlobPrefixes += prefix }
  }

  companion object {
    private const val BUCKET_NAME = "test-bucket"
    private const val BUCKET_URI = "gs://$BUCKET_NAME"
    private const val EDP_IMPRESSION_PATH = "edp/test/vid-labeled-impressions"
    private const val DATA_PROVIDER_NAME = "dataProviders/test-provider"
    private const val MISSING_BLOBS_METRIC = "edpa.data_availability_recovery.missing_blobs"
    private const val RECOVERED_BLOBS_METRIC = "edpa.data_availability_recovery.recovered_blobs"
    private const val FAILED_BLOBS_METRIC = "edpa.data_availability_recovery.failed_blobs"
    private const val DELETED_RECORDS_WITH_BLOBS_METRIC =
      "edpa.data_availability_recovery.deleted_records_with_blobs"
  }
}
