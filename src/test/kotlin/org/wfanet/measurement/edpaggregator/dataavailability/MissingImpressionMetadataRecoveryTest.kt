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
import io.grpc.Status
import io.grpc.StatusException
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
import org.wfanet.measurement.edpaggregator.v1alpha.UndeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class MissingImpressionMetadataRecoveryTest {
  private val registeredMetadata = mutableMapOf<String, ImpressionMetadata>()
  private val listRequests = mutableListOf<ListImpressionMetadataRequest>()
  private val undeleteRequests = mutableListOf<UndeleteImpressionMetadataRequest>()
  private var undeleteError: StatusException? = null
  private var publicationCompletes = true
  private val impressionMetadataServiceMock: ImpressionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listImpressionMetadata(any<ListImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListImpressionMetadataRequest>(0)
          listImpressionMetadataResponse {
            listRequests += request
            impressionMetadata +=
              registeredMetadata.values.filter {
                it.blobUri.startsWith(request.filter.blobUriPrefix) &&
                  (request.showDeleted || it.state != ImpressionMetadata.State.DELETED) &&
                  (request.filter.state == ImpressionMetadata.State.STATE_UNSPECIFIED ||
                    it.state == request.filter.state)
              }
          }
        }
      onBlocking { undeleteImpressionMetadata(any<UndeleteImpressionMetadataRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<UndeleteImpressionMetadataRequest>(0)
          undeleteRequests += request
          undeleteError?.let { throw it }
          val entry = registeredMetadata.entries.single { it.value.name == request.name }
          val undeleted = entry.value.copy { state = ImpressionMetadata.State.ACTIVE }
          registeredMetadata[entry.key] = undeleted
          undeleted
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
        name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-1"
        blobUri = deletedUri
        state = ImpressionMetadata.State.DELETED
      }
      val syncedDoneBlobUris = mutableListOf<String>()

      val result =
        buildRecovery(
            storageClient,
            impressionMetadataBatchSize = 100,
            registerSyncedMetadata = true,
          ) { doneBlobUri, _ ->
            syncedDoneBlobUris += doneBlobUri
          }
          .recover()

      assertThat(result.finalizedMetadataBlobs).isEqualTo(2)
      assertThat(result.missingBlobs).isEqualTo(1)
      assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
      assertThat(result.undeletedRecords).isEqualTo(1)
      assertThat(result.failedUndeletes).isEqualTo(0)
      assertThat(result.recoveredBlobs).isEqualTo(1)
      assertThat(result.failedBlobs).isEqualTo(0)
      assertThat(result.dateFoldersResynced).isEqualTo(1)
      assertThat(syncedDoneBlobUris)
        .containsExactly("$BUCKET_URI/$EDP_IMPRESSION_PATH/model-line/model-line-1/2026-08-02/done")
      assertThat(undeleteRequests.single().name)
        .isEqualTo("$DATA_PROVIDER_NAME/impressionMetadata/deleted-1")
    }

  @Test
  fun `recover syncs a date folder once for multiple missing blobs`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeFinalizedMetadata(storageClient, "2026-08-01", "metadata-a.json", "metadata-b.binpb")
    val syncedDoneBlobUris = mutableListOf<String>()
    val syncedMetadataBlobKeys = mutableSetOf<String>()

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { doneBlobUri, blobKeys ->
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
    assertThat(metricValue(FAILED_BLOBS_METRIC)).isEqualTo(0)
    assertThat(metricValue(DELETED_RECORDS_WITH_BLOBS_METRIC)).isEqualTo(0)
    assertThat(metricValue(FAILED_UNDELETES_METRIC)).isEqualTo(0)
  }

  @Test
  fun `recover passes only repaired in-scope blob keys to sync`(): Unit = runBlocking {
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
      name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-2"
      blobUri = deletedUri
      state = ImpressionMetadata.State.DELETED
    }
    val syncedMetadataBlobKeys = mutableSetOf<String>()

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { _, blobKeys ->
          syncedMetadataBlobKeys += blobKeys
        }
        .recover()

    assertThat(result.finalizedMetadataBlobs).isEqualTo(2)
    assertThat(result.missingBlobs).isEqualTo(1)
    assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
    assertThat(result.undeletedRecords).isEqualTo(1)
    assertThat(syncedMetadataBlobKeys)
      .containsExactly(
        metadataKey("2026-08-02", "metadata-missing.json"),
        metadataKey("2026-08-02", "metadata-deleted.json"),
      )
  }

  @Test
  fun `recover resyncs a finalized folder after undeleting its only record`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    val deletedUri = metadataUri("2026-08-03", "metadata-deleted.json")
    writeFinalizedMetadata(storageClient, "2026-08-03", "metadata-deleted.json")
    registeredMetadata[deletedUri] = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-only"
      blobUri = deletedUri
      state = ImpressionMetadata.State.DELETED
    }
    val syncedMetadataBlobKeys = mutableSetOf<String>()

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { _, blobKeys ->
          syncedMetadataBlobKeys += blobKeys
        }
        .recover()

    assertThat(result.missingBlobs).isEqualTo(0)
    assertThat(result.undeletedRecords).isEqualTo(1)
    assertThat(result.dateFoldersResynced).isEqualTo(1)
    assertThat(syncedMetadataBlobKeys)
      .containsExactly(metadataKey("2026-08-03", "metadata-deleted.json"))
    assertThat(registeredMetadata.getValue(deletedUri).name)
      .isEqualTo("$DATA_PROVIDER_NAME/impressionMetadata/deleted-only")
  }

  @Test
  fun `recover continues after a date folder fails`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeFinalizedMetadata(storageClient, "2026-08-01", "metadata.json")
    writeFinalizedMetadata(storageClient, "2026-08-02", "metadata.json")

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { doneBlobUri, _ ->
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

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { _, _ ->
        }
        .recover()

    assertThat(result.finalizedMetadataBlobs).isEqualTo(1)
    assertThat(storageClient.listedBlobPrefixes)
      .containsExactly("$EDP_IMPRESSION_PATH/model-line/model-line-1/2026-08-31/")
    assertThat(storageClient.listedBlobPrefixes)
      .doesNotContain("$EDP_IMPRESSION_PATH/model-line/model-line-1/2025-01-01/")
    assertThat(storageClient.listedBlobPrefixes).doesNotContain("$EDP_IMPRESSION_PATH/")
  }

  @Test
  fun `recover lists active and deleted resources by date folder prefix`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    val firstUri = metadataUri("2026-08-01", "metadata.json")
    val secondUri = metadataUri("2026-08-02", "metadata.json")
    writeFinalizedMetadata(storageClient, "2026-08-01", "metadata.json")
    writeFinalizedMetadata(storageClient, "2026-08-02", "metadata.json")
    registeredMetadata[firstUri] = impressionMetadata { blobUri = firstUri }
    registeredMetadata[secondUri] = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-3"
      blobUri = secondUri
      state = ImpressionMetadata.State.DELETED
    }

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 2,
          registerSyncedMetadata = true,
        ) { _, _ ->
        }
        .recover()

    assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
    assertThat(result.undeletedRecords).isEqualTo(1)
    assertThat(result.failedUndeletes).isEqualTo(0)
    assertThat(metricValue(DELETED_RECORDS_WITH_BLOBS_METRIC)).isEqualTo(1)
    assertThat(metricValue(FAILED_UNDELETES_METRIC)).isEqualTo(0)
    assertThat(listRequests).hasSize(3)
    for (request in listRequests) {
      assertThat(request.pageSize).isEqualTo(2)
      assertThat(request.showDeleted).isTrue()
      assertThat(request.filter.blobUrisList).isEmpty()
    }
    assertThat(listRequests.map { it.filter.blobUriPrefix })
      .containsExactly(
        dateFolderUriPrefix("2026-08-01"),
        dateFolderUriPrefix("2026-08-02"),
        dateFolderUriPrefix("2026-08-02"),
      )
      .inOrder()
  }

  @Test
  fun `recover reports undelete failures and continues`() = runBlocking {
    val storageClient = InMemoryStorageClient()
    val deletedUri = metadataUri("2026-08-03", "metadata-deleted.json")
    storageClient.writeBlob(
      metadataKey("2026-08-03", "metadata-deleted.json"),
      ByteString.copyFromUtf8("metadata"),
    )
    registeredMetadata[deletedUri] = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-failure"
      blobUri = deletedUri
      state = ImpressionMetadata.State.DELETED
    }
    undeleteError = Status.INTERNAL.withDescription("undelete failed").asException()

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { _, _ ->
        }
        .recover()

    assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
    assertThat(result.undeletedRecords).isEqualTo(0)
    assertThat(result.failedUndeletes).isEqualTo(1)
    assertThat(result.errors.single().target)
      .isEqualTo("$DATA_PROVIDER_NAME/impressionMetadata/deleted-failure")
    assertThat(metricValue(FAILED_UNDELETES_METRIC)).isEqualTo(1)
  }

  @Test
  fun `recover does not sync unmarked blob when undelete fails`() = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeFinalizedMetadata(storageClient, "2026-08-03", "metadata-deleted.json")
    val deletedBlobKey = metadataKey("2026-08-03", "metadata-deleted.json")
    val deletedUri = metadataUri("2026-08-03", "metadata-deleted.json")
    storageClient.writeBlob(deletedBlobKey, ByteString.copyFromUtf8("rewritten metadata"))
    registeredMetadata[deletedUri] = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-failure"
      blobUri = deletedUri
      modelLine = MODEL_LINE_NAME
      state = ImpressionMetadata.State.DELETED
    }
    undeleteError = Status.PERMISSION_DENIED.asException()
    var syncCalls = 0

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { _, _ ->
          syncCalls++
        }
        .recover()

    assertThat(result.failedUndeletes).isEqualTo(1)
    assertThat(result.dateFoldersResynced).isEqualTo(0)
    assertThat(syncCalls).isEqualTo(0)
  }

  @Test
  fun `recover treats concurrent undelete as success`() = runBlocking {
    val storageClient = InMemoryStorageClient()
    val deletedUri = metadataUri("2026-08-03", "metadata-deleted.json")
    storageClient.writeBlob(
      metadataKey("2026-08-03", "metadata-deleted.json"),
      ByteString.copyFromUtf8("metadata"),
    )
    registeredMetadata[deletedUri] = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-race"
      blobUri = deletedUri
      state = ImpressionMetadata.State.DELETED
    }
    undeleteError = Status.ALREADY_EXISTS.asException()

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { _, _ ->
        }
        .recover()

    assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
    assertThat(result.undeletedRecords).isEqualTo(1)
    assertThat(result.failedUndeletes).isEqualTo(0)
    assertThat(result.errors).isEmpty()
  }

  @Test
  fun `recover reports metadata omitted by sync as failed`() = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeFinalizedMetadata(storageClient, "2026-08-03", "metadata-skipped.json")

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = false,
        ) { _, _ ->
        }
        .recover()

    assertThat(result.missingBlobs).isEqualTo(1)
    assertThat(result.recoveredBlobs).isEqualTo(0)
    assertThat(result.failedBlobs).isEqualTo(1)
    assertThat(result.dateFoldersResynced).isEqualTo(0)
    assertThat(result.errors.single().message).contains("still missing or inactive")
    assertThat(metricValue(MISSING_BLOBS_METRIC)).isEqualTo(1)
    assertThat(metricValue(FAILED_BLOBS_METRIC)).isEqualTo(1)
  }

  @Test
  fun `recover reports undeleted metadata that is inactive after sync as failed`() = runBlocking {
    val storageClient = InMemoryStorageClient()
    val deletedUri = metadataUri("2026-08-03", "metadata-deleted.json")
    writeFinalizedMetadata(storageClient, "2026-08-03", "metadata-deleted.json")
    registeredMetadata[deletedUri] = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/deleted-after-sync"
      blobUri = deletedUri
      state = ImpressionMetadata.State.DELETED
    }

    val result =
      buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = false,
        ) { _, _ ->
          registeredMetadata[deletedUri] =
            registeredMetadata.getValue(deletedUri).copy {
              state = ImpressionMetadata.State.DELETED
            }
        }
        .recover()

    assertThat(result.deletedRecordsWithBlobs).isEqualTo(1)
    assertThat(result.undeletedRecords).isEqualTo(0)
    assertThat(result.failedUndeletes).isEqualTo(1)
    assertThat(result.dateFoldersResynced).isEqualTo(0)
    assertThat(result.errors.single().message).contains("still missing or inactive")
    assertThat(metricValue(FAILED_UNDELETES_METRIC)).isEqualTo(1)
  }

  @Test
  fun `recover backfills legacy folder once with representative per model line`(): Unit =
    runBlocking {
      val storageClient = InMemoryStorageClient()
      writeIncompleteFullSyncMetadata(
        storageClient,
        "2026-08-03",
        "metadata-a.json",
        "metadata-b.json",
        "metadata-c.json",
        includeSyncId = false,
      )
      for (fileName in listOf("metadata-a.json", "metadata-b.json")) {
        val uri = metadataUri("2026-08-03", fileName)
        registeredMetadata[uri] = impressionMetadata {
          name = "$DATA_PROVIDER_NAME/impressionMetadata/$fileName"
          blobUri = uri
          modelLine = MODEL_LINE_NAME
          state = ImpressionMetadata.State.ACTIVE
        }
      }
      val secondModelLineUri = metadataUri("2026-08-03", "metadata-c.json")
      registeredMetadata[secondModelLineUri] = impressionMetadata {
        name = "$DATA_PROVIDER_NAME/impressionMetadata/metadata-c.json"
        blobUri = secondModelLineUri
        modelLine = SECOND_MODEL_LINE_NAME
        state = ImpressionMetadata.State.ACTIVE
      }
      val syncedBlobKeys = mutableListOf<Set<String>>()
      val recovery =
        buildRecovery(
          storageClient,
          impressionMetadataBatchSize = 100,
          registerSyncedMetadata = true,
        ) { _, blobKeys ->
          syncedBlobKeys += blobKeys
        }

      val firstResult = recovery.recover()
      val secondResult = recovery.recover()

      assertThat(firstResult.missingBlobs).isEqualTo(0)
      assertThat(firstResult.incompleteFullSyncFolders).isEqualTo(1)
      assertThat(firstResult.dateFoldersResynced).isEqualTo(1)
      assertThat(firstResult.errors).isEmpty()
      assertThat(syncedBlobKeys).hasSize(1)
      assertThat(syncedBlobKeys.single()).hasSize(2)
      assertThat(
          syncedBlobKeys.single().map { blobKey ->
            registeredMetadata.getValue("$BUCKET_URI/$blobKey").modelLine
          }
        )
        .containsExactly(MODEL_LINE_NAME, SECOND_MODEL_LINE_NAME)
      assertThat(secondResult.incompleteFullSyncFolders).isEqualTo(0)
      assertThat(secondResult.dateFoldersResynced).isEqualTo(0)
    }

  @Test
  fun `recover retries overwritten active blob in published folder once`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeFinalizedMetadata(storageClient, "2026-08-03", "metadata-a.json", "metadata-b.json")
    val overwrittenBlobKey = metadataKey("2026-08-03", "metadata-a.json")
    storageClient.writeBlob(overwrittenBlobKey, ByteString.copyFromUtf8("rewritten metadata"))
    for (fileName in listOf("metadata-a.json", "metadata-b.json")) {
      val uri = metadataUri("2026-08-03", fileName)
      registeredMetadata[uri] = impressionMetadata {
        name = "$DATA_PROVIDER_NAME/impressionMetadata/$fileName"
        blobUri = uri
        modelLine = MODEL_LINE_NAME
        state = ImpressionMetadata.State.ACTIVE
      }
    }
    val syncedBlobKeys = mutableListOf<Set<String>>()
    val recovery =
      buildRecovery(
        storageClient,
        impressionMetadataBatchSize = 100,
        registerSyncedMetadata = true,
      ) { _, blobKeys ->
        syncedBlobKeys += blobKeys
      }

    val firstResult = recovery.recover()
    val secondResult = recovery.recover()

    assertThat(firstResult.incompleteFullSyncFolders).isEqualTo(0)
    assertThat(firstResult.dateFoldersResynced).isEqualTo(1)
    assertThat(firstResult.errors).isEmpty()
    assertThat(syncedBlobKeys).containsExactly(setOf(overwrittenBlobKey))
    assertThat(secondResult.dateFoldersResynced).isEqualTo(0)
    assertThat(secondResult.errors).isEmpty()
  }

  @Test
  fun `recover retries every unmarked metadata blob in incomplete folder`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeIncompleteFullSyncMetadata(
      storageClient,
      "2026-08-03",
      "metadata-a.json",
      "metadata-b.json",
      metadataBlobsSynced = false,
    )
    for (fileName in listOf("metadata-a.json", "metadata-b.json")) {
      val uri = metadataUri("2026-08-03", fileName)
      registeredMetadata[uri] = impressionMetadata {
        name = "$DATA_PROVIDER_NAME/impressionMetadata/$fileName"
        blobUri = uri
        modelLine = MODEL_LINE_NAME
        state = ImpressionMetadata.State.ACTIVE
      }
    }
    val syncedBlobKeys = mutableListOf<Set<String>>()
    val recovery =
      buildRecovery(
        storageClient,
        impressionMetadataBatchSize = 100,
        registerSyncedMetadata = true,
      ) { _, blobKeys ->
        syncedBlobKeys += blobKeys
      }

    val result = recovery.recover()

    assertThat(result.missingBlobs).isEqualTo(0)
    assertThat(result.incompleteFullSyncFolders).isEqualTo(1)
    assertThat(result.dateFoldersResynced).isEqualTo(1)
    assertThat(result.errors).isEmpty()
    assertThat(syncedBlobKeys).hasSize(1)
    assertThat(syncedBlobKeys.single())
      .containsExactly(
        metadataKey("2026-08-03", "metadata-a.json"),
        metadataKey("2026-08-03", "metadata-b.json"),
      )
  }

  @Test
  fun `recover retries when gap-blocked sync leaves publication incomplete`(): Unit = runBlocking {
    val storageClient = InMemoryStorageClient()
    writeIncompleteFullSyncMetadata(storageClient, "2026-08-03", "metadata.json")
    val uri = metadataUri("2026-08-03", "metadata.json")
    registeredMetadata[uri] = impressionMetadata {
      name = "$DATA_PROVIDER_NAME/impressionMetadata/metadata"
      blobUri = uri
      modelLine = MODEL_LINE_NAME
      state = ImpressionMetadata.State.ACTIVE
    }
    publicationCompletes = false
    var syncCalls = 0
    val recovery =
      buildRecovery(
        storageClient,
        impressionMetadataBatchSize = 100,
        registerSyncedMetadata = true,
      ) { _, _ ->
        syncCalls++
      }

    val firstResult = recovery.recover()
    val secondResult = recovery.recover()

    assertThat(firstResult.incompleteFullSyncFolders).isEqualTo(1)
    assertThat(firstResult.dateFoldersResynced).isEqualTo(0)
    assertThat(firstResult.errors.single().message).contains("publication")
    assertThat(secondResult.incompleteFullSyncFolders).isEqualTo(1)
    assertThat(secondResult.dateFoldersResynced).isEqualTo(0)
    assertThat(secondResult.errors.single().message).contains("publication")
    assertThat(syncCalls).isEqualTo(2)
  }

  private fun buildRecovery(
    storageClient: BlobMetadataStorageClient,
    impressionMetadataBatchSize: Int,
    registerSyncedMetadata: Boolean,
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
      sync = { doneBlobUri, blobKeys ->
        sync(doneBlobUri, blobKeys)
        for (blobKey in blobKeys) {
          storageClient.updateBlobMetadata(
            blobKey,
            metadata =
              mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
          )
        }
        if (registerSyncedMetadata) {
          for (blobKey in blobKeys) {
            val blobUri = "$BUCKET_URI/$blobKey"
            registeredMetadata.putIfAbsent(
              blobUri,
              impressionMetadata {
                name = "$DATA_PROVIDER_NAME/impressionMetadata/recovered-${registeredMetadata.size}"
                this.blobUri = blobUri
                state = ImpressionMetadata.State.ACTIVE
              },
            )
          }
        }
        if (publicationCompletes) {
          val doneBlobKey = doneBlobUri.removePrefix("$BUCKET_URI/")
          storageClient.updateBlobMetadata(
            doneBlobKey,
            metadata =
              mapOf(
                DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
                DataAvailabilityBlobs.SYNC_ID_KEY to TEST_SYNC_ID,
                DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY to TEST_SYNC_ID,
              ),
          )
        }
      },
      metrics = metrics,
    )

  private suspend fun writeFinalizedMetadata(
    storageClient: InMemoryStorageClient,
    date: String,
    vararg fileNames: String,
  ) {
    for (fileName in fileNames) {
      val blobKey = metadataKey(date, fileName)
      storageClient.writeBlob(blobKey, ByteString.copyFromUtf8("metadata"))
      storageClient.updateBlobMetadata(
        blobKey,
        metadata =
          mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
      )
    }
    storageClient.writeBlob(
      "$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/done",
      ByteString.EMPTY,
    )
    storageClient.updateBlobMetadata(
      "$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/done",
      metadata =
        mapOf(
          DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE,
          DataAvailabilityBlobs.SYNC_ID_KEY to TEST_SYNC_ID,
          DataAvailabilityBlobs.PUBLISHED_SYNC_ID_KEY to TEST_SYNC_ID,
        ),
    )
  }

  private suspend fun writeIncompleteFullSyncMetadata(
    storageClient: InMemoryStorageClient,
    date: String,
    vararg fileNames: String,
    includeSyncId: Boolean = true,
    metadataBlobsSynced: Boolean = true,
  ) {
    for (fileName in fileNames) {
      val blobKey = metadataKey(date, fileName)
      storageClient.writeBlob(blobKey, ByteString.copyFromUtf8("metadata"))
      if (metadataBlobsSynced) {
        storageClient.updateBlobMetadata(
          blobKey,
          metadata =
            mapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE),
        )
      }
    }
    val doneBlobKey = "$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/done"
    storageClient.writeBlob(doneBlobKey, ByteString.EMPTY)
    val metadata =
      mutableMapOf(DataAvailabilityBlobs.SYNCED_BY_KEY to DataAvailabilityBlobs.SYNCED_BY_VALUE)
    if (includeSyncId) {
      metadata[DataAvailabilityBlobs.SYNC_ID_KEY] = TEST_SYNC_ID
    }
    storageClient.updateBlobMetadata(doneBlobKey, metadata = metadata)
  }

  private fun metadataKey(date: String, fileName: String): String =
    "$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/$fileName"

  private fun metadataUri(date: String, fileName: String): String =
    "$BUCKET_URI/${metadataKey(date, fileName)}"

  private fun dateFolderUriPrefix(date: String): String =
    "$BUCKET_URI/$EDP_IMPRESSION_PATH/model-line/model-line-1/$date/"

  private fun metricValue(name: String): Long {
    metricReader.forceFlush()
    val metric: MetricData = metricExporter.finishedMetricItems.last { it.name == name }
    return metric.longGaugeData.points.single().value
  }

  private class RecordingStorageClient(private val delegate: BlobMetadataStorageClient) :
    BlobMetadataStorageClient by delegate {
    val listedBlobPrefixes = mutableListOf<String?>()

    override suspend fun listBlobs(prefix: String?) =
      delegate.listBlobs(prefix).also { listedBlobPrefixes += prefix }
  }

  companion object {
    private const val BUCKET_NAME = "test-bucket"
    private const val BUCKET_URI = "gs://$BUCKET_NAME"
    private const val EDP_IMPRESSION_PATH = "edp/test/vid-labeled-impressions"
    private const val DATA_PROVIDER_NAME = "dataProviders/test-provider"
    private const val MODEL_LINE_NAME =
      "modelProviders/test-provider/modelSuites/test-suite/modelLines/model-line-1"
    private const val SECOND_MODEL_LINE_NAME =
      "modelProviders/test-provider/modelSuites/test-suite/modelLines/model-line-2"
    private const val MISSING_BLOBS_METRIC = "edpa.data_availability_recovery.missing_blobs"
    private const val TEST_SYNC_ID = "test-sync-id"
    private const val FAILED_BLOBS_METRIC = "edpa.data_availability_recovery.failed_blobs"
    private const val FAILED_UNDELETES_METRIC = "edpa.data_availability_recovery.failed_undeletes"
    private const val DELETED_RECORDS_WITH_BLOBS_METRIC =
      "edpa.data_availability_recovery.deleted_records_with_blobs"
  }
}
