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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanup
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerDataAvailabilitySyncLeaseService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerImpressionMetadataService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataKey
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchUndeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchUpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.CreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkImpressionMetadataDeletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.UpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markImpressionMetadataDeletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.updateImpressionMetadataRequest
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.BatchCreateImpressionMetadataRequest as InternalBatchCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.BatchUndeleteImpressionMetadataRequest as InternalBatchUndeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.BatchUpdateImpressionMetadataRequest as InternalBatchUpdateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ComputeModelLineBoundsRequest as InternalComputeModelLineBoundsRequest
import org.wfanet.measurement.internal.edpaggregator.CreateImpressionMetadataRequest as InternalCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteImpressionMetadataRequest as InternalDeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.GetImpressionMetadataRequest as InternalGetImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase as InternalImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub as InternalImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest as InternalListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.MarkImpressionMetadataDeletedRequest as InternalMarkImpressionMetadataDeletedRequest
import org.wfanet.measurement.internal.edpaggregator.UpdateImpressionMetadataRequest as InternalUpdateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.copy as internalCopy
import org.wfanet.measurement.internal.edpaggregator.markImpressionMetadataDeletedRequest as internalMarkImpressionMetadataDeletedRequest
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class DataAvailabilityCleanupRaceTest {
  private val directThrottler =
    object : Throttler {
      override suspend fun <T> onReady(block: suspend () -> T): T = block()
    }

  private val database =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)
  private var legacyInternalApi = false
  private var legacyDeleteCalls = 0
  private var legacyPublicApi = false
  private var legacyPublicDeleteCalls = 0
  private var beforeBatchUpdate: suspend () -> Unit = {}
  private var beforeBatchCreate: suspend () -> Unit = {}
  private var beforeBatchUndelete: suspend () -> Unit = {}
  private val internalServer = GrpcTestServerRule {
    val service = SpannerImpressionMetadataService(database.databaseClient)
    addService(
      object : InternalImpressionMetadataServiceCoroutineImplBase() {
        override suspend fun batchCreateImpressionMetadata(
          request: InternalBatchCreateImpressionMetadataRequest
        ) = service.batchCreateImpressionMetadata(request)

        override suspend fun batchUpdateImpressionMetadata(
          request: InternalBatchUpdateImpressionMetadataRequest
        ) = service.batchUpdateImpressionMetadata(request)

        override suspend fun batchUndeleteImpressionMetadata(
          request: InternalBatchUndeleteImpressionMetadataRequest
        ) = service.batchUndeleteImpressionMetadata(request)

        override suspend fun computeModelLineBounds(
          request: InternalComputeModelLineBoundsRequest
        ) = service.computeModelLineBounds(request)

        override suspend fun createImpressionMetadata(
          request: InternalCreateImpressionMetadataRequest
        ) = service.createImpressionMetadata(request)

        override suspend fun updateImpressionMetadata(
          request: InternalUpdateImpressionMetadataRequest
        ) = service.updateImpressionMetadata(request)

        override suspend fun getImpressionMetadata(request: InternalGetImpressionMetadataRequest) =
          service.getImpressionMetadata(request)

        override suspend fun listImpressionMetadata(
          request: InternalListImpressionMetadataRequest
        ) = service.listImpressionMetadata(request)

        override suspend fun deleteImpressionMetadata(
          request: InternalDeleteImpressionMetadataRequest
        ) =
          service.deleteImpressionMetadata(
            if (legacyInternalApi) {
              legacyDeleteCalls++
              request.internalCopy { clearEtag() }
            } else {
              request
            }
          )

        override suspend fun markImpressionMetadataDeleted(
          request: InternalMarkImpressionMetadataDeletedRequest
        ) =
          if (legacyInternalApi) {
            super.markImpressionMetadataDeleted(request)
          } else {
            service.markImpressionMetadataDeleted(request)
          }
      }
    )
  }
  private val publicServer = GrpcTestServerRule {
    addService(
      object : DataProvidersGrpcKt.DataProvidersCoroutineImplBase() {
        override suspend fun replaceDataAvailabilityIntervals(
          request: ReplaceDataAvailabilityIntervalsRequest
        ) = DataProvider.getDefaultInstance()
      }
    )
    val service =
      ImpressionMetadataService(
        InternalImpressionMetadataServiceCoroutineStub(internalServer.channel)
      )
    addService(
      object : ImpressionMetadataServiceCoroutineImplBase() {
        override suspend fun batchCreateImpressionMetadata(
          request: BatchCreateImpressionMetadataRequest
        ): org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataResponse {
          beforeBatchCreate()
          return service.batchCreateImpressionMetadata(request)
        }

        override suspend fun batchUpdateImpressionMetadata(
          request: BatchUpdateImpressionMetadataRequest
        ): org.wfanet.measurement.edpaggregator.v1alpha.BatchUpdateImpressionMetadataResponse {
          beforeBatchUpdate()
          return service.batchUpdateImpressionMetadata(request)
        }

        override suspend fun batchUndeleteImpressionMetadata(
          request: BatchUndeleteImpressionMetadataRequest
        ): org.wfanet.measurement.edpaggregator.v1alpha.BatchUndeleteImpressionMetadataResponse {
          beforeBatchUndelete()
          return service.batchUndeleteImpressionMetadata(request)
        }

        override suspend fun computeModelLineBounds(request: ComputeModelLineBoundsRequest) =
          computeModelLineBoundsResponse {
            modelLineBounds += modelLineBoundMapEntry {
              key = "modelProviders/provider-1/modelSuites/suite-1/modelLines/line-1"
              value = interval {
                startTime = timestamp { seconds = 300 }
                endTime = timestamp { seconds = 400 }
              }
            }
          }

        override suspend fun createImpressionMetadata(request: CreateImpressionMetadataRequest) =
          service.createImpressionMetadata(request)

        override suspend fun updateImpressionMetadata(request: UpdateImpressionMetadataRequest) =
          service.updateImpressionMetadata(request)

        // Reads can reach an upgraded replica while deletion reaches a legacy replica.
        override suspend fun getImpressionMetadata(request: GetImpressionMetadataRequest) =
          service.getImpressionMetadata(request)

        override suspend fun listImpressionMetadata(request: ListImpressionMetadataRequest) =
          service.listImpressionMetadata(request)

        override suspend fun deleteImpressionMetadata(request: DeleteImpressionMetadataRequest) =
          service.deleteImpressionMetadata(
            if (legacyPublicApi) {
              legacyPublicDeleteCalls++
              request.copy { clearEtag() }
            } else {
              request
            }
          )

        override suspend fun markImpressionMetadataDeleted(
          request: MarkImpressionMetadataDeletedRequest
        ) =
          if (legacyPublicApi) {
            super.markImpressionMetadataDeleted(request)
          } else {
            service.markImpressionMetadataDeleted(request)
          }
      }
    )
  }

  @get:Rule
  val serverRules: TestRule = chainRulesSequentially(database, internalServer, publicServer)

  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `sync restores replacement when cleanup finishes before its update`() = runBlocking {
    coroutineScope {
      val original = createMetadata(generation = 0L)
      val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
      val storage = TestMetadataStorage(FileSystemStorageClient(temporaryFolder.newFolder()))
      val absenceObserved = CompletableDeferred<Unit>()
      val finishCleanup = CompletableDeferred<Unit>()
      val pausedStorage =
        object : StorageClient by storage {
          override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
            val result = storage.getBlob(blobKey)
            absenceObserved.complete(Unit)
            finishCleanup.await()
            return result
          }
        }
      val cleanup = async {
        DataAvailabilityCleanup(
            stub,
            DATA_PROVIDER_NAME,
            pausedStorage,
            throttler = directThrottler,
          )
          .cleanup(BLOB_URI, original.name)
      }
      absenceObserved.await()
      seedReplacement(storage, original)
      val updateStarted = CompletableDeferred<Unit>()
      val finishUpdate = CompletableDeferred<Unit>()
      beforeBatchUpdate = {
        updateStarted.complete(Unit)
        finishUpdate.await()
      }
      val sync = async { newSync(storage).sync("gs://bucket/path/to/done", LEASE_NAME, 1L) {} }
      updateStarted.await()
      finishCleanup.complete(Unit)
      try {
        assertThat(cleanup.await().status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)
      } finally {
        finishUpdate.complete(Unit)
      }
      sync.await()
      val actual = stub.getImpressionMetadata(getImpressionMetadataRequest { name = original.name })
      assertThat(actual.interval.startTime.seconds).isEqualTo(300L)
      assertThat(actual.state).isEqualTo(ImpressionMetadata.State.ACTIVE)
      assertThat(storage.getBlob(BLOB_KEY)).isNotNull()
    }
  }

  @Test
  fun `sync cleans up replacement sidecar deleted before it restores metadata`() = runBlocking {
    assertDeletedBeforeRestore(BLOB_KEY)
  }

  @Test
  fun `sync cleans up impressions deleted before it restores metadata`() = runBlocking {
    assertDeletedBeforeRestore("path/to/impressions")
  }

  private suspend fun assertDeletedBeforeRestore(deletedBlobKey: String) {
    coroutineScope {
      val original = createMetadata(generation = 0L)
      val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
      stub.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = original.name })
      val storage = TestMetadataStorage(FileSystemStorageClient(temporaryFolder.newFolder()))
      seedReplacement(storage, original)
      val restoreStarted = CompletableDeferred<Unit>()
      val finishRestore = CompletableDeferred<Unit>()
      beforeBatchUndelete = {
        restoreStarted.complete(Unit)
        finishRestore.await()
      }
      val sync = async { newSync(storage).sync("gs://bucket/path/to/done", LEASE_NAME, 1L) {} }
      restoreStarted.await()
      storage.getBlob(deletedBlobKey)!!.delete()
      val result =
        DataAvailabilityCleanup(stub, DATA_PROVIDER_NAME, storage, throttler = directThrottler)
          .cleanup("gs://bucket/$deletedBlobKey", original.name)
      assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SKIPPED)
      finishRestore.complete(Unit)
      sync.await()
      val actual = stub.getImpressionMetadata(getImpressionMetadataRequest { name = original.name })
      assertThat(actual.interval.startTime.seconds).isEqualTo(300L)
      assertThat(storage.stampedKeys).doesNotContain(BLOB_KEY)
      assertThat(actual.state).isEqualTo(ImpressionMetadata.State.DELETED)
    }
  }

  @Test
  fun `sync cleans up a file deleted before its metadata is created`() = runBlocking {
    coroutineScope {
      acquireLease()
      val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
      val storage = TestMetadataStorage(FileSystemStorageClient(temporaryFolder.newFolder()))
      seedReplacement(
        storage,
        impressionMetadata {
          eventGroupReferenceId = "event-group-1"
          modelLine = "modelProviders/provider-1/modelSuites/suite-1/modelLines/line-1"
        },
      )
      val createStarted = CompletableDeferred<Unit>()
      val finishCreate = CompletableDeferred<Unit>()
      beforeBatchCreate = {
        createStarted.complete(Unit)
        finishCreate.await()
      }
      val sync = async { newSync(storage).sync("gs://bucket/path/to/done", LEASE_NAME, 1L) {} }
      createStarted.await()
      storage.getBlob(BLOB_KEY)!!.delete()
      try {
        val result =
          DataAvailabilityCleanup(stub, DATA_PROVIDER_NAME, storage, throttler = directThrottler)
            .cleanup(BLOB_URI, null)
        assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SKIPPED)
      } finally {
        finishCreate.complete(Unit)
      }
      sync.await()
      val actual =
        stub
          .listImpressionMetadata(
            listImpressionMetadataRequest {
              parent = DATA_PROVIDER_NAME
              showDeleted = true
            }
          )
          .impressionMetadataList
          .single()
      assertThat(actual.state).isEqualTo(ImpressionMetadata.State.DELETED)
      assertThat(storage.stampedKeys).doesNotContain(BLOB_KEY)
    }
  }

  private suspend fun seedReplacement(storage: StorageClient, original: ImpressionMetadata) {
    storage.writeBlob(
      BLOB_KEY,
      blobDetails {
          blobUri = "gs://bucket/path/to/impressions"
          eventGroupReferenceId = original.eventGroupReferenceId
          modelLine = original.modelLine
          interval = interval {
            startTime = timestamp { seconds = 300 }
            endTime = timestamp { seconds = 400 }
          }
        }
        .toByteString(),
    )
    storage.writeBlob(
      "path/to/impressions",
      com.google.protobuf.ByteString.copyFromUtf8("impressions"),
    )
    storage.writeBlob("path/to/done", com.google.protobuf.ByteString.EMPTY)
  }

  private fun newSync(storage: BlobMetadataStorageClient) =
    DataAvailabilitySync(
      edpImpressionPath = "path",
      storageClient = storage,
      dataProvidersStub = DataProvidersGrpcKt.DataProvidersCoroutineStub(publicServer.channel),
      impressionMetadataServiceStub = ImpressionMetadataServiceCoroutineStub(publicServer.channel),
      dataProviderName = DATA_PROVIDER_NAME,
      throttler =
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T = block()
        },
      impressionMetadataBatchSize = 10,
      modelLineMap = emptyMap(),
      errorIfGapsExist = false,
      publishEventGroupActivities = null,
    )

  private class TestMetadataStorage(delegate: FileSystemStorageClient) :
    BlobMetadataStorageClient, StorageClient by delegate {
    val stampedKeys = mutableListOf<String>()

    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: Instant?,
      metadata: Map<String, String>,
    ) {
      stampedKeys += blobKey
    }
  }

  @Test
  fun `cleanup preserves a synchronized replacement for an unversioned event with a resource ID`() =
    runBlocking {
      assertReplacementPreserved(resourceIdProvided = true, replacementRemainsLive = true)
    }

  @Test
  fun `cleanup preserves a synchronized replacement for an unversioned event without a resource ID`() =
    runBlocking {
      assertReplacementPreserved(resourceIdProvided = false, replacementRemainsLive = true)
    }

  @Test
  fun `cleanup accepts an unversioned queued deletion event with a resource ID`() = runBlocking {
    assertQueuedDeletionProcessed(resourceIdProvided = true)
  }

  @Test
  fun `cleanup accepts an unversioned queued deletion event without a resource ID`() = runBlocking {
    assertQueuedDeletionProcessed(resourceIdProvided = false)
  }

  @Test
  fun `cleanup rechecks a replacement deleted again with a resource ID`() = runBlocking {
    assertReplacementPreserved(resourceIdProvided = true, replacementRemainsLive = false)
  }

  @Test
  fun `cleanup rechecks a replacement deleted again without a resource ID`() = runBlocking {
    assertReplacementPreserved(resourceIdProvided = false, replacementRemainsLive = false)
  }

  @Test
  fun `cleanup retries a queued deletion after the internal API upgrades with a resource ID`() =
    runBlocking {
      assertPartialUpgradeRetried(resourceIdProvided = true, publicApiLegacy = false)
    }

  @Test
  fun `cleanup retries a queued deletion after the internal API upgrades without a resource ID`() =
    runBlocking {
      assertPartialUpgradeRetried(resourceIdProvided = false, publicApiLegacy = false)
    }

  @Test
  fun `cleanup retries when deletion reaches a legacy public replica with a resource ID`() =
    runBlocking {
      assertPartialUpgradeRetried(resourceIdProvided = true, publicApiLegacy = true)
    }

  @Test
  fun `cleanup retries when deletion reaches a legacy public replica without a resource ID`() =
    runBlocking {
      assertPartialUpgradeRetried(resourceIdProvided = false, publicApiLegacy = true)
    }

  @Test
  fun `legacy delete requests remain accepted while the internal API upgrades`() = runBlocking {
    val metadata = createMetadata(generation = 0L)
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
    legacyInternalApi = true

    stub.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = metadata.name })

    assertThat(legacyDeleteCalls).isEqualTo(1)
    assertThat(
        stub.getImpressionMetadata(getImpressionMetadataRequest { name = metadata.name }).state
      )
      .isEqualTo(ImpressionMetadata.State.DELETED)
  }

  @Test
  fun `conditional internal deletion rejects a missing etag`() = runBlocking {
    val metadata = createMetadata(generation = 0L)
    val key = checkNotNull(ImpressionMetadataKey.fromName(metadata.name))
    val internalStub = InternalImpressionMetadataServiceCoroutineStub(internalServer.channel)

    val exception =
      assertFailsWith<StatusException> {
        internalStub.markImpressionMetadataDeleted(
          internalMarkImpressionMetadataDeletedRequest {
            dataProviderResourceId = key.dataProviderId
            impressionMetadataResourceId = key.impressionMetadataId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
    assertThat(
        stub.getImpressionMetadata(getImpressionMetadataRequest { name = metadata.name }).state
      )
      .isEqualTo(ImpressionMetadata.State.ACTIVE)
  }

  @Test
  fun `conditional public deletion rejects a missing etag`() = runBlocking {
    val metadata = createMetadata(generation = 0L)
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)

    val exception =
      assertFailsWith<StatusException> {
        stub.markImpressionMetadataDeleted(
          markImpressionMetadataDeletedRequest { name = metadata.name }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(
        stub.getImpressionMetadata(getImpressionMetadataRequest { name = metadata.name }).state
      )
      .isEqualTo(ImpressionMetadata.State.ACTIVE)
  }

  private suspend fun assertPartialUpgradeRetried(
    resourceIdProvided: Boolean,
    publicApiLegacy: Boolean,
  ) {
    val metadata = createMetadata(generation = 0L)
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
    val cleanup =
      DataAvailabilityCleanup(
        stub,
        DATA_PROVIDER_NAME,
        FileSystemStorageClient(temporaryFolder.newFolder()),
        throttler = directThrottler,
      )
    val resourceId = if (resourceIdProvided) metadata.name else null
    legacyInternalApi = !publicApiLegacy
    legacyPublicApi = publicApiLegacy

    val exception = assertFailsWith<StatusException> { cleanup.cleanup(BLOB_URI, resourceId) }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    assertThat(exception.status.description)
      .isEqualTo(
        if (publicApiLegacy) "Upgrade the EDPA public API server before retrying cleanup."
        else "Upgrade the EDPA internal metadata API server before retrying cleanup."
      )

    assertThat(legacyDeleteCalls).isEqualTo(0)
    assertThat(legacyPublicDeleteCalls).isEqualTo(0)
    assertThat(
        stub.getImpressionMetadata(getImpressionMetadataRequest { name = metadata.name }).state
      )
      .isEqualTo(ImpressionMetadata.State.ACTIVE)
    legacyInternalApi = false
    legacyPublicApi = false

    val result = cleanup.cleanup(BLOB_URI, resourceId)

    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)
    assertThat(
        stub.getImpressionMetadata(getImpressionMetadataRequest { name = metadata.name }).state
      )
      .isEqualTo(ImpressionMetadata.State.DELETED)
  }

  private suspend fun createMetadata(generation: Long): ImpressionMetadata {
    acquireLease()
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
    return stub.createImpressionMetadata(
      createImpressionMetadataRequest {
        parent = DATA_PROVIDER_NAME
        dataAvailabilitySyncLease = LEASE_NAME
        impressionMetadata = impressionMetadata {
          blobUri = BLOB_URI
          blobTypeUrl = "type.googleapis.com/example.Impression"
          eventGroupReferenceId = "event-group-1"
          modelLine = "modelProviders/provider-1/modelSuites/suite-1/modelLines/line-1"
          interval = interval {
            startTime = timestamp { seconds = 1 }
            endTime = timestamp { seconds = 9 }
          }
          metadataBlobGeneration = generation
        }
      }
    )
  }

  private suspend fun acquireLease() {
    SpannerDataAvailabilitySyncLeaseService(database.databaseClient)
      .acquireDataAvailabilitySyncLease(
        acquireDataAvailabilitySyncLeaseRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          synchronizationAttemptId = ATTEMPT_ID
          requestId = LEASE_REQUEST_ID
        }
      )
  }

  private suspend fun assertQueuedDeletionProcessed(resourceIdProvided: Boolean) {
    val metadata = createMetadata(generation = 0L)
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
    val storage = FileSystemStorageClient(temporaryFolder.newFolder())
    val cleanup =
      DataAvailabilityCleanup(stub, DATA_PROVIDER_NAME, storage, throttler = directThrottler)

    // The queued event has only its original path and optional resource ID.
    val result = cleanup.cleanup(BLOB_URI, if (resourceIdProvided) metadata.name else null)
    val actual = stub.getImpressionMetadata(getImpressionMetadataRequest { name = metadata.name })

    assertThat(metadata.metadataBlobGeneration).isEqualTo(0L)
    assertThat(metadata.etag).isNotEmpty()
    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)
    assertThat(actual.state).isEqualTo(ImpressionMetadata.State.DELETED)
    assertThat(actual.etag).isNotEqualTo(metadata.etag)
  }

  private suspend fun assertReplacementPreserved(
    resourceIdProvided: Boolean,
    replacementRemainsLive: Boolean,
  ) = coroutineScope {
    val original = createMetadata(generation = 41L)
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
    val root = temporaryFolder.newFolder()
    val file = File(root, BLOB_KEY)
    file.parentFile.mkdirs()
    file.writeBytes(byteArrayOf(1))
    assertThat(file.delete()).isTrue()
    val storage = FileSystemStorageClient(root)
    val absenceObserved = CompletableDeferred<Unit>()
    val replacementSynchronized = CompletableDeferred<Unit>()
    val pausedStorage =
      object : StorageClient by storage {
        override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
          val blob = storage.getBlob(blobKey)
          absenceObserved.complete(Unit)
          replacementSynchronized.await()
          return blob
        }
      }
    val cleanup =
      DataAvailabilityCleanup(stub, DATA_PROVIDER_NAME, pausedStorage, throttler = directThrottler)
    val pendingCleanup = async {
      cleanup.cleanup(BLOB_URI, if (resourceIdProvided) original.name else null)
    }
    absenceObserved.await()
    try {
      file.writeBytes(byteArrayOf(2))
      stub.updateImpressionMetadata(
        updateImpressionMetadataRequest {
          dataAvailabilitySyncLease = LEASE_NAME
          impressionMetadata = original.copy { metadataBlobGeneration = 42L }
        }
      )
      if (!replacementRemainsLive) {
        assertThat(file.delete()).isTrue()
      }
    } finally {
      replacementSynchronized.complete(Unit)
    }
    val result = pendingCleanup.await()
    val actual = stub.getImpressionMetadata(getImpressionMetadataRequest { name = original.name })

    assertThat(actual.metadataBlobGeneration).isEqualTo(42L)
    if (replacementRemainsLive) {
      assertThat(storage.getBlob(BLOB_KEY)).isNotNull()
      assertThat(actual.state).isEqualTo(ImpressionMetadata.State.ACTIVE)
      assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SKIPPED)
    } else {
      assertThat(storage.getBlob(BLOB_KEY)).isNull()
      assertThat(actual.state).isEqualTo(ImpressionMetadata.State.DELETED)
      assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)
    }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
    private const val DATA_PROVIDER_ID = "data-provider-1"
    private const val DATA_PROVIDER_NAME = "dataProviders/$DATA_PROVIDER_ID"
    private const val ATTEMPT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val LEASE_REQUEST_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    private const val LEASE_NAME = "$DATA_PROVIDER_NAME/dataAvailabilitySyncLeases/$ATTEMPT_ID"
    private const val BLOB_KEY = "path/to/metadata.binpb"
    private const val BLOB_URI = "gs://bucket/$BLOB_KEY"
  }
}
