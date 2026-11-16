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
import java.io.File
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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanup
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerDataAvailabilitySyncLeaseService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerImpressionMetadataService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.updateImpressionMetadataRequest
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub as InternalImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class DataAvailabilityCleanupRaceTest {
  private val database =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)
  private val internalServer = GrpcTestServerRule {
    addService(SpannerImpressionMetadataService(database.databaseClient))
  }
  private val publicServer = GrpcTestServerRule {
    addService(
      ImpressionMetadataService(
        InternalImpressionMetadataServiceCoroutineStub(internalServer.channel)
      )
    )
  }

  @get:Rule
  val serverRules: TestRule = chainRulesSequentially(database, internalServer, publicServer)

  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `cleanup preserves a synchronized replacement for an unversioned event with a resource ID`() =
    runBlocking {
      assertReplacementPreserved(resourceIdProvided = true)
    }

  @Test
  fun `cleanup preserves a synchronized replacement for an unversioned event without a resource ID`() =
    runBlocking {
      assertReplacementPreserved(resourceIdProvided = false)
    }

  @Test
  fun `cleanup accepts an unversioned queued deletion event with a resource ID`() = runBlocking {
    assertQueuedDeletionProcessed(resourceIdProvided = true)
  }

  @Test
  fun `cleanup accepts an unversioned queued deletion event without a resource ID`() = runBlocking {
    assertQueuedDeletionProcessed(resourceIdProvided = false)
  }

  private suspend fun createMetadata(generation: Long): ImpressionMetadata {
    SpannerDataAvailabilitySyncLeaseService(database.databaseClient)
      .acquireDataAvailabilitySyncLease(
        acquireDataAvailabilitySyncLeaseRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          synchronizationAttemptId = ATTEMPT_ID
          requestId = LEASE_REQUEST_ID
        }
      )
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

  private suspend fun assertQueuedDeletionProcessed(resourceIdProvided: Boolean) {
    val metadata = createMetadata(generation = 0L)
    val stub = ImpressionMetadataServiceCoroutineStub(publicServer.channel)
    val storage = FileSystemStorageClient(temporaryFolder.newFolder())
    val cleanup = DataAvailabilityCleanup(stub, DATA_PROVIDER_NAME, storage)

    // The queued event has only its original path and optional resource ID.
    val result = cleanup.cleanup(BLOB_URI, if (resourceIdProvided) metadata.name else null)
    val actual = stub.getImpressionMetadata(getImpressionMetadataRequest { name = metadata.name })

    assertThat(metadata.metadataBlobGeneration).isEqualTo(0L)
    assertThat(metadata.etag).isNotEmpty()
    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SUCCESS)
    assertThat(actual.state).isEqualTo(ImpressionMetadata.State.DELETED)
    assertThat(actual.etag).isNotEqualTo(metadata.etag)
  }

  private suspend fun assertReplacementPreserved(resourceIdProvided: Boolean) = coroutineScope {
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
    val cleanup = DataAvailabilityCleanup(stub, DATA_PROVIDER_NAME, pausedStorage)
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
    } finally {
      replacementSynchronized.complete(Unit)
    }
    val result = pendingCleanup.await()
    val actual = stub.getImpressionMetadata(getImpressionMetadataRequest { name = original.name })

    assertThat(storage.getBlob(BLOB_KEY)).isNotNull()
    assertThat(actual.state).isEqualTo(ImpressionMetadata.State.ACTIVE)
    assertThat(actual.metadataBlobGeneration).isEqualTo(42L)
    assertThat(result.status).isEqualTo(DataAvailabilityCleanup.CleanupStatus.SKIPPED)
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
