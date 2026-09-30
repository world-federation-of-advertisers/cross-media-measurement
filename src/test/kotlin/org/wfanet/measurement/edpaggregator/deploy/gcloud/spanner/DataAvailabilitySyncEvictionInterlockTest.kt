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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.io.File
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.hours
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.acquireRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.internal.edpaggregator.advanceRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.internal.edpaggregator.releaseDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.renewDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.validateDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class DataAvailabilitySyncEvictionInterlockTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  @get:Rule val tempFolder = TemporaryFolder()

  private var createCount = 0

  private val dataProvidersService =
    object : DataProvidersCoroutineImplBase() {
      override suspend fun replaceDataAvailabilityIntervals(
        request: ReplaceDataAvailabilityIntervalsRequest
      ): DataProvider = DataProvider.getDefaultInstance()
    }

  private val impressionMetadataService =
    object : ImpressionMetadataServiceCoroutineImplBase() {
      override suspend fun listImpressionMetadata(request: ListImpressionMetadataRequest) =
        listImpressionMetadataResponse {}

      override suspend fun batchCreateImpressionMetadata(
        request: BatchCreateImpressionMetadataRequest
      ) = batchCreateImpressionMetadataResponse {
        createCount++
        impressionMetadata += request.requestsList.map { it.impressionMetadata }
      }

      override suspend fun computeModelLineBounds(request: ComputeModelLineBoundsRequest) =
        computeModelLineBoundsResponse {
          modelLineBounds += modelLineBoundMapEntry {
            key = MODEL_LINE
            value = interval {
              startTime = timestamp { seconds = 1L }
              endTime = timestamp { seconds = 2L }
            }
          }
        }
    }

  @get:Rule
  val grpcServer = GrpcTestServerRule {
    addService(dataProvidersService)
    addService(impressionMetadataService)
  }

  @Test
  fun `sync that started before fence drains before eviction`(): Unit = runBlocking {
    val clock = MutableClock(Instant.parse("2026-09-30T00:00:00Z"))
    val leaseService =
      SpannerDataAvailabilitySyncLeaseService(
        spannerDatabase.databaseClient,
        clock = clock,
        leaseDuration = Duration.ofMinutes(10),
      )
    val uploadService =
      SpannerRawImpressionUploadService(spannerDatabase.databaseClient, clock = clock)
    val readStarted = CompletableDeferred<Unit>()
    val finishRead = CompletableDeferred<Unit>()
    val fileStorageClient = FileSystemStorageClient(File(tempFolder.root.toString()))
    fileStorageClient.writeBlob("date/impressions", emptyFlow())
    fileStorageClient.writeBlob(
      "date/metadata.binpb",
      flowOf(
        blobDetails {
            blobUri = "gs://bucket/date/impressions"
            eventGroupReferenceId = "event-group"
            modelLine = MODEL_LINE
            interval = interval {
              startTime = timestamp { seconds = 1L }
              endTime = timestamp { seconds = 2L }
            }
          }
          .toByteString()
      ),
    )
    val storageClient = PausingBlobMetadataStorageClient(fileStorageClient, readStarted, finishRead)
    val sync =
      DataAvailabilitySync(
        edpImpressionPath = "",
        storageClient = storageClient,
        dataProvidersStub = DataProvidersCoroutineStub(grpcServer.channel),
        impressionMetadataServiceStub = ImpressionMetadataServiceCoroutineStub(grpcServer.channel),
        dataProviderName = DATA_PROVIDER_NAME,
        throttler = DirectThrottler,
        impressionMetadataBatchSize = 1,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
      )
    val ids = REQUEST_IDS.iterator()
    val leaseClient = InternalLeaseClient(leaseService)
    val runner =
      DataAvailabilitySyncLeaseRunner(
        leaseClient,
        renewalInterval = 1.hours,
        uuidGenerator = ids::next,
      )

    supervisorScope {
      val syncResult = async {
        runner.run(DATA_PROVIDER_NAME) { ensureLeaseActive ->
          sync.sync("gs://bucket/date/done", ensureLeaseActive)
        }
      }
      readStarted.await()
      uploadService.acquireRawImpressionUploadEvictionFence(
        acquireRawImpressionUploadEvictionFenceRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          evictionOperationId = EVICTION_OPERATION_ID
          state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
        }
      )
      uploadService.advanceRawImpressionUploadEvictionFence(
        advanceRawImpressionUploadEvictionFenceRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          evictionOperationId = EVICTION_OPERATION_ID
          state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING
        }
      )

      val blocked =
        assertFailsWith<StatusRuntimeException> {
          uploadService.advanceRawImpressionUploadEvictionFence(
            advanceRawImpressionUploadEvictionFenceRequest {
              dataProviderResourceId = DATA_PROVIDER_ID
              evictionOperationId = EVICTION_OPERATION_ID
              state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
            }
          )
        }
      assertThat(blocked.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)

      clock.advance(Duration.ofMinutes(11))
      uploadService.advanceRawImpressionUploadEvictionFence(
        advanceRawImpressionUploadEvictionFenceRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          evictionOperationId = EVICTION_OPERATION_ID
          state = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
        }
      )
      finishRead.complete(Unit)
      val syncError = assertFailsWith<StatusRuntimeException> { syncResult.await() }
      assertThat(syncError.status.code).isEqualTo(Status.Code.UNAVAILABLE)
      assertThat(leaseClient.validationCount).isGreaterThan(1)
      assertThat(createCount).isEqualTo(0)
    }
  }

  private class PausingBlobMetadataStorageClient(
    private val delegate: StorageClient,
    private val readStarted: CompletableDeferred<Unit>,
    private val finishRead: CompletableDeferred<Unit>,
  ) : BlobMetadataStorageClient, StorageClient by delegate {
    override suspend fun listBlobs(prefix: String?) = flow {
      readStarted.complete(Unit)
      finishRead.await()
      delegate.listBlobs(prefix).collect(::emit)
    }

    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: java.time.Instant?,
      metadata: Map<String, String>,
    ) {}
  }

  private class InternalLeaseClient(private val service: SpannerDataAvailabilitySyncLeaseService) :
    DataAvailabilitySyncLeaseClient {
    var validationCount = 0
      private set

    override suspend fun acquire(
      parent: String,
      attemptId: String,
      requestId: String,
    ): DataAvailabilitySyncLease =
      service
        .acquireDataAvailabilitySyncLease(
          acquireDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = attemptId
            this.requestId = requestId
          }
        )
        .toPublic()

    override suspend fun renew(
      lease: DataAvailabilitySyncLease,
      requestId: String,
    ): DataAvailabilitySyncLease =
      service
        .renewDataAvailabilitySyncLease(
          renewDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = lease.name.substringAfterLast('/')
            etag = lease.etag
            this.requestId = requestId
          }
        )
        .toPublic()

    override suspend fun validate(lease: DataAvailabilitySyncLease): DataAvailabilitySyncLease {
      validationCount++
      return service
        .validateDataAvailabilitySyncLease(
          validateDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = lease.name.substringAfterLast('/')
            etag = lease.etag
          }
        )
        .toPublic()
    }

    override suspend fun release(
      lease: DataAvailabilitySyncLease,
      requestId: String,
    ): DataAvailabilitySyncLease =
      service
        .releaseDataAvailabilitySyncLease(
          releaseDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = lease.name.substringAfterLast('/')
            etag = lease.etag
            this.requestId = requestId
          }
        )
        .toPublic()

    private fun org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLease.toPublic() =
      dataAvailabilitySyncLease {
        name = "$DATA_PROVIDER_NAME/dataAvailabilitySyncLeases/$synchronizationAttemptId"
        state =
          when (this@toPublic.state) {
            org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
              .DATA_AVAILABILITY_SYNC_LEASE_STATE_ACTIVE -> DataAvailabilitySyncLease.State.ACTIVE
            org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
              .DATA_AVAILABILITY_SYNC_LEASE_STATE_RELEASED ->
              DataAvailabilitySyncLease.State.RELEASED
            org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
              .DATA_AVAILABILITY_SYNC_LEASE_STATE_EXPIRED -> DataAvailabilitySyncLease.State.EXPIRED
            org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
              .DATA_AVAILABILITY_SYNC_LEASE_STATE_UNSPECIFIED,
            org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState
              .UNRECOGNIZED -> DataAvailabilitySyncLease.State.STATE_UNSPECIFIED
          }
        etag = this@toPublic.etag
      }
  }

  private object DirectThrottler : Throttler {
    override suspend fun <T> onReady(block: suspend () -> T): T = block()
  }

  private class MutableClock(private var current: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
      current = current.plus(duration)
    }
  }

  companion object {
    @JvmField @ClassRule val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "data-provider"
    private const val DATA_PROVIDER_NAME = "dataProviders/$DATA_PROVIDER_ID"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val EVICTION_OPERATION_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    private val REQUEST_IDS =
      listOf(
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
        "44444444-4444-4444-8444-444444444444",
        "55555555-5555-4555-8555-555555555555",
        "66666666-6666-4666-8666-666666666666",
        "77777777-7777-4777-8777-777777777777",
        "88888888-8888-4888-8888-888888888888",
        "99999999-9999-4999-8999-999999999999",
      )
  }
}
