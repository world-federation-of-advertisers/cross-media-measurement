// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.File
import java.io.StringReader
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.gcsStorage
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.controlPlaneQueueSink
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanup
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.GetImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkImpressionMetadataDeletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilityCleanupParams
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.EnsureWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RenewWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/** Exercises storage-event intake and the cleanup HTTP worker against in-process APIs. */
class DataAvailabilityCleanupFunctionTest {
  private val items = ConcurrentHashMap<String, WorkItem>()
  private val attempts = ConcurrentHashMap<String, WorkItemAttempt>()
  private val records = ConcurrentHashMap<String, ImpressionMetadata>()
  private val marks = AtomicInteger()
  private val lists = AtomicInteger()
  private val gets = AtomicInteger()
  private val renewed = CompletableDeferred<Unit>()
  private val failuresRemaining = AtomicInteger()
  private val completionFailuresRemaining = AtomicInteger()
  private val lastEtag = ConcurrentHashMap<String, String>()
  private var legacyApi = false
  private var leasedAttempts = true
  private var loseCompletionResponse = false
  private var legacyEtag = false
  private var intakeFailure: Status? = null
  private var legacyEnsure = false
  private val directThrottler =
    object : Throttler {
      override suspend fun <T> onReady(block: suspend () -> T): T = block()
    }

  @get:Rule val temp = TemporaryFolder()
  private val itemService =
    object : WorkItemsGrpcKt.WorkItemsCoroutineImplBase() {
      override suspend fun ensureWorkItem(request: EnsureWorkItemRequest): WorkItem {
        intakeFailure?.let { throw it.asException() }
        if (legacyEnsure) throw Status.UNIMPLEMENTED.asException()
        return items.compute("workItems/" + request.workItemId) { name, existing ->
          if (existing == null)
            request.workItem.copy {
              this.name = name
              generation = 1
              state = WorkItem.State.QUEUED
            }
          else {
            val stored =
              existing.workItemParams.unpack(WorkItem.WorkItemParams::class.java).copy {
                traceContext.clear()
              }
            val requested =
              request.workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java).copy {
                traceContext.clear()
              }
            if (requested != stored) throw Status.FAILED_PRECONDITION.asException()
            assertThat(request.workItem.serializationKey).isEqualTo(existing.serializationKey)
            existing
          }
        }!!
      }

      override suspend fun createWorkItem(
        request: org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
      ): WorkItem {
        val item =
          request.workItem.copy {
            name = "workItems/" + request.workItemId
            generation = 1
            state = WorkItem.State.QUEUED
          }
        if (items.putIfAbsent(item.name, item) != null) throw Status.ALREADY_EXISTS.asException()
        return item
      }

      override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem =
        items.getValue(request.name)

      override suspend fun failWorkItem(request: FailWorkItemRequest): WorkItem =
        items.compute(request.name) { _, current ->
          requireNotNull(current).copy { state = WorkItem.State.FAILED }
        }!!
    }
  private val attemptService =
    object : WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase() {
      override suspend fun createWorkItemAttempt(
        request: CreateWorkItemAttemptRequest
      ): WorkItemAttempt {
        items.compute(request.parent) { _, current ->
          val item = requireNotNull(current)
          if (request.expectedWorkItemGeneration != item.generation)
            throw failure("WORK_ITEM_GENERATION_MISMATCH", item.state.name)
          if (item.state != WorkItem.State.QUEUED)
            throw failure("INVALID_WORK_ITEM_STATE", item.state.name)
          item.copy { state = WorkItem.State.RUNNING }
        }
        assertThat(request.supportsAttemptLease).isTrue()
        return workItemAttempt {
            name = request.parent + "/workItemAttempts/" + request.workItemAttemptId
            state = WorkItemAttempt.State.ACTIVE
            if (leasedAttempts) leaseExpirationTime = timestamp { seconds = 2_000_000_000 }
          }
          .also { attempts[it.name] = it }
      }

      override suspend fun completeWorkItemAttempt(
        request: CompleteWorkItemAttemptRequest
      ): WorkItemAttempt {
        if (completionFailuresRemaining.getAndUpdate { maxOf(0, it - 1) } > 0)
          throw Status.UNAVAILABLE.asException()
        if (attempts.getValue(request.name).state == WorkItemAttempt.State.SUCCEEDED) {
          throw StatusProto.toStatusException(
            com.google.rpc.status {
              code = Status.Code.FAILED_PRECONDITION.value()
              details +=
                errorInfo {
                    reason = "INVALID_WORK_ITEM_ATTEMPT_STATE"
                    metadata["workItemAttemptState"] = "SUCCEEDED"
                  }
                  .pack()
            }
          )
        }
        val result =
          attempts.getValue(request.name).copy { state = WorkItemAttempt.State.SUCCEEDED }
        attempts[request.name] = result
        items.compute(request.name.substringBefore("/workItemAttempts/")) { _, item ->
          requireNotNull(item).copy { state = WorkItem.State.SUCCEEDED }
        }
        if (loseCompletionResponse) {
          loseCompletionResponse = false
          throw Status.UNAVAILABLE.asException()
        }
        return result
      }

      override suspend fun failWorkItemAttempt(
        request: FailWorkItemAttemptRequest
      ): WorkItemAttempt {
        val result = attempts.getValue(request.name).copy { state = WorkItemAttempt.State.FAILED }
        attempts[request.name] = result
        items.compute(request.name.substringBefore("/workItemAttempts/")) { _, item ->
          requireNotNull(item).copy { state = WorkItem.State.QUEUED }
        }
        return result
      }

      override suspend fun renewWorkItemAttempt(
        request: RenewWorkItemAttemptRequest
      ): WorkItemAttempt {
        renewed.complete(Unit)
        return attempts.getValue(request.name)
      }
    }
  private val metadataService =
    object : ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase() {
      override suspend fun getImpressionMetadata(
        request: GetImpressionMetadataRequest
      ): ImpressionMetadata {
        gets.incrementAndGet()
        val record = records[request.name] ?: throw Status.NOT_FOUND.asException()
        return if (legacyEtag) record.copy { etag = "" } else record
      }

      override suspend fun listImpressionMetadata(request: ListImpressionMetadataRequest) =
        listImpressionMetadataResponse {
          lists.incrementAndGet()
          impressionMetadata += records.values.filter { it.blobUri in request.filter.blobUrisList }
        }

      override suspend fun markImpressionMetadataDeleted(
        request: MarkImpressionMetadataDeletedRequest
      ): ImpressionMetadata {
        if (legacyApi) throw Status.UNIMPLEMENTED.asException()
        if (failuresRemaining.getAndUpdate { maxOf(0, it - 1) } > 0)
          throw Status.UNAVAILABLE.asException()
        marks.incrementAndGet()
        lastEtag[request.name] = request.etag
        return records.compute(request.name) { _, record ->
          val current = requireNotNull(record)
          assertThat(request.etag).isEqualTo(current.etag)
          current.copy { state = ImpressionMetadata.State.DELETED }
        }!!
      }
    }
  @get:Rule
  val server = GrpcTestServerRule {
    addService(itemService)
    addService(attemptService)
    addService(metadataService)
  }
  private val config = dataAvailabilitySyncConfig {
    dataProvider = PROVIDER
    dataAvailabilityStorage = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
  }
  private val root by lazy { temp.newFolder() }

  private fun function() =
    DataAvailabilityCleanupFunction(
      listOf(config),
      {
        DataAvailabilityCleanup(
          ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(server.channel),
          PROVIDER,
          FileSystemStorageClient(root),
          directThrottler,
        )
      },
      {
        Pair(
          WorkItemsGrpcKt.WorkItemsCoroutineStub(server.channel),
          WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(server.channel),
        )
      },
      { awaitCancellation() },
      {},
    )

  private fun record(index: Int): ImpressionMetadata =
    impressionMetadata {
        name = "$PROVIDER/impressionMetadata/record-$index"
        blobUri = "gs://bucket/files/$index"
        etag = "version-1"
        state = ImpressionMetadata.State.ACTIVE
      }
      .also { records[it.name] = it }

  private fun queued(record: ImpressionMetadata, includeName: Boolean): WorkItem =
    workItem {
        name = "workItems/cleanup-" + record.name.substringAfterLast("-")
        generation = 1
        queue = "data-availability-cleanup-queue"
        state = WorkItem.State.QUEUED
        workItemParams =
          workItemParams {
              appParams = dataAvailabilityCleanupParams { dataProvider = PROVIDER }.pack()
              dataPathParams = dataPathParams {
                dataPath = record.blobUri
                generation = 1
                createTime = timestamp { seconds = 1 }
                eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.DELETED
                if (includeName)
                  objectMetadata[WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY] = record.name
              }
            }
            .pack()
      }
      .also { items[it.name] = it }

  private fun invoke(
    worker: DataAvailabilityCleanupFunction,
    bytes: ByteArray,
    headers: Map<String, String>,
  ): Int {
    val request =
      mock<HttpRequest> {
        on { inputStream } doReturn ByteArrayInputStream(bytes)
        on { reader } doReturn BufferedReader(StringReader(bytes.decodeToString()))
      }
    whenever(request.getFirstHeader(any())).thenAnswer {
      Optional.ofNullable(headers[it.getArgument<String>(0)])
    }
    val response = mock<HttpResponse>()
    var code = 0
    doAnswer {
        code = it.getArgument(0)
        null
      }
      .whenever(response)
      .setStatusCode(any())
    worker.service(request, response)
    return code
  }

  private fun deliver(item: WorkItem): Int = invoke(function(), item.toByteArray(), emptyMap())

  @Test
  fun `service completes cleanup using the queued metadata name`() {
    val record = record(1)
    val item = queued(record, true)
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(records.getValue(record.name).state).isEqualTo(ImpressionMetadata.State.DELETED)
    assertThat(lists.get()).isEqualTo(0)
    assertThat(lastEtag[record.name]).isEqualTo("version-1")
  }

  @Test
  fun `service looks up metadata when an old event has no resource name`() {
    val item = queued(record(1), false)
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(lists.get()).isEqualTo(1)
    assertThat(marks.get()).isEqualTo(1)
  }

  @Test
  fun `service keeps legacy HTTP deliveries without a generation working`() {
    val record = record(1)
    val headers =
      mapOf(
        "X-DataWatcher-Path" to record.blobUri,
        "X-Impression-Metadata-Resource-Id" to record.name,
      )
    assertThat(invoke(function(), config.toJson().toByteArray(), headers)).isEqualTo(200)
    assertThat(marks.get()).isEqualTo(1)
    assertThat(attempts).isEmpty()
  }

  @Test
  fun `service reads the metadata version when the delayed job executes`() {
    val record = record(1)
    val item = queued(record, true)
    records[record.name] = record.copy { etag = "replacement-version" }
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(lastEtag[record.name]).isEqualTo("replacement-version")
  }

  @Test
  fun `service preserves a replacement file uploaded after enqueueing`() {
    val record = record(1)
    val item = queued(record, true)
    File(root, "files/1").apply {
      parentFile.mkdirs()
      writeText("replacement")
    }
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(marks.get()).isEqualTo(0)
    assertThat(records.getValue(record.name).state).isEqualTo(ImpressionMetadata.State.ACTIVE)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `service acknowledges successful duplicate deliveries without repeating cleanup`() {
    val item = queued(record(1), true)
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(marks.get()).isEqualTo(1)
    assertThat(attempts).hasSize(1)
  }

  @Test
  fun `service rejects an active duplicate so delivery can retry`() {
    val item = queued(record(1), true)
    items[item.name] = item.copy { state = WorkItem.State.RUNNING }
    assertThat(deliver(item)).isEqualTo(500)
    assertThat(marks.get()).isEqualTo(0)
  }

  @Test
  fun `service acknowledges an obsolete execution generation`() {
    val item = queued(record(1), true)
    items[item.name] = item.copy { generation = 2 }
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(marks.get()).isEqualTo(0)
  }

  @Test
  fun `service retries temporary metadata failures after the worker restarts`() {
    val item = queued(record(1), true)
    failuresRemaining.set(1)
    assertThat(deliver(item)).isEqualTo(500)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(attempts.values.count { it.state == WorkItemAttempt.State.FAILED }).isEqualTo(1)
    assertThat(marks.get()).isEqualTo(1)
  }

  @Test
  fun `service retries when an older public API lacks conditional deletion`() {
    val item = queued(record(1), true)
    legacyApi = true
    assertThat(deliver(item)).isEqualTo(500)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.QUEUED)
    legacyApi = false
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(marks.get()).isEqualTo(1)
  }

  @Test
  fun `service retries when an older API omits the metadata version`() {
    val item = queued(record(1), true)
    legacyEtag = true
    assertThat(deliver(item)).isEqualTo(500)
    assertThat(marks.get()).isEqualTo(0)
    legacyEtag = false
    assertThat(deliver(item)).isEqualTo(200)
  }

  @Test
  fun `service accepts an older publisher's internal WorkItem envelope`() {
    val item = queued(record(1), true)
    val internal =
      org.wfanet.measurement.internal.securecomputation.controlplane.workItem {
        workItemResourceId = item.name.substringAfter("/")
        queueResourceId = item.queue
        generation = item.generation
        workItemParams = item.workItemParams
      }
    assertThat(invoke(function(), internal.toByteArray(), emptyMap())).isEqualTo(200)
    assertThat(lists.get()).isEqualTo(0)
    assertThat(marks.get()).isEqualTo(1)
  }

  @Test
  fun `service retries completion without repeating metadata deletion`() {
    val item = queued(record(1), true)
    completionFailuresRemaining.set(1)
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(marks.get()).isEqualTo(1)
  }

  @Test
  fun `service handles a lost completion response without failing the completed job`() {
    val item = queued(record(1), true)
    loseCompletionResponse = true
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(attempts.values.single().state).isEqualTo(WorkItemAttempt.State.SUCCEEDED)
    assertThat(marks.get()).isEqualTo(1)
  }

  @Test
  fun `service accepts older attempt responses without a lease`() {
    val item = queued(record(1), true)
    leasedAttempts = false
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(renewed.isCompleted).isFalse()
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `service records a finalized event as a terminal failure`() {
    val original = queued(record(1), true)
    val params =
      original.workItemParams.unpack(WorkItem.WorkItemParams::class.java).copy {
        dataPathParams =
          dataPathParams.copy {
            eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
          }
      }
    val item = original.copy { workItemParams = params.pack() }
    items[item.name] = item
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.FAILED)
    assertThat(marks.get()).isEqualTo(0)
  }

  @Test
  fun `service rejects cleanup against another storage bucket`() {
    val record = record(1).copy { blobUri = "gs://another-bucket/files/1" }
    val item = queued(record, true)
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.FAILED)
    assertThat(marks.get()).isEqualTo(0)
  }

  @Test
  fun `service renews the attempt while storage checking waits`() = runBlocking {
    val record = record(1)
    val item = queued(record, true)
    val entered = CompletableDeferred<Unit>()
    val release = CompletableDeferred<Unit>()
    val renewal = Channel<Unit>(Channel.UNLIMITED)
    val storage =
      object : StorageClient by FileSystemStorageClient(root) {
        override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
          entered.complete(Unit)
          release.await()
          return null
        }
      }
    val worker =
      DataAvailabilityCleanupFunction(
        listOf(config),
        {
          DataAvailabilityCleanup(
            ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(server.channel),
            PROVIDER,
            storage,
            directThrottler,
          )
        },
        {
          Pair(
            WorkItemsGrpcKt.WorkItemsCoroutineStub(server.channel),
            WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(server.channel),
          )
        },
        {
          renewal.receive()
          Unit
        },
        {},
      )
    val response = async(Dispatchers.IO) { invoke(worker, item.toByteArray(), emptyMap()) }
    entered.await()
    renewal.send(Unit)
    renewed.await()
    release.complete(Unit)
    assertThat(response.await()).isEqualTo(200)
    assertThat(items.getValue(item.name).state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `a failed cleanup does not hold up other files or repeat successful deletions`() =
    runBlocking {
      val batch = (0 until 10).map { queued(record(it), true) }
      failuresRemaining.set(1)
      Executors.newFixedThreadPool(5).asCoroutineDispatcher().use { dispatcher ->
        val results = batch.map { item -> async(dispatcher) { deliver(item) } }.awaitAll()
        assertThat(results.count { it == 200 }).isEqualTo(9)
        assertThat(results.count { it == 500 }).isEqualTo(1)
      }
      val failed = items.values.single { it.state == WorkItem.State.QUEUED }
      assertThat(deliver(failed)).isEqualTo(200)
      assertThat(marks.get()).isEqualTo(10)
    }

  private fun watcher() =
    DataWatcher(
      WorkItemsGrpcKt.WorkItemsCoroutineStub(server.channel),
      listOf(
        watchedPath {
          identifier = "cleanup"
          sourcePathRegex = "^gs://bucket/.*"
          controlPlaneQueueSink = controlPlaneQueueSink {
            queue = "data-availability-cleanup-queue"
            includeObjectMetadata = true
            allowMissingSourceIdentity = true
            appParams = dataAvailabilityCleanupParams { dataProvider = PROVIDER }.pack()
          }
        }
      ),
    )

  private fun notification(record: ImpressionMetadata, generation: Long) =
    mapOf(
      DataWatcher.GENERATION_METADATA_KEY to generation.toString(),
      DataWatcher.CREATE_TIME_METADATA_KEY to "2000-01-01T00:00:00Z",
      DataWatcher.EVENT_TYPE_METADATA_KEY to DataWatcher.DELETED_EVENT_TYPE,
      WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY to record.name,
    )

  @Test
  fun `intake retries an API outage before acknowledging a deletion`() = runBlocking {
    val record = record(1)
    intakeFailure = Status.UNAVAILABLE
    val watcher = watcher()
    assertFailsWith<StatusException> {
      watcher.receivePath(record.blobUri, notification(record, 1))
    }
    assertThat(items).isEmpty()
    intakeFailure = null
    watcher.receivePath(record.blobUri, notification(record, 1))
    assertThat(deliver(items.values.single())).isEqualTo(200)
  }

  @Test
  fun `intake preserves a deletion when queue permissions need repair`() = runBlocking {
    val record = record(1)
    intakeFailure = Status.PERMISSION_DENIED
    assertFailsWith<StatusException> {
      watcher().receivePath(record.blobUri, notification(record, 1))
    }
    assertThat(items).isEmpty()
    assertThat(records.getValue(record.name).state).isEqualTo(ImpressionMetadata.State.ACTIVE)
  }

  @Test
  fun `intake deduplicates legacy events with an older control plane API`() = runBlocking {
    legacyEnsure = true
    val record = record(1)
    val event =
      mapOf(
        DataWatcher.EVENT_TYPE_METADATA_KEY to DataWatcher.DELETED_EVENT_TYPE,
        DataWatcher.EVENT_ID_METADATA_KEY to "legacy-event",
      )
    val watcher = watcher()
    repeat(2) { watcher.receivePath(record.blobUri, event) }
    assertThat(items).hasSize(1)
    assertThat(deliver(items.values.single())).isEqualTo(200)
  }

  @Test
  fun `storage notifications without object identity enqueue a repeatable legacy cleanup`() =
    runBlocking {
      val record = record(1)
      val notification =
        mapOf(
          DataWatcher.EVENT_TYPE_METADATA_KEY to DataWatcher.ARCHIVED_EVENT_TYPE,
          DataWatcher.EVENT_ID_METADATA_KEY to "legacy-notification-1",
        )
      val watcher = watcher()
      watcher.receivePath(record.blobUri, notification)
      watcher.receivePath(record.blobUri, notification)
      assertThat(items).hasSize(1)
      assertThat(deliver(items.values.single())).isEqualTo(200)
      assertThat(marks.get()).isEqualTo(1)
    }

  @Test
  fun `archive and delete notifications for the same file version share a cleanup job`() =
    runBlocking {
      val record = record(1)
      val watcher = watcher()
      watcher.receivePath(
        record.blobUri,
        notification(record, 1) +
          (DataWatcher.EVENT_TYPE_METADATA_KEY to DataWatcher.ARCHIVED_EVENT_TYPE),
      )
      watcher.receivePath(record.blobUri, notification(record, 1))
      assertThat(items).hasSize(1)
      assertThat(deliver(items.values.single())).isEqualTo(200)
      assertThat(marks.get()).isEqualTo(1)
    }

  @Test
  fun `a replacement at the same filename enqueues a distinct cleanup job`() = runBlocking {
    val record = record(1)
    val watcher = watcher()
    watcher.receivePath(record.blobUri, notification(record, 1))
    watcher.receivePath(record.blobUri, notification(record, 2))
    assertThat(items).hasSize(2)
  }

  @Test
  fun `repeated notifications retain the first custom metadata snapshot`() = runBlocking {
    val record = record(1)
    val watcher = watcher()
    watcher.receivePath(record.blobUri, notification(record, 1) + ("label" to "original"))
    watcher.receivePath(record.blobUri, notification(record, 1) + ("label" to "updated"))
    assertThat(items).hasSize(1)
    val item = items.values.single()
    val params = item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
    assertThat(params.dataPathParams.objectMetadataMap["label"]).isEqualTo("original")
    assertThat(deliver(item)).isEqualTo(200)
    assertThat(marks.get()).isEqualTo(1)
  }

  @Test
  fun `notifications with and without creation time share the generation's cleanup job`() =
    runBlocking {
      val record = record(1)
      val watcher = watcher()
      watcher.receivePath(
        record.blobUri,
        notification(record, 1) - DataWatcher.CREATE_TIME_METADATA_KEY,
      )
      watcher.receivePath(record.blobUri, notification(record, 1))
      assertThat(items).hasSize(1)
      assertThat(deliver(items.values.single())).isEqualTo(200)
    }

  @Test
  fun `a burst of ten thousand deletions persists distinct jobs and drains after duplicate deliveries`() =
    runBlocking {
      val watcher = watcher()
      repeat(10_000) { index ->
        val record = record(index)
        watcher.receivePath(record.blobUri, notification(record, 1))
        if (index % 10 == 0) watcher.receivePath(record.blobUri, notification(record, 1))
      }
      assertThat(items).hasSize(10_000)
      assertThat(items.values.all { it.serializationKey.isEmpty() }).isTrue()
      Executors.newFixedThreadPool(5).asCoroutineDispatcher().use { dispatcher ->
        items.values
          .toList()
          .map { item -> async(dispatcher) { assertThat(deliver(item)).isEqualTo(200) } }
          .awaitAll()
      }
      assertThat(items.values.count { it.state == WorkItem.State.SUCCEEDED }).isEqualTo(10_000)
      assertThat(marks.get()).isEqualTo(10_000)
      assertThat(lists.get()).isEqualTo(0)
    }

  private fun failure(reason: String, state: String) =
    StatusProto.toStatusException(
      com.google.rpc.status {
        code = Status.Code.FAILED_PRECONDITION.value()
        details +=
          errorInfo {
              this.reason = reason
              metadata["workItemState"] = state
            }
            .pack()
      }
    )

  companion object {
    init {
      EdpaTelemetry.ensureInitialized()
    }

    private const val PROVIDER = "dataProviders/AAAAAAAAAHs"
  }
}
