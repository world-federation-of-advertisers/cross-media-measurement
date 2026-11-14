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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.type.date
import io.grpc.Status
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys

class GrpcManagedDoneObjectResolverTest {
  private val rows = linkedMapOf<String, RawImpressionUploadModelLine>()
  private val uploadRows = linkedMapOf<String, RawImpressionUpload>()
  private val inputRows = linkedMapOf<String, List<RawImpressionUploadFile>>()
  private val jobRows = linkedMapOf<String, List<VidLabelingJob>>()
  private var paginate = false
  private var lookupFailure: Status? = null
  private var downloadedFiles = 0
  private var wildcardFilesOverride: List<RawImpressionUploadFile>? = null

  private val uploads: RawImpressionUploadServiceCoroutineImplBase = mockService {
    onBlocking { getRawImpressionUpload(any<GetRawImpressionUploadRequest>()) }
      .thenAnswer {
        val name = it.getArgument<GetRawImpressionUploadRequest>(0).name
        uploadRows[name] ?: throw Status.NOT_FOUND.asException()
      }
    onBlocking { listRawImpressionUploads(any<ListRawImpressionUploadsRequest>()) }
      .thenAnswer {
        val request = it.getArgument<ListRawImpressionUploadsRequest>(0)
        listRawImpressionUploadsResponse {
          val selected = uploadRows.values.toList()
          rawImpressionUploads +=
            when {
              !paginate -> selected
              request.pageToken.isEmpty() -> selected.take(1)
              else -> selected.drop(1)
            }
          if (paginate && request.pageToken.isEmpty()) nextPageToken = "next"
        }
      }
  }
  private val models: RawImpressionUploadModelLineServiceCoroutineImplBase = mockService {
    onBlocking {
        listRawImpressionUploadModelLines(any<ListRawImpressionUploadModelLinesRequest>())
      }
      .thenAnswer {
        lookupFailure?.let { failure -> throw failure.asException() }
        val request = it.getArgument<ListRawImpressionUploadModelLinesRequest>(0)
        listRawImpressionUploadModelLinesResponse {
          val selected = rows.values.toList()
          rawImpressionUploadModelLines +=
            when {
              !paginate -> selected
              request.pageToken.isEmpty() -> selected.take(1)
              else -> selected.drop(1)
            }
          if (paginate && request.pageToken.isEmpty()) nextPageToken = "next"
        }
      }
  }
  private val files: RawImpressionUploadFileServiceCoroutineImplBase = mockService {
    onBlocking { listRawImpressionUploadFiles(any<ListRawImpressionUploadFilesRequest>()) }
      .thenAnswer {
        val request = it.getArgument<ListRawImpressionUploadFilesRequest>(0)
        val selected =
          if (request.parent.endsWith("/rawImpressionUploads/-")) {
            wildcardFilesOverride
              ?: inputRows.values.flatten().filter { it.blobUri in request.filter.blobUriInList }
          } else inputRows[request.parent].orEmpty()
        val page =
          when {
            !paginate -> selected
            request.pageToken.isEmpty() -> selected.take(1)
            else -> selected.drop(1)
          }
        downloadedFiles += page.size
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += page
          if (paginate && request.pageToken.isEmpty()) nextPageToken = "next"
        }
      }
  }
  private val jobs: VidLabelingJobServiceCoroutineImplBase = mockService {
    onBlocking { listVidLabelingJobs(any<ListVidLabelingJobsRequest>()) }
      .thenAnswer {
        val request = it.getArgument<ListVidLabelingJobsRequest>(0)
        listVidLabelingJobsResponse {
          val selected = jobRows[request.parent].orEmpty()
          vidLabelingJobs +=
            when {
              !paginate -> selected
              request.pageToken.isEmpty() -> selected.take(1)
              else -> selected.drop(1)
            }
          if (paginate && request.pageToken.isEmpty()) nextPageToken = "next"
        }
      }
  }
  @get:Rule
  val server = GrpcTestServerRule {
    addService(uploads)
    addService(models)
    addService(files)
    addService(jobs)
  }

  @Test
  fun `zero-output model lookup streams paginated registered owners`() =
    runBlocking<Unit> {
      seed("unrelated", RawImpressionUploadModelLine.State.COMPLETED)
      rows["unrelated"] =
        rows.getValue("unrelated").copy { cmmsModelLine = "$MODEL_LINE-unrelated" }
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      paginate = true
      assertThat(resolver().modelLineFor(EVENT_DATE, done("owner"))).isEqualTo(MODEL_LINE)
      assertThat(downloadedFiles).isEqualTo(0)
      verifyBlocking(uploads, never()) { getRawImpressionUpload(any()) }
    }

  @Test
  fun `zero-output model lookup rejects unknown or noncanonical marker ownership`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      for (key in
        listOf(
          done("unknown"),
          done("owner").replace("/model-line/model/", "/model-line/other/"),
        )) {
        assertFailsWith<IllegalStateException> { resolver().modelLineFor(EVENT_DATE, key) }
      }
    }

  @Test
  fun `zero-output model lookup rejects ambiguous full model names sharing a directory`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      rows["ambiguous"] =
        rows.getValue("owner").copy {
          name = "${upload("owner")}/rawImpressionUploadModelLines/ambiguous"
          cmmsModelLine = MODEL_LINE.replace("modelProviders/provider/", "modelProviders/other/")
        }
      assertFailsWith<IllegalStateException> { resolver().modelLineFor(EVENT_DATE, done("owner")) }
    }

  @Test
  fun `resolve accepts durable owners under active or sibling failed parents`() =
    runBlocking<Unit> {
      for (state in
        listOf(
          RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
          RawImpressionUploadModelLine.State.COMPLETED,
        )) {
        seed("owner", state)
        uploadRows[upload("owner")] =
          uploadRows.getValue(upload("owner")).copy {
            this.state = RawImpressionUpload.State.FAILED
          }
        assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      }
    }

  @Test
  fun `resolve classifies uncommitted computation as finalizing`() =
    runBlocking<Unit> {
      for (state in
        listOf(
          RawImpressionUploadModelLine.State.CREATED,
          RawImpressionUploadModelLine.State.POOL_ASSIGNING,
          RawImpressionUploadModelLine.State.RANKING,
          RawImpressionUploadModelLine.State.LABELING,
        )) {
        seed("owner", state)
        assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
      }
    }

  @Test
  fun `resolve ignores failed and evicted retained markers`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.FAILED)
      assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
      rows["owner"] =
        rows.getValue("owner").copy {
          failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
        }
      assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
    }

  @Test
  fun `resolve preserves an append ancestor with a distinct live contribution`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed(
        "new",
        RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
        predecessor = "old",
        uris = listOf("gs://raw/new"),
      )
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      assertThat(resolve("new")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
    }

  @Test
  fun `resolve ignores a fully replaced ancestor but retains partially replaced contribution`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("new", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING, predecessor = "old")
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
      assertThat(resolve("new")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      seed(
        "old",
        RawImpressionUploadModelLine.State.COMPLETED,
        uris = listOf("gs://raw/input", "gs://raw/retained"),
      )
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
    }

  @Test
  fun `resolve excludes prior output invalidated by a later eviction`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("new", RawImpressionUploadModelLine.State.FAILED, predecessor = "old")
      rows["new"] =
        rows.getValue("new").copy {
          failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
        }
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
    }

  @Test
  fun `resolve holds overwritten output while a descendant can write labeling files`() =
    runBlocking<Unit> {
      for (state in
        listOf(
          RawImpressionUploadModelLine.State.CREATED,
          RawImpressionUploadModelLine.State.POOL_ASSIGNING,
          RawImpressionUploadModelLine.State.RANKING,
          RawImpressionUploadModelLine.State.LABELING,
          RawImpressionUploadModelLine.State.FAILED,
        )) {
        seed("old", RawImpressionUploadModelLine.State.COMPLETED)
        seed("new", state, predecessor = "old")
        jobRows[upload("new")] =
          listOf(
            vidLabelingJob {
              name = "${upload("new")}/vidLabelingJobs/job"
              cmmsModelLines += MODEL_LINE
              rawImpressionUploadFiles += inputRows.getValue(upload("new")).single().name
            }
          )
        assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
      }
    }

  @Test
  fun `resolve preserves untouched prior output before descendant labeling exists`() =
    runBlocking<Unit> {
      for (state in
        listOf(
          RawImpressionUploadModelLine.State.CREATED,
          RawImpressionUploadModelLine.State.POOL_ASSIGNING,
          RawImpressionUploadModelLine.State.RANKING,
          RawImpressionUploadModelLine.State.LABELING,
          RawImpressionUploadModelLine.State.FAILED,
        )) {
        seed("old", RawImpressionUploadModelLine.State.COMPLETED)
        seed("new", state, predecessor = "old")
        jobRows.clear()
        assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      }
    }

  @Test
  fun `resolve follows the latest descendant independent of listing order`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("latest", RawImpressionUploadModelLine.State.COMPLETED, predecessor = "middle")
      seed("middle", RawImpressionUploadModelLine.State.COMPLETED, predecessor = "old")
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
    }

  @Test
  fun `resolve paginates every owner input and labeling job`() =
    runBlocking<Unit> {
      paginate = true
      seed(
        "old",
        RawImpressionUploadModelLine.State.COMPLETED,
        uris = listOf("gs://raw/input", "gs://raw/retained"),
      )
      seed("new", RawImpressionUploadModelLine.State.LABELING, predecessor = "old")
      jobRows[upload("new")] =
        listOf(
          vidLabelingJob {
            name = "${upload("new")}/vidLabelingJobs/job"
            rawImpressionUploadFiles += inputRows.getValue(upload("new")).single().name
          }
        )
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
    }

  @Test
  fun `resolve reuses one metadata snapshot within a synchronization`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      val snapshot = resolver()
      val key = done("owner")
      assertThat(snapshot.resolve(MODEL_LINE, EVENT_DATE, key))
        .isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      rows["owner"] =
        rows.getValue("owner").copy { state = RawImpressionUploadModelLine.State.FAILED }
      assertThat(snapshot.resolve(MODEL_LINE, EVENT_DATE, key))
        .isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      assertThat(resolver().resolve(MODEL_LINE, EVENT_DATE, key))
        .isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
      verifyBlocking(models, times(2)) { listRawImpressionUploadModelLines(any()) }
    }

  @Test
  fun `resolve rejects unknown wrong model and wrong date provenance`() =
    runBlocking<Unit> {
      assertFailsWith<IllegalStateException> { resolve("missing") }
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertFailsWith<IllegalStateException> {
        resolver()
          .resolve(MODEL_LINE, EVENT_DATE.plusDays(1), done("owner", EVENT_DATE.plusDays(1)))
      }
      assertFailsWith<IllegalStateException> {
        resolver().resolve(MODEL_LINE, EVENT_DATE, done("owner", EVENT_DATE.plusDays(1)))
      }
      rows["owner"] =
        rows.getValue("owner").copy {
          cmmsModelLine = "modelProviders/other/modelSuites/suite/modelLines/model"
        }
      assertFailsWith<IllegalStateException> { resolve("owner") }
    }

  @Test
  fun `resolve rejects invalid states foreign parents and foreign file bindings`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.STATE_UNSPECIFIED)
      assertFailsWith<IllegalStateException> { resolve("owner") }
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      rows["owner"] =
        rows.getValue("owner").copy {
          name = "dataProviders/other/rawImpressionUploads/owner/rawImpressionUploadModelLines/row"
        }
      assertFailsWith<IllegalStateException> { resolve("owner") }
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      inputRows[upload("owner")] =
        inputRows.getValue(upload("owner")).map {
          it.copy { name = "${upload("foreign")}/files/input" }
        }
      assertFailsWith<IllegalStateException> { resolve("owner") }
    }

  @Test
  fun `resolve rejects ambiguous branches and cyclic revisions`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("branch1", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING, predecessor = "old")
      seed("branch2", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING, predecessor = "old")
      assertFailsWith<IllegalStateException> { resolve("old") }
      rows.remove("branch2")
      uploadRows[upload("branch1")] =
        uploadRows.getValue(upload("branch1")).copy {
          replacesRawImpressionUpload = upload("branch1")
        }
      assertFailsWith<IllegalStateException> { resolve("old") }
    }

  @Test
  fun `resolve propagates unavailable ownership evidence without treating it obsolete`() =
    runBlocking<Unit> {
      lookupFailure = Status.UNAVAILABLE
      val failure = assertFailsWith<Exception> { resolve("missing") }
      assertThat(failure.message).contains("ownership lookup failed")
    }

  @Test
  fun `resolve waits for incomplete registration and excludes quarantined removed owners`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      uploadRows[upload("owner")] =
        uploadRows.getValue(upload("owner")).copy { registrationComplete = false }
      assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
      for (state in
        listOf(
          RawImpressionUpload.State.CORRECTION_REQUIRED,
          RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT,
        )) {
        uploadRows[upload("owner")] =
          uploadRows.getValue(upload("owner")).copy {
            registrationComplete = true
            this.state = state
          }
        assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
      }
    }

  @Test
  fun `resolve rejects a lookup that returns the wrong raw owner`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.COMPLETED)
      uploadRows[upload("owner")] =
        uploadRows.getValue(upload("owner")).copy { name = upload("other") }
      assertFailsWith<IllegalStateException> { resolve("owner") }
    }

  @Test
  fun `resolve caches labeling evidence within one invocation`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("new", RawImpressionUploadModelLine.State.LABELING, predecessor = "old")
      jobRows[upload("new")] =
        listOf(
          vidLabelingJob {
            rawImpressionUploadFiles += inputRows.getValue(upload("new")).single().name
          }
        )
      val snapshot = resolver()
      repeat(2) {
        assertThat(snapshot.resolve(MODEL_LINE, EVENT_DATE, done("old")))
          .isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
      }
      verifyBlocking(jobs, times(1)) { listVidLabelingJobs(any()) }
    }

  @Test
  fun `resolve filters overlaps without downloading unrelated owner files`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.COMPLETED)
      repeat(20) { id ->
        seed(
          "unrelated-$id",
          RawImpressionUploadModelLine.State.COMPLETED,
          uris = (0 until 500).map { "gs://raw/unrelated-$id-$it" },
        )
      }
      val snapshot = resolver()
      repeat(2) {
        assertThat(snapshot.resolve(MODEL_LINE, EVENT_DATE, done("owner")))
          .isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      }

      val requests = argumentCaptor<ListRawImpressionUploadFilesRequest>()
      verifyBlocking(files, times(2)) { listRawImpressionUploadFiles(requests.capture()) }
      val overlap = requests.allValues.single { it.parent.endsWith("/rawImpressionUploads/-") }
      assertThat(overlap.filter.blobUriInList).containsExactly("gs://raw/input")
      assertThat(downloadedFiles).isEqualTo(2)
      verifyBlocking(uploads, times(1)) { getRawImpressionUpload(any()) }
      verifyBlocking(uploads, times(0)) { listRawImpressionUploads(any()) }
    }

  @Test
  fun `resolve rejects unknown wire states and holds a created parent`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      // Raw wire values represent enum values unknown to this client.
      rows["owner"] = rows.getValue("owner").toBuilder().setStateValue(99).build()
      assertFailsWith<IllegalStateException> { resolve("owner") }
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      uploadRows[upload("owner")] =
        uploadRows.getValue(upload("owner")).copy {
          state = RawImpressionUpload.State.STATE_UNSPECIFIED
        }
      assertFailsWith<IllegalStateException> { resolve("owner") }
      uploadRows[upload("owner")] =
        uploadRows.getValue(upload("owner")).toBuilder().setStateValue(99).build()
      assertFailsWith<IllegalStateException> { resolve("owner") }
      uploadRows[upload("owner")] =
        uploadRows.getValue(upload("owner")).copy { state = RawImpressionUpload.State.CREATED }
      assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
    }

  @Test
  fun `resolve does not restore old output because a newer registration has no labeling yet`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("middle", RawImpressionUploadModelLine.State.FAILED, predecessor = "old")
      rows["middle"] =
        rows.getValue("middle").copy {
          failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
        }
      seed("new", RawImpressionUploadModelLine.State.CREATED, predecessor = "middle")
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
      rows["middle"] =
        rows.getValue("middle").copy { state = RawImpressionUploadModelLine.State.RANKING }
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
      rows["middle"] =
        rows.getValue("middle").copy {
          state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
          failureReason = RawImpressionUploadModelLine.FailureReason.FAILURE_REASON_UNSPECIFIED
        }
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
      assertThat(resolve("middle")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
    }

  @Test
  fun `resolve bounds overlap URI batches and keeps ownership across cache eviction`() =
    runBlocking<Unit> {
      seed(
        "many",
        RawImpressionUploadModelLine.State.COMPLETED,
        uris = (0 until 201).map { "gs://raw/input-$it" },
      )
      val snapshot = resolver()
      assertThat(snapshot.resolve(MODEL_LINE, EVENT_DATE, done("many")))
        .isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      val requests = argumentCaptor<ListRawImpressionUploadFilesRequest>()
      verifyBlocking(files, times(4)) { listRawImpressionUploadFiles(requests.capture()) }
      val overlaps = requests.allValues.filter { it.parent.endsWith("/rawImpressionUploads/-") }
      assertThat(overlaps.map { it.filter.blobUriInCount }).containsExactly(100, 100, 1)
      assertThat(downloadedFiles).isEqualTo(402)

      repeat(40) { id ->
        seed(
          "cached-$id",
          RawImpressionUploadModelLine.State.COMPLETED,
          uris = listOf("gs://raw/cached-$id"),
        )
      }
      val fresh = resolver()
      repeat(40) { id ->
        assertThat(fresh.resolve(MODEL_LINE, EVENT_DATE, done("cached-$id")))
          .isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      }
      assertThat(fresh.resolve(MODEL_LINE, EVENT_DATE, done("cached-0")))
        .isEqualTo(ManagedDoneObjectDisposition.CURRENT)
    }

  @Test
  fun `zero-output model lookup accepts a canonical key without a storage root`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      val key = LabeledImpressionsBlobKeys.forDone(upload("owner"), MODEL_LINE, EVENT_DATE)
      assertThat(resolver().modelLineFor(EVENT_DATE, key)).isEqualTo(MODEL_LINE)
      assertThat(downloadedFiles).isEqualTo(0)
    }

  @Test
  fun `resolver default rejects model inference without consulting output disposition`() =
    runBlocking<Unit> {
      var resolved = false
      val owners = ManagedDoneObjectResolver { _, _, _ ->
        resolved = true
        ManagedDoneObjectDisposition.CURRENT
      }
      val failure =
        assertFailsWith<IllegalStateException> { owners.modelLineFor(EVENT_DATE, done("owner")) }
      assertThat(failure).hasMessageThat().contains("authoritative lookup")
      assertThat(resolved).isFalse()
    }

  @Test
  fun `resolve does not supersede the same input URI registered for another date`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("new", RawImpressionUploadModelLine.State.COMPLETED, predecessor = "old")
      val nextDate = EVENT_DATE.plusDays(1)
      inputRows[upload("new")] =
        inputRows.getValue(upload("new")).map {
          it.copy {
            eventDate = date {
              year = nextDate.year
              month = nextDate.monthValue
              day = nextDate.dayOfMonth
            }
          }
        }
      assertThat(resolve("old")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
    }

  @Test
  fun `resolve rejects unknown state of an overlapping descendant`() =
    runBlocking<Unit> {
      seed("old", RawImpressionUploadModelLine.State.COMPLETED)
      seed("new", RawImpressionUploadModelLine.State.CREATED, predecessor = "old")
      val original = rows.getValue("new")
      for (state in listOf(0, 999)) {
        rows["new"] = original.toBuilder().setStateValue(state).build()
        val failure = assertFailsWith<IllegalStateException> { resolve("old") }
        assertThat(failure).hasMessageThat().contains("invalid state")
      }
    }

  @Test
  fun `resolve rejects a wildcard input response that ignores its URI filter`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.COMPLETED)
      wildcardFilesOverride =
        inputRows.getValue(upload("owner")).map { it.copy { blobUri = "gs://raw/not-requested" } }
      val failure = assertFailsWith<IllegalStateException> { resolve("owner") }
      assertThat(failure).hasMessageThat().contains("ignored its URI filter")
    }

  @Test
  fun `resolve rejects a matching wildcard input belonging to another provider`() =
    runBlocking<Unit> {
      seed("owner", RawImpressionUploadModelLine.State.COMPLETED)
      wildcardFilesOverride =
        inputRows.getValue(upload("owner")).map {
          it.copy { name = "dataProviders/other/rawImpressionUploads/foreign/files/input" }
        }
      val failure = assertFailsWith<IllegalStateException> { resolve("owner") }
      assertThat(failure).hasMessageThat().contains("another DataProvider")
    }

  @Test
  fun `resolve exhausts unrelated labeling job pages and caches negative ownership evidence`() =
    runBlocking<Unit> {
      seed(
        "old",
        RawImpressionUploadModelLine.State.COMPLETED,
        uris = listOf("gs://raw/shared", "gs://raw/retained"),
      )
      seed(
        "new",
        RawImpressionUploadModelLine.State.RANKING,
        predecessor = "old",
        uris = listOf("gs://raw/shared", "gs://raw/new-1", "gs://raw/new-2"),
      )
      jobRows[upload("new")] =
        inputRows.getValue(upload("new")).drop(1).mapIndexed { index, file ->
          vidLabelingJob {
            name = "${upload("new")}/vidLabelingJobs/job-$index"
            cmmsModelLines += MODEL_LINE
            rawImpressionUploadFiles += file.name
          }
        }
      paginate = true
      val snapshot = resolver()
      repeat(2) {
        assertThat(snapshot.resolve(MODEL_LINE, EVENT_DATE, done("old")))
          .isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      }
      val requests = argumentCaptor<ListVidLabelingJobsRequest>()
      verifyBlocking(jobs, times(2)) { listVidLabelingJobs(requests.capture()) }
      assertThat(requests.allValues.map { it.pageToken }).containsExactly("", "next").inOrder()
    }

  @Test
  fun `resolve holds recoverable failures until a fresh snapshot observes recovered labeling`() =
    runBlocking<Unit> {
      for (reason in
        listOf(
          RawImpressionUploadModelLine.FailureReason.FAILURE_REASON_UNSPECIFIED,
          RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE,
        )) {
        seed("owner", RawImpressionUploadModelLine.State.FAILED)
        rows["owner"] = rows.getValue("owner").copy { failureReason = reason }
        assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.FINALIZING)
        rows["owner"] =
          rows.getValue("owner").copy {
            state = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
          }
        assertThat(resolve("owner")).isEqualTo(ManagedDoneObjectDisposition.CURRENT)
      }
      seed("evicted", RawImpressionUploadModelLine.State.FAILED, uris = listOf("gs://raw/evicted"))
      rows["evicted"] =
        rows.getValue("evicted").copy {
          failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
        }
      assertThat(resolve("evicted")).isEqualTo(ManagedDoneObjectDisposition.OBSOLETE)
    }

  private fun seed(
    id: String,
    state: RawImpressionUploadModelLine.State,
    predecessor: String = "",
    uris: List<String> = listOf("gs://raw/input"),
  ) {
    val name = upload(id)
    rows[id] = rawImpressionUploadModelLine {
      this.name = "$name/rawImpressionUploadModelLines/row"
      cmmsModelLine = MODEL_LINE
      this.state = state
    }
    uploadRows[name] = rawImpressionUpload {
      this.name = name
      registrationComplete = true
      this.state = RawImpressionUpload.State.ACTIVE
      if (predecessor.isNotEmpty()) replacesRawImpressionUpload = upload(predecessor)
    }
    inputRows[name] =
      uris.mapIndexed { index, uri ->
        rawImpressionUploadFile {
          this.name = "$name/files/file-$index"
          blobUri = uri
          sizeBytes = 1L
          blobGeneration = 1L
          eventDate = date {
            year = EVENT_DATE.year
            month = EVENT_DATE.monthValue
            day = EVENT_DATE.dayOfMonth
          }
        }
      }
  }

  private fun resolver() =
    GrpcManagedDoneObjectResolver(
      DATA_PROVIDER,
      RawImpressionUploadServiceCoroutineStub(server.channel),
      RawImpressionUploadModelLineServiceCoroutineStub(server.channel),
      RawImpressionUploadFileServiceCoroutineStub(server.channel),
      VidLabelingJobServiceCoroutineStub(server.channel),
      object : Throttler {
        override suspend fun <T> onReady(block: suspend () -> T): T = block()
      },
    )

  private suspend fun resolve(id: String): ManagedDoneObjectDisposition =
    resolver().resolve(MODEL_LINE, EVENT_DATE, done(id))

  private fun upload(id: String) = "$DATA_PROVIDER/rawImpressionUploads/$id"

  private fun done(id: String, date: LocalDate = EVENT_DATE) =
    "labeled/" + LabeledImpressionsBlobKeys.forDone(upload(id), MODEL_LINE, date)

  companion object {
    private const val DATA_PROVIDER = "dataProviders/provider"
    private const val MODEL_LINE = "modelProviders/provider/modelSuites/suite/modelLines/model"
    private val EVENT_DATE = LocalDate.of(2026, 1, 1)
  }
}
