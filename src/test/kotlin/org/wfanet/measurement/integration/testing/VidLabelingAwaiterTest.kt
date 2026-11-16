/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.testing

import com.google.common.truth.Truth.assertThat
import io.grpc.Context
import io.grpc.Status
import io.grpc.StatusException
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

@OptIn(ExperimentalCoroutinesApi::class)
class VidLabelingAwaiterTest {
  private val uploadRequests = mutableListOf<ListRawImpressionUploadsRequest>()
  private val modelLineRequests = mutableListOf<ListRawImpressionUploadModelLinesRequest>()
  private val deadlines = mutableListOf<Long>()
  private var readUploads: (ListRawImpressionUploadsRequest) -> ListRawImpressionUploadsResponse = {
    listRawImpressionUploadsResponse { rawImpressionUploads += UPLOAD }
  }
  private var readModelLines:
    (ListRawImpressionUploadModelLinesRequest) -> ListRawImpressionUploadModelLinesResponse =
    {
      listRawImpressionUploadModelLinesResponse { rawImpressionUploadModelLines += COMPLETED_LINE }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(
      object : RawImpressionUploadServiceCoroutineImplBase(Dispatchers.Unconfined) {
        override suspend fun listRawImpressionUploads(
          request: ListRawImpressionUploadsRequest
        ): ListRawImpressionUploadsResponse {
          uploadRequests += request
          deadlines += checkNotNull(Context.current().deadline).timeRemaining(TimeUnit.MILLISECONDS)
          return readUploads(request)
        }
      }
    )
    addService(
      object : RawImpressionUploadModelLineServiceCoroutineImplBase(Dispatchers.Unconfined) {
        override suspend fun listRawImpressionUploadModelLines(
          request: ListRawImpressionUploadModelLinesRequest
        ): ListRawImpressionUploadModelLinesResponse {
          modelLineRequests += request
          deadlines += checkNotNull(Context.current().deadline).timeRemaining(TimeUnit.MILLISECONDS)
          return readModelLines(request)
        }
      }
    )
  }

  @Test
  fun `accepts this generation even if registration finished before polling`() = runTest {
    val lines = awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)

    assertThat(lines).containsExactly(COMPLETED_LINE)
    assertThat(uploadRequests).hasSize(1)
    assertThat(modelLineRequests.single().parent).isEqualTo(UPLOAD.name)
    assertThat(deadlines).hasSize(2)
    for (deadline in deadlines) {
      assertThat(deadline).isGreaterThan(0L)
      assertThat(deadline).isAtMost(30_000L)
    }
  }

  @Test
  fun `retries public INTERNAL then succeeds`() = runTest {
    readUploads = {
      if (uploadRequests.size == 1) throw Status.INTERNAL.asException()
      listRawImpressionUploadsResponse { rawImpressionUploads += UPLOAD }
    }

    assertThat(awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS))
      .containsExactly(COMPLETED_LINE)
    assertThat(uploadRequests).hasSize(2)
    assertThat(testScheduler.currentTime).isEqualTo(10_000L)
  }

  @Test
  fun `persistent public INTERNAL fails with its cause`() = runTest {
    readUploads = { throw Status.INTERNAL.withDescription("metadata read failed").asException() }

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
      }

    assertThat(failure).hasMessageThat().contains("3 consecutive times (INTERNAL)")
    val rpcFailure =
      generateSequence<Throwable>(failure) { it.cause }.filterIsInstance<StatusException>().single()
    assertThat(rpcFailure.status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(uploadRequests).hasSize(3)
  }

  @Test
  fun `successful upload reads do not reset repeated model line failures`() = runTest {
    readModelLines = { throw Status.UNAVAILABLE.asException() }

    assertFailsWith<IllegalStateException> {
      awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
    }

    assertThat(uploadRequests).hasSize(3)
    assertThat(modelLineRequests).hasSize(3)
  }

  @Test
  fun `successful full polls reset the read failure count`() = runTest {
    readModelLines = {
      when (modelLineRequests.size) {
        1,
        3 -> throw Status.ABORTED.asException()
        2,
        4 -> listRawImpressionUploadModelLinesResponse {}
        else ->
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += COMPLETED_LINE
          }
      }
    }

    assertThat(awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS))
      .containsExactly(COMPLETED_LINE)
    assertThat(modelLineRequests).hasSize(5)
  }

  @Test
  fun `permission errors fail immediately`() = runTest {
    readUploads = { throw Status.PERMISSION_DENIED.asException() }

    val failure =
      assertFailsWith<StatusException> {
        awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
      }

    assertThat(failure.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(uploadRequests).hasSize(1)
  }

  @Test
  fun `failed model lines fail immediately`() = runTest {
    readModelLines = {
      listRawImpressionUploadModelLinesResponse {
        rawImpressionUploadModelLines +=
          COMPLETED_LINE.toBuilder().setState(RawImpressionUploadModelLine.State.FAILED).build()
      }
    }

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
      }

    assertThat(failure).hasMessageThat().contains(COMPLETED_LINE.name)
    assertThat(modelLineRequests).hasSize(1)
  }

  @Test
  fun `failed uploads fail immediately`() = runTest {
    readUploads = {
      listRawImpressionUploadsResponse {
        rawImpressionUploads +=
          UPLOAD.toBuilder().setState(RawImpressionUpload.State.FAILED).build()
      }
    }

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
      }

    assertThat(failure).hasMessageThat().contains(UPLOAD.name)
    assertThat(uploadRequests).hasSize(1)
    assertThat(modelLineRequests).isEmpty()
  }

  @Test
  fun `correction required uploads fail immediately`() = runTest {
    readUploads = {
      listRawImpressionUploadsResponse {
        rawImpressionUploads +=
          UPLOAD.copy { state = RawImpressionUpload.State.CORRECTION_REQUIRED }
      }
    }
    readModelLines = { listRawImpressionUploadModelLinesResponse {} }

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
      }

    assertThat(failure).hasMessageThat().contains(UPLOAD.name)
    assertThat(failure).hasMessageThat().contains("CORRECTION_REQUIRED")
    assertThat(uploadRequests).hasSize(1)
    assertThat(modelLineRequests).isEmpty()
    assertThat(testScheduler.currentTime).isEqualTo(0L)
  }

  @Test
  fun `uploads removed without replacement fail immediately`() = runTest {
    readUploads = {
      listRawImpressionUploadsResponse {
        rawImpressionUploads +=
          UPLOAD.copy { state = RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT }
      }
    }
    readModelLines = { listRawImpressionUploadModelLinesResponse {} }

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
      }

    assertThat(failure).hasMessageThat().contains(UPLOAD.name)
    assertThat(failure).hasMessageThat().contains("REMOVED_WITHOUT_REPLACEMENT")
    assertThat(uploadRequests).hasSize(1)
    assertThat(modelLineRequests).isEmpty()
    assertThat(testScheduler.currentTime).isEqualTo(0L)
  }

  @Test
  fun `completed model lines cannot finish an incompletely registered upload`() = runTest {
    readUploads = {
      listRawImpressionUploadsResponse {
        rawImpressionUploads +=
          UPLOAD.toBuilder().setRegistrationComplete(uploadRequests.size > 1).build()
      }
    }

    assertThat(awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS))
      .containsExactly(COMPLETED_LINE)
    assertThat(uploadRequests).hasSize(2)
  }

  @Test
  fun `old generations and missing uploads cannot satisfy the wait`() = runTest {
    val oldUpload =
      UPLOAD.toBuilder()
        .setName("$DATA_PROVIDER/rawImpressionUploads/old")
        .setDoneBlobGeneration(6)
        .build()
    readUploads = { listRawImpressionUploadsResponse { rawImpressionUploads += oldUpload } }

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS)
      }

    assertThat(failure).hasMessageThat().contains("Timed out")
    assertThat(failure).hasMessageThat().contains(DONE_URI)
    assertThat(testScheduler.currentTime).isEqualTo(60_000L)
    assertThat(modelLineRequests).isEmpty()
  }

  @Test
  fun `requires completed registration and nonempty lines for every expected upload`() = runTest {
    val otherUri = "gs://test-bucket/raw/2021-03-22/done"
    val otherUpload =
      UPLOAD.toBuilder()
        .setName("$DATA_PROVIDER/rawImpressionUploads/other")
        .setDoneBlobUri(otherUri)
        .setDoneBlobGeneration(8)
        .build()
    val otherLine =
      COMPLETED_LINE.toBuilder().setName("${otherUpload.name}/modelLines/model").build()
    readUploads = {
      listRawImpressionUploadsResponse {
        rawImpressionUploads +=
          UPLOAD.toBuilder().setRegistrationComplete(uploadRequests.size > 1).build()
        rawImpressionUploads += otherUpload
      }
    }
    readModelLines = { request ->
      listRawImpressionUploadModelLinesResponse {
        if (request.parent == UPLOAD.name) rawImpressionUploadModelLines += COMPLETED_LINE
        else if (uploadRequests.size > 2) rawImpressionUploadModelLines += otherLine
      }
    }

    val lines = awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS + (otherUri to 8L))

    assertThat(lines).containsExactly(COMPLETED_LINE, otherLine)
    assertThat(uploadRequests).hasSize(3)
  }

  @Test
  fun `reads all upload and model line pages`() = runTest {
    val oldUpload = UPLOAD.toBuilder().setDoneBlobGeneration(6).build()
    readUploads = { request ->
      listRawImpressionUploadsResponse {
        if (request.pageToken.isEmpty()) {
          rawImpressionUploads += oldUpload
          nextPageToken = "uploads-page-2"
        } else rawImpressionUploads += UPLOAD
      }
    }
    readModelLines = { request ->
      listRawImpressionUploadModelLinesResponse {
        if (request.pageToken.isEmpty()) nextPageToken = "lines-page-2"
        else rawImpressionUploadModelLines += COMPLETED_LINE
      }
    }

    assertThat(awaiter().awaitCompletion(DATA_PROVIDER, EXPECTED_DONE_BLOBS))
      .containsExactly(COMPLETED_LINE)
    assertThat(uploadRequests.map { it.pageToken }).containsExactly("", "uploads-page-2").inOrder()
    assertThat(modelLineRequests.map { it.pageToken }).containsExactly("", "lines-page-2").inOrder()
  }

  private fun awaiter() =
    VidLabelingAwaiter(
      RawImpressionUploadServiceCoroutineStub(grpcTestServerRule.channel),
      RawImpressionUploadModelLineServiceCoroutineStub(grpcTestServerRule.channel),
      pollInterval = 10.seconds,
      timeout = 1.minutes,
      rpcTimeout = 30.seconds,
      maxConsecutiveReadFailures = 3,
    )

  companion object {
    private const val DATA_PROVIDER = "dataProviders/test"
    private const val DONE_URI = "gs://test-bucket/raw/2021-03-21/done"
    private val EXPECTED_DONE_BLOBS = mapOf(DONE_URI to 7L)
    private val UPLOAD = rawImpressionUpload {
      name = "$DATA_PROVIDER/rawImpressionUploads/current"
      doneBlobUri = DONE_URI
      doneBlobGeneration = 7
      registrationComplete = true
      state = RawImpressionUpload.State.COMPLETED
    }
    private val COMPLETED_LINE = rawImpressionUploadModelLine {
      name = "${UPLOAD.name}/modelLines/model"
      state = RawImpressionUploadModelLine.State.COMPLETED
    }
  }
}
