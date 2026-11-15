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

import io.grpc.Status
import io.grpc.StatusException
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.Duration
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest

/** Waits for the exact raw-impression uploads seeded by a cloud test to finish labeling. */
class VidLabelingAwaiter(
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val pollInterval: Duration,
  private val timeout: Duration,
  private val rpcTimeout: Duration,
  private val maxConsecutiveReadFailures: Int,
) {
  init {
    require(pollInterval.isPositive() && pollInterval.isFinite())
    require(timeout.isPositive() && timeout.isFinite())
    require(rpcTimeout.isPositive() && rpcTimeout.isFinite())
    require(maxConsecutiveReadFailures > 0)
  }

  /** [expectedDoneBlobs] maps each seeded GCS URI to the generation returned by its write. */
  suspend fun awaitCompletion(
    dataProvider: String,
    expectedDoneBlobs: Map<String, Long>,
  ): List<RawImpressionUploadModelLine> {
    require(expectedDoneBlobs.isNotEmpty()) { "No seeded done blobs to await" }
    require(expectedDoneBlobs.values.all { it > 0 }) { "Done-blob generations must be positive" }
    var consecutiveReadFailures = 0
    var lastReadFailure: StatusException? = null
    var lastSummary = "no successful metadata read yet"
    val completedLines =
      withTimeoutOrNull(timeout) {
        while (true) {
          try {
            val uploads =
              listUploads(dataProvider).filter {
                expectedDoneBlobs[it.doneBlobUri] == it.doneBlobGeneration
              }
            val failedUploads = uploads.filter { it.state == RawImpressionUpload.State.FAILED }
            check(failedUploads.isEmpty()) {
              "VID labeling FAILED for upload(s): ${failedUploads.joinToString { it.name }}"
            }
            val linesByUpload = uploads.associateWith { listModelLines(it.name) }
            val lines = linesByUpload.values.flatten()
            val failed = lines.filter { it.state == RawImpressionUploadModelLine.State.FAILED }
            check(failed.isEmpty()) {
              "VID labeling FAILED for model line(s): ${failed.joinToString { it.name }}"
            }
            // Reset only after every read in the poll succeeds, including model-line pages.
            consecutiveReadFailures = 0
            lastReadFailure = null
            val missing = expectedDoneBlobs.keys - uploads.map { it.doneBlobUri }.toSet()
            lastSummary =
              "missing done blobs=$missing; " +
                linesByUpload.entries.joinToString { (upload, children) ->
                  "${upload.name}: registrationComplete=${upload.registrationComplete}, " +
                    "model lines=${children.groupingBy { it.state.name }.eachCount()}"
                }
            if (
              missing.isEmpty() &&
                linesByUpload.values.isNotEmpty() &&
                linesByUpload.all { (upload, children) ->
                  upload.registrationComplete &&
                    children.isNotEmpty() &&
                    children.all { it.state == RawImpressionUploadModelLine.State.COMPLETED }
                }
            ) {
              return@withTimeoutOrNull lines
            }
          } catch (e: StatusException) {
            if (e.status.code !in RETRYABLE_READ_CODES) throw e
            lastReadFailure = e
            consecutiveReadFailures++
            if (consecutiveReadFailures >= maxConsecutiveReadFailures) {
              throw IllegalStateException(
                "Metadata polling failed $consecutiveReadFailures consecutive times " +
                  "(${e.status.code}). Last successful state: $lastSummary",
                e,
              )
            }
            logger.log(
              Level.WARNING,
              "Metadata read failed (${e.status.code}); retry " +
                "$consecutiveReadFailures/$maxConsecutiveReadFailures. Last state: $lastSummary",
              e,
            )
          }
          logger.info("Waiting for VID labeling. $lastSummary")
          delay(pollInterval)
        }
        @Suppress("UNREACHABLE_CODE") emptyList<RawImpressionUploadModelLine>()
      }
    return completedLines
      ?: throw IllegalStateException(
        "Timed out after $timeout waiting for VID labeling. Last state: $lastSummary",
        lastReadFailure,
      )
  }

  private suspend fun listUploads(dataProvider: String): List<RawImpressionUpload> {
    val result = mutableListOf<RawImpressionUpload>()
    var pageToken = ""
    do {
      val response =
        uploadsStub
          .withDeadlineAfter(rpcTimeout.inWholeNanoseconds, TimeUnit.NANOSECONDS)
          .listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = dataProvider
              pageSize = LIST_PAGE_SIZE
              this.pageToken = pageToken
            }
          )
      result.addAll(response.rawImpressionUploadsList)
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return result
  }

  private suspend fun listModelLines(uploadName: String): List<RawImpressionUploadModelLine> {
    val result = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        modelLinesStub
          .withDeadlineAfter(rpcTimeout.inWholeNanoseconds, TimeUnit.NANOSECONDS)
          .listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = uploadName
              pageSize = LIST_PAGE_SIZE
              this.pageToken = pageToken
            }
          )
      result.addAll(response.rawImpressionUploadModelLinesList)
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return result
  }

  companion object {
    private val logger = Logger.getLogger(VidLabelingAwaiter::class.java.name)
    private const val LIST_PAGE_SIZE = 100
    // Public INTERNAL can represent a dependency interruption. Only these read-only polls retry;
    // persistent server errors still fail after a bounded number of consecutive failures.
    private val RETRYABLE_READ_CODES =
      setOf(
        Status.Code.INTERNAL,
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.ABORTED,
      )
  }
}
