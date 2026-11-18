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

package org.wfanet.measurement.integration.testing

import io.grpc.Status
import io.grpc.StatusException
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.time.Duration
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest

/** Waits until every labeled fixture's exact file and completion generations are registered. */
class ImpressionMetadataAwaiter(
  private val metadataStub: ImpressionMetadataServiceCoroutineStub,
  private val pollInterval: Duration,
  private val timeout: Duration,
  private val rpcTimeout: Duration,
) {
  init {
    require(pollInterval.isPositive() && pollInterval.isFinite())
    require(timeout.isPositive() && timeout.isFinite())
    require(rpcTimeout.isPositive() && rpcTimeout.isFinite())
  }

  /** Expected metadata is grouped by its DataProvider parent. */
  suspend fun awaitCompletion(expectedByParent: Map<String, List<ImpressionMetadata>>) {
    require(expectedByParent.isNotEmpty() && expectedByParent.values.all { it.isNotEmpty() }) {
      "No labeled fixtures to await"
    }
    require(
      expectedByParent.values.flatten().all {
        it.blobUri.isNotEmpty() &&
          it.modelLine.isNotEmpty() &&
          it.metadataBlobGeneration > 0 &&
          it.impressionsBlobGeneration > 0 &&
          it.outputDoneBlobGeneration > 0
      }
    ) {
      "Fixture URIs, model lines, and positive generations are required"
    }
    var lastSummary = "no successful metadata read yet"
    var lastReadFailure: StatusException? = null
    val completed =
      withTimeoutOrNull(timeout) {
        while (true) {
          val pending = mutableListOf<String>()
          try {
            for ((parent, expected) in expectedByParent) {
              val actualByUri =
                listMetadata(parent, expected.map { it.blobUri }).associateBy { it.blobUri }
              for (fixture in expected) {
                val actual = actualByUri[fixture.blobUri]
                if (
                  actual == null ||
                    actual.state != ImpressionMetadata.State.ACTIVE ||
                    actual.modelLine != fixture.modelLine ||
                    actual.metadataBlobGeneration != fixture.metadataBlobGeneration ||
                    actual.impressionsBlobGeneration != fixture.impressionsBlobGeneration ||
                    actual.outputDoneBlobGeneration != fixture.outputDoneBlobGeneration
                ) {
                  pending +=
                    "${fixture.blobUri}: expected ${versions(fixture)}, observed " +
                      (actual?.let { "${it.state}, ${it.modelLine}, ${versions(it)}" } ?: "missing")
                }
              }
            }
            lastReadFailure = null
            if (pending.isEmpty()) return@withTimeoutOrNull true
            lastSummary = pending.joinToString("; ")
          } catch (e: StatusException) {
            if (e.status.code !in RETRYABLE_READ_CODES) throw e
            lastReadFailure = e
            logger.warning("Fixture metadata read failed (${e.status.code}); retrying")
          }
          logger.info("Waiting for labeled fixture synchronization. $lastSummary")
          delay(pollInterval)
        }
        @Suppress("UNREACHABLE_CODE") false
      }
    if (completed != true) {
      throw IllegalStateException(
        "Timed out after $timeout waiting for labeled fixture synchronization. $lastSummary",
        lastReadFailure,
      )
    }
  }

  private suspend fun listMetadata(
    parent: String,
    blobUris: List<String>,
  ): List<ImpressionMetadata> {
    val result = mutableListOf<ImpressionMetadata>()
    for (chunk in blobUris.chunked(LIST_PAGE_SIZE)) {
      var pageToken = ""
      do {
        val response =
          metadataStub
            .withDeadlineAfter(rpcTimeout.inWholeNanoseconds, TimeUnit.NANOSECONDS)
            .listImpressionMetadata(
              listImpressionMetadataRequest {
                this.parent = parent
                pageSize = LIST_PAGE_SIZE
                this.pageToken = pageToken
                showDeleted = true
                this.filter = filter { this.blobUris += chunk }
              }
            )
        result += response.impressionMetadataList
        pageToken = response.nextPageToken
      } while (pageToken.isNotEmpty())
    }
    return result
  }

  private fun versions(metadata: ImpressionMetadata): String =
    "metadata=${metadata.metadataBlobGeneration}, impressions=${metadata.impressionsBlobGeneration}, " +
      "done=${metadata.outputDoneBlobGeneration}"

  companion object {
    /** Rejects incomplete rewritten fixtures before their completion markers are created. */
    fun requireCompleteFixtureCoverage(
      expectedBlobs: Map<String, Long>,
      doneBlobUris: Collection<String>,
    ) {
      check(doneBlobUris.isNotEmpty()) { "No fixture folders to validate" }
      for (doneBlobUri in doneBlobUris) {
        val directory = doneBlobUri.substringBeforeLast('/')
        val metadataBlobs =
          expectedBlobs.filterKeys {
            it.substringBeforeLast('/') == directory &&
              it.substringAfterLast('/').startsWith("metadata") &&
              it.endsWith(".binpb")
          }
        check(metadataBlobs.isNotEmpty()) {
          "No rewritten labeled fixtures recorded for $directory"
        }
        for ((uri, generation) in metadataBlobs) {
          val suffix = uri.substringAfterLast('/').removePrefix("metadata").removeSuffix(".binpb")
          val impressionsUri = "$directory/impressions$suffix"
          check(generation > 0 && (expectedBlobs[impressionsUri] ?: 0L) > 0) {
            "Incomplete rewritten labeled fixture: $uri and $impressionsUri"
          }
        }
      }
    }

    private val logger = Logger.getLogger(ImpressionMetadataAwaiter::class.java.name)
    private const val LIST_PAGE_SIZE = 100
    private val RETRYABLE_READ_CODES =
      setOf(
        Status.Code.INTERNAL,
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.ABORTED,
      )
  }
}
