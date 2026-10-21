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

import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.StorageClient

/** Replays [DataAvailabilitySync] for selected completion blobs. */
class DataAvailabilitySyncReplay(
  private val storageClient: StorageClient,
  private val storageRootUri: BlobUri,
  private val selection: Selection,
  private val sync: suspend (doneBlobUri: String) -> DataAvailabilitySync.Outcome,
) {
  sealed interface Selection {
    data class BlobPrefix(val value: String) : Selection {
      init {
        require(value.isNotBlank()) { "blob prefix must not be blank" }
        require(!value.startsWith('/')) { "blob prefix must not start with a slash" }
      }
    }

    data class DoneBlobUris(val values: Set<String>) : Selection {
      init {
        require(values.isNotEmpty()) { "done blob URIs must not be empty" }
      }
    }
  }

  /** A failed replay for [doneBlobUri], retaining [exception] and its cause chain. */
  data class ReplayError(val doneBlobUri: String, val exception: Exception) {
    /** Human-readable summary of [exception]. */
    val message: String
      get() = exception.message ?: exception::class.java.simpleName
  }

  /** Aggregate result for a replay run. */
  data class Result(
    val doneBlobsSelected: Int,
    val published: Int,
    val noWork: Int,
    val blockedByGaps: Int,
    val errors: List<ReplayError>,
  )

  /** Replays completion blobs sequentially to avoid overwhelming downstream services. */
  suspend fun replay(): Result {
    val doneBlobUris = listDoneBlobUris()

    var published = 0
    var noWork = 0
    var blockedByGaps = 0
    val errors = mutableListOf<ReplayError>()
    for (doneBlobUri in doneBlobUris) {
      try {
        when (sync(doneBlobUri)) {
          DataAvailabilitySync.Outcome.PUBLISHED -> published++
          DataAvailabilitySync.Outcome.NO_WORK -> noWork++
          DataAvailabilitySync.Outcome.BLOCKED_GAPS -> blockedByGaps++
        }
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        errors += ReplayError(doneBlobUri, e)
      }
    }

    return Result(
      doneBlobsSelected = doneBlobUris.size,
      published = published,
      noWork = noWork,
      blockedByGaps = blockedByGaps,
      errors = errors,
    )
  }

  private suspend fun listDoneBlobUris(): List<String> {
    return when (selection) {
      is Selection.BlobPrefix -> {
        val normalizedBlobPrefix = "${selection.value.trimEnd('/')}/"
        storageClient
          .listBlobs(normalizedBlobPrefix)
          .filter { it.blobKey.substringAfterLast('/') == DONE_BLOB_FILE_NAME }
          .map { BlobUris.buildUri(storageRootUri, it.blobKey) }
          .toList()
          .sorted()
      }
      is Selection.DoneBlobUris -> selection.values.sorted()
    }
  }

  companion object {
    private const val DONE_BLOB_FILE_NAME = "done"
  }
}
