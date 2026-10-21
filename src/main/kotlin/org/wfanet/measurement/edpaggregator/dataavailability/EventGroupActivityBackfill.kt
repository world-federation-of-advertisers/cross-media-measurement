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

import io.grpc.StatusException
import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter as listImpressionMetadataFilter
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.StorageClient

/**
 * Backfills EventGroup activity from active metadata in folders selected by completion blobs.
 *
 * This only reads ImpressionMetadata and publishes activity. It does not persist or undelete
 * metadata, publish data availability, or modify completion blobs.
 */
class EventGroupActivityBackfill(
  private val storageClient: StorageClient,
  private val storageRootUri: BlobUri,
  private val selection: Selection,
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val throttler: Throttler,
  private val impressionMetadataPageSize: Int,
  private val publishEventGroupActivities: suspend (Collection<ImpressionMetadata>) -> Int,
) {
  init {
    require(dataProviderName.isNotEmpty()) { "dataProviderName must not be empty" }
    require(impressionMetadataPageSize > 0) { "impressionMetadataPageSize must be positive" }
  }

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

  /** A failed backfill for one completion blob. */
  data class BackfillError(val doneBlobUri: String, val message: String)

  /** Aggregate result for a backfill run. */
  data class Result(
    val doneBlobsSelected: Int,
    val activityPairsPublished: Int,
    val noWork: Int,
    val errors: List<BackfillError>,
  )

  /** Processes completion blobs sequentially to avoid overwhelming downstream services. */
  suspend fun backfill(): Result {
    val doneBlobUris = listDoneBlobUris()

    var activityPairsPublished = 0
    var noWork = 0
    val errors = mutableListOf<BackfillError>()
    for (doneBlobUri in doneBlobUris) {
      try {
        val impressionMetadata = listActiveImpressionMetadata(doneBlobUri)
        val published =
          if (impressionMetadata.isEmpty()) {
            0
          } else {
            publishEventGroupActivities(impressionMetadata)
          }
        if (published == 0) {
          noWork++
        } else {
          activityPairsPublished += published
        }
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        errors += BackfillError(doneBlobUri, e.message ?: e::class.java.simpleName)
      }
    }

    return Result(
      doneBlobsSelected = doneBlobUris.size,
      activityPairsPublished = activityPairsPublished,
      noWork = noWork,
      errors = errors,
    )
  }

  private suspend fun listActiveImpressionMetadata(doneBlobUri: String): List<ImpressionMetadata> {
    require(doneBlobUri.endsWith(DONE_BLOB_SUFFIX)) {
      "Completion blob URI must end with $DONE_BLOB_SUFFIX: $doneBlobUri"
    }
    val blobUriPrefix = doneBlobUri.removeSuffix(DONE_BLOB_FILE_NAME)
    val impressionMetadata = mutableListOf<ImpressionMetadata>()
    var pageToken = ""
    do {
      val response =
        try {
          throttler.onReady {
            impressionMetadataStub.listImpressionMetadata(
              listImpressionMetadataRequest {
                parent = dataProviderName
                pageSize = impressionMetadataPageSize
                this.pageToken = pageToken
                showDeleted = false
                filter = listImpressionMetadataFilter {
                  this.blobUriPrefix = blobUriPrefix
                  state = ImpressionMetadata.State.ACTIVE
                }
              }
            )
          }
        } catch (e: StatusException) {
          throw Exception("Error listing active ImpressionMetadata for $blobUriPrefix", e)
        }
      impressionMetadata += response.impressionMetadataList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return impressionMetadata
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
    private const val DONE_BLOB_SUFFIX = "/$DONE_BLOB_FILE_NAME"
  }
}
