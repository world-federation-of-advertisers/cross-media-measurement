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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.type.Date
import com.google.type.date
import io.grpc.StatusException
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.service.RankIndexBlobKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest

/**
 * Storage garbage-collection of aged-out `DAY_ONLY` rank-index blobs for a single subpool.
 *
 * Since the cumulative `SNAPSHOT` now carries each fingerprint's `last_seen` recency (see
 * [RankAllocator]), the **rank-freeing** decision is made entirely in heap by
 * [RankAllocator.freeAgedRanks] and no longer depends on these blobs. This class is therefore pure
 * storage cleanup: it soft-deletes the `RankIndexBlob` rows whose `max_event_date` is older than
 * the retention window and deletes their bytes from Cloud Storage. It reads no blob contents; owner
 * lifecycle metadata protects pending labeling inputs.
 *
 * A DAY_ONLY blob is eligible after its owner completes or is evicted nonretryably and its event
 * date is older than the retention window. Unfinished and processing-failed owners retain their
 * labeling inputs. The retention window MUST exceed the deployment's maximum measurement-report
 * window (see Data Deletion).
 *
 * @param rankIndexBlobsStub metadata-storage service for listing/soft-deleting `RankIndexBlob`s.
 * @param rankIndexStore deletes the aged blobs' bytes from Cloud Storage.
 * @param dataProvider the EDP resource name (`dataProviders/{dp}`), parent of the upload wildcard.
 * @param modelLine the model line scoping the retention query.
 * @param retentionDays retention window in days.
 * @param today the UTC date treated as "now" for the age computation.
 * @param rpcThrottlers process-scoped rate limiters for rank-index metadata RPCs.
 */
class SubpoolRetention(
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rankIndexStore: RankIndexStore,
  private val dataProvider: String,
  private val modelLine: String,
  private val retentionDays: Int,
  private val today: LocalDate,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
) {
  /** Soft-deletes and hard-deletes the aged-out `DAY_ONLY` blobs of [poolOffset]. */
  suspend fun deleteAgedBlobs(poolOffset: Long) {
    val cutoff = today.minusDays(retentionDays.toLong() + 1L) // strictly older than RETENTION_DAYS
    val candidates = listDeletionCandidates(poolOffset, cutoff)
    if (candidates.isEmpty()) return

    logger.info(
      "Subpool $poolOffset for $modelLine: garbage-collecting ${candidates.size} aged DAY_ONLY blob(s)"
    )
    val deletionAllowedByUpload = mutableMapOf<String, Boolean>()
    for (candidate in candidates) {
      val key =
        checkNotNull(RankIndexBlobKey.fromName(candidate.name)) {
          "Malformed rank-index resource: ${candidate.name}"
        }
      val upload = key.parentKey.toName()
      check(
        upload.startsWith("$dataProvider/rawImpressionUploads/") &&
          candidate.cmmsModelLine == modelLine &&
          candidate.blobType == RankIndexBlob.BlobType.DAY_ONLY &&
          candidate.poolOffset == poolOffset
      ) {
        "Unexpected rank-index cleanup candidate"
      }
      val allowed =
        deletionAllowedByUpload[upload]
          ?: ownerAllowsDeletion(upload).also { deletionAllowedByUpload[upload] = it }
      if (!allowed) continue
      // Keep the row discoverable until its bytes are gone so a failed storage deletion is retried.
      rankIndexStore.delete(candidate.blobUri)
      rpcThrottlers.metadataWrite.onReady {
        rankIndexBlobsStub.deleteRankIndexBlob(deleteRankIndexBlobRequest { name = candidate.name })
      }
    }
  }

  /** Completed executions and nonretryable evictions no longer need their labeling inputs. */
  private suspend fun ownerAllowsDeletion(upload: String): Boolean {
    val owners = mutableListOf<RawImpressionUploadModelLine>()
    rawImpressionUploadModelLinesStub
      .listResources { pageToken: String ->
        val response =
          try {
            rpcThrottlers.metadataRead.onReady {
              rawImpressionUploadModelLinesStub.listRawImpressionUploadModelLines(
                listRawImpressionUploadModelLinesRequest {
                  parent = upload
                  filter =
                    ListRawImpressionUploadModelLinesRequestKt.filter { cmmsModelLine = modelLine }
                  this.pageToken = pageToken
                }
              )
            }
          } catch (e: StatusException) {
            throw IllegalStateException("Unable to read the rank-index owner $upload", e)
          }
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .collect { page -> owners.addAll(page) }
    val owner =
      checkNotNull(owners.singleOrNull()) {
        "Missing or ambiguous model-line owner for $upload under $modelLine"
      }
    check(
      owner.cmmsModelLine == modelLine &&
        owner.name.startsWith("$upload/rawImpressionUploadModelLines/")
    ) {
      "Unexpected rank-index owner"
    }
    return when (owner.state) {
      RawImpressionUploadModelLine.State.COMPLETED -> true
      RawImpressionUploadModelLine.State.FAILED ->
        owner.failureReason == RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
      RawImpressionUploadModelLine.State.STATE_UNSPECIFIED,
      RawImpressionUploadModelLine.State.CREATED,
      RawImpressionUploadModelLine.State.POOL_ASSIGNING,
      RawImpressionUploadModelLine.State.RANKING,
      RawImpressionUploadModelLine.State.LABELING,
      RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
      RawImpressionUploadModelLine.State.UNRECOGNIZED -> false
    }
  }

  /**
   * `DAY_ONLY` blobs for [poolOffset] whose `max_event_date` is on or before [cutoff], not deleted.
   */
  private suspend fun listDeletionCandidates(
    poolOffset: Long,
    cutoff: LocalDate,
  ): List<RankIndexBlob> {
    val candidates = mutableListOf<RankIndexBlob>()
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            rankIndexBlobsStub.listRankIndexBlobs(
              listRankIndexBlobsRequest {
                parent = "$dataProvider/rawImpressionUploads/${ResourceKey.WILDCARD_ID}"
                filter =
                  ListRankIndexBlobsRequestKt.filter {
                    blobType = RankIndexBlob.BlobType.DAY_ONLY
                    cmmsModelLine = modelLine
                    this.poolOffset = poolOffset
                    maxEventDateOnOrBefore = cutoff.toDate()
                  }
                this.pageToken = pageToken
              }
            )
          }
        ResourceList(response.rankIndexBlobsList, response.nextPageToken)
      }
      .collect { page -> candidates.addAll(page) }
    return candidates
  }

  private fun LocalDate.toDate(): Date = date {
    year = this@toDate.year
    month = this@toDate.monthValue
    day = this@toDate.dayOfMonth
  }

  companion object {
    private val logger = Logger.getLogger(SubpoolRetention::class.java.name)
  }
}
