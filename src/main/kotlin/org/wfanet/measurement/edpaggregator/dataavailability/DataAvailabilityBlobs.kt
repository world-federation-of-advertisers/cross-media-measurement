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

import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.storage.StorageClient

/**
 * Conventions for blobs in an EDP date folder, shared by [DataAvailabilitySync] (the writer) and
 * [DataAvailabilityMonitor] (the reader).
 *
 * EDP date folders contain a per-date `done` marker and one or more metadata files whose names
 * contain the substring "metadata" (e.g. `metadata.binpb`, `metadata-creative.binpb`). The
 * encrypted impressions blobs that the metadata files reference live in a separate directory tree
 * and are not present here.
 */
object DataAvailabilityBlobs {
  /**
   * GCS custom metadata key written by [DataAvailabilitySync] to signal that the blob has been
   * processed. Written on both:
   * - each scanned metadata blob (used by `DataAvailabilityMonitor`'s late-arrival check — an
   *   unmarked metadata blob in a date whose `done` blob *does* carry the marker means the metadata
   *   blob was rewritten after Sync ran);
   * - the per-date `done` blob (used by `DataAvailabilityMonitor`'s unprocessed-done check — an
   *   unmarked `done` blob older than the threshold means Sync never completed for the date).
   *
   * The marker is content-independent: blob content / `updateTime` is unreliable for distinguishing
   * these cases because Sync's own metadata writes bump `updateTime`.
   */
  const val SYNCED_BY_KEY = "synced-by"

  /** Value written to [SYNCED_BY_KEY] on processed blobs. */
  const val SYNCED_BY_VALUE = "data-availability-sync"

  /** Filename suffix on the per-date "done" marker. */
  private const val DONE_SUFFIX = "/done"

  /** Filename substring that identifies a metadata blob. */
  private const val METADATA_FILE_NAME = "metadata"

  /**
   * Returns true if [blob] is a non-empty metadata blob in an EDP date folder.
   *
   * Matches the filename predicate used by both [DataAvailabilitySync] (when scanning blobs to
   * process) and [DataAvailabilityMonitor] (when scanning for unsynced files): case-insensitive
   * substring match on "metadata", excluding the per-date done marker and zero-byte blobs.
   */
  fun isMetadataBlob(blob: StorageClient.Blob): Boolean {
    if (blob.size == 0L) return false
    if (blob.blobKey.endsWith(DONE_SUFFIX)) return false
    val fileName = blob.blobKey.substringAfterLast("/").lowercase()
    return METADATA_FILE_NAME in fileName
  }

  /** Returns true if [blob] carries the marker written by [DataAvailabilitySync]. */
  fun isSynced(blob: StorageClient.Blob): Boolean = blob.metadata[SYNCED_BY_KEY] == SYNCED_BY_VALUE

  /**
   * Storage-only enumeration of date folders under [prefix]. Classifies each date-named subfolder
   * as either having a `done` blob or not, without doing any threshold/staleness/marker
   * interpretation.
   *
   * Shared by [DataAvailabilitySync] (which uses it for its in-flight gap check) and
   * [DataAvailabilityMonitor] (which layers staleness/late-arrival/unprocessed-done classification
   * on top).
   *
   * @param storageClient client to enumerate against.
   * @param prefix path prefix to enumerate under; typically `"$edpImpressionPath/model-line/$id/"`.
   * @return [EnumeratedDateInfo] with disjoint `datesWithDoneBlob` and `datesWithoutDoneBlob`.
   */
  suspend fun enumerateDateInfo(storageClient: StorageClient, prefix: String): EnumeratedDateInfo {
    val datesWithDoneBlob = mutableMapOf<LocalDate, StorageClient.Blob>()
    val datesWithoutDoneBlob = mutableListOf<LocalDate>()

    val datePrefixes = storageClient.listBlobKeysAndPrefixes(prefix).toList()
    for (datePrefix in datePrefixes) {
      val dateString = datePrefix.removePrefix(prefix).trimEnd('/')
      val date =
        try {
          LocalDate.parse(dateString)
        } catch (e: DateTimeParseException) {
          logger.warning("Skipping non-date folder: $dateString")
          continue
        }
      val doneBlob = storageClient.getBlob("${prefix}$dateString/done")
      if (doneBlob != null) {
        datesWithDoneBlob[date] = doneBlob
      } else {
        datesWithoutDoneBlob.add(date)
      }
    }

    // datesWithDoneBlob and datesWithoutDoneBlob come from mutually exclusive branches above, so
    // they must never overlap. The in-range gating in DataAvailabilitySync relies on this
    // invariant (a date is either finalized or unfinalized, never both), so assert it explicitly.
    val overlap = datesWithDoneBlob.keys.intersect(datesWithoutDoneBlob.toSet())
    check(overlap.isEmpty()) { "A date cannot both have and lack a done blob: $overlap" }

    return EnumeratedDateInfo(
      datesWithDoneBlob = datesWithDoneBlob,
      datesWithoutDoneBlob = datesWithoutDoneBlob.sorted(),
    )
  }

  /**
   * Finds dates that are missing between the first and last dates in [presentDates]. Operates on
   * the *union* of finalized and unfinalized dates so a folder that exists but lacks a `done` blob
   * is not mistaken for a missing date.
   */
  fun findGaps(presentDates: Collection<LocalDate>): List<LocalDate> {
    if (presentDates.size <= 1) return emptyList()
    val sorted = presentDates.sorted()
    val dateSet = sorted.toSet()
    return generateSequence(sorted.first().plusDays(1)) { it.plusDays(1) }
      .takeWhile { it.isBefore(sorted.last()) }
      .filter { it !in dateSet }
      .toList()
  }

  /**
   * Result of [enumerateDateInfo]. Each date is in exactly one of the two collections (enforced by
   * a `check` in the producer).
   *
   * @property datesWithDoneBlob map from date to the `done` blob itself, so callers can read
   *   `createTime` / `metadata` without re-fetching.
   * @property datesWithoutDoneBlob sorted list of date folders that exist but have no `done` blob.
   */
  data class EnumeratedDateInfo(
    val datesWithDoneBlob: Map<LocalDate, StorageClient.Blob>,
    val datesWithoutDoneBlob: List<LocalDate>,
  )

  private val logger: Logger = Logger.getLogger(DataAvailabilityBlobs::class.java.name)
}
