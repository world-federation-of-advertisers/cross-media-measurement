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
   * GCS custom metadata key written on metadata blobs by [DataAvailabilitySync] to signal that the
   * blob has been synced. The monitor uses the presence of this marker to identify which metadata
   * blobs have been processed, rather than using updateTime (which the sync itself bumps after the
   * done blob is written).
   */
  const val SYNCED_BY_KEY = "synced-by"

  /** Value written to [SYNCED_BY_KEY] on processed metadata blobs. */
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
    if (blob.blobKey.endsWith(DONE_SUFFIX)) return false
    val fileName = blob.blobKey.substringAfterLast("/").lowercase()
    return METADATA_FILE_NAME in fileName
  }

  /** Returns true if [blob] carries the marker written by [DataAvailabilitySync]. */
  fun isSynced(blob: StorageClient.Blob): Boolean = blob.metadata[SYNCED_BY_KEY] == SYNCED_BY_VALUE
}
