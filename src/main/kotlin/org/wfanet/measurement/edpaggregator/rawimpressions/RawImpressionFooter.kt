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

package org.wfanet.measurement.edpaggregator.rawimpressions

import java.time.LocalDate
import org.wfanet.measurement.storage.ParquetStorageClient

/**
 * Reads a raw-impression file's UTC event date from its plaintext Parquet footer.
 *
 * Reuses [FileEntityKeys.parseEventDate] so the footer key name and date parsing (including the
 * friendly error messages) live in one place. Parsing only the date — rather than the full
 * [FileEntityKeys] — keeps this path from coupling to `event_group_reference_id` presence, which it
 * does not need. The footer key-value metadata is plaintext (PLAINTEXT_FOOTER mode), so no KMS/DEK
 * is needed and only the footer bytes are fetched (a tail range read, not the whole file).
 */
suspend fun readEventDateFromFooter(
  parquetStorageClient: ParquetStorageClient,
  blobKey: String,
): LocalDate {
  val blob =
    parquetStorageClient.getBlob(blobKey) ?: error("Raw-impression blob not found: $blobKey")
  return FileEntityKeys.parseEventDate(blob.readKeyValueMetadata())
}
