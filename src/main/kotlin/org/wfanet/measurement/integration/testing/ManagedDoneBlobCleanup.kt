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

package org.wfanet.measurement.integration.testing

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import java.time.LocalDate

/**
 * Removes managed done markers in a fixture's model-line directories before its owners are reset.
 */
class ManagedDoneBlobCleanup(
  private val storage: Storage,
  private val bucket: String,
  impressionsPrefix: String,
  dates: Set<LocalDate>,
) {
  private val prefix: String = impressionsPrefix.trimEnd('/')
  private val markerPattern: Regex

  init {
    require(prefix.isNotEmpty()) { "Impressions prefix must not be empty" }
    require(dates.isNotEmpty()) { "Fixture dates must not be empty" }
    val datePattern = dates.joinToString("|") { Regex.escape(it.toString()) }
    markerPattern =
      Regex("${Regex.escape(prefix)}/model-line/[^/]+/($datePattern)/done-[0-9a-f]{64}")
  }

  fun delete() {
    for (blob in
      storage.list(bucket, Storage.BlobListOption.prefix("$prefix/model-line/")).iterateAll()) {
      if (!markerPattern.matches(blob.name)) continue
      // A rewrite after listing must fail the cleanup before its metadata owners are removed.
      storage.delete(
        BlobId.of(bucket, blob.name),
        Storage.BlobSourceOption.generationMatch(checkNotNull(blob.generation)),
      )
    }
  }
}
