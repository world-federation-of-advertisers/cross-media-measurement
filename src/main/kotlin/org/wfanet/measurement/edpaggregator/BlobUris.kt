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

package org.wfanet.measurement.edpaggregator

import java.net.URI
import org.wfanet.measurement.storage.BlobUri

/** Utilities for working with storage [BlobUri]s. */
object BlobUris {
  /** Returns a canonical URI for a GCS object. */
  fun canonicalGcsUri(uri: String): String {
    val parsed = URI.create(uri)
    require(parsed.scheme.equals("gs", ignoreCase = true)) { "blob URI must use gs://" }
    require(!parsed.authority.isNullOrEmpty() && parsed.rawPath.length > 1) {
      "blob URI must include a bucket and object"
    }
    require(parsed.rawQuery == null && parsed.rawFragment == null) {
      "blob URI must not include a query or fragment"
    }
    return "gs://${parsed.authority}${parsed.rawPath}"
  }

  /**
   * Reconstructs the full storage URI for [blobKey] using the scheme and bucket of [blobUri].
   *
   * @throws IllegalArgumentException if [blobUri] uses an unsupported scheme.
   */
  fun buildUri(blobUri: BlobUri, blobKey: String): String =
    when (blobUri.scheme) {
      "gs" -> "${blobUri.scheme}://${blobUri.bucket}/$blobKey"
      "file" -> "${blobUri.scheme}:///${blobUri.bucket}/$blobKey"
      else -> throw IllegalArgumentException("Unsupported scheme: ${blobUri.scheme}")
    }
}
