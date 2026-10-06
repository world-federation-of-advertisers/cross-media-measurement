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

import org.wfanet.measurement.storage.BlobUri

/** Utilities for working with storage [BlobUri]s. */
object BlobUris {
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
