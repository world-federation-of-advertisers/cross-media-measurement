/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.rawimpressions

import java.net.URI

/** One registered raw-impression object and the immutable GCS generation it must be read from. */
data class RawImpressionBlob(val blobUri: String, val blobGeneration: Long) {
  val generationMatchedBlobUri: String = generationMatchedBlobUri(blobUri, blobGeneration)
}

/**
 * Encodes [blobGeneration] in a reserved path prefix consumed and removed by
 * [GenerationMatchedGoogleHadoopFileSystem], so the object name sent to GCS remains unchanged.
 *
 * Local-file URIs and relative paths are left unchanged because they have no GCS generation. They
 * are used by local development and unit tests only; production raw-impression URIs use `gs://`.
 */
fun generationMatchedBlobUri(blobUri: String, blobGeneration: Long): String {
  val uri = URI.create(blobUri)
  if (uri.scheme != GCS_SCHEME) return blobUri
  require(blobGeneration > 0) {
    "Raw-impression object $blobUri is missing its Cloud Storage generation"
  }
  require(uri.rawUserInfo == null) {
    "Raw-impression object URI must not contain user-info: $blobUri"
  }
  return "$GCS_SCHEME://${uri.rawAuthority}$GENERATION_PATH_PREFIX$blobGeneration${uri.rawPath}"
}

const val GENERATION_PATH_PREFIX = "/.wfa-generation-match/"
private const val GCS_SCHEME = "gs"
