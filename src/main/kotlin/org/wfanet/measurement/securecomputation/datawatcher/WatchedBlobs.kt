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

package org.wfanet.measurement.securecomputation.datawatcher

/**
 * Custom object-metadata keys that [DataWatcher] reads off triggered blobs and forwards to
 * downstream sinks. Producers (e.g. `DataAvailabilitySync`) and the watcher must agree on these
 * names; this object is the single source of truth.
 */
object WatchedBlobs {
  /**
   * GCS custom metadata key carrying the resource name of the `ImpressionMetadata` that a metadata
   * blob corresponds to. Appears as `x-goog-meta-impression-metadata-resource-id` in GCS and is
   * forwarded by [DataWatcher] as the `X-Impression-Metadata-Resource-Id` HTTP header on HTTP-sink
   * dispatches.
   */
  const val IMPRESSION_METADATA_RESOURCE_ID_KEY = "impression-metadata-resource-id"
}
