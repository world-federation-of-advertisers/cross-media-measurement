// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator

import java.io.File

/**
 * Configuration for storage clients.
 *
 * @property rootDirectory local filesystem root, for `file://`-backed clients (tests).
 * @property projectId GCS billing/auth project, for `gs://`-backed clients.
 * @property blobPrefix the store's `gs://`/`file://` blob prefix; its bucket roots a multi-key
 *   client (see [BaseVidLabelingTeeAppRunner.buildStorageClient]). Null for single-blob/Parquet
 *   uses that resolve absolute URIs on their own.
 */
data class StorageConfig(
  val rootDirectory: File? = null,
  val projectId: String? = null,
  val blobPrefix: String? = null,
)
