// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.storage

import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.Store

private const val BLOB_KEY_PREFIX = "sketches"

/** A [Store] instance for managing Blobs associated with sketches (for test purpose only). */
class SketchStore(storageClient: StorageClient) : Store<Requisition>(storageClient) {
  override val blobKeyPrefix = BLOB_KEY_PREFIX

  override fun deriveBlobKey(context: Requisition): String {
    return context.name
  }
}
