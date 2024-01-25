// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.storage

import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.Store

private const val BLOB_KEY_PREFIX = "requisitions"

/** A [Store] instance for managing Blobs associated with requisitions in the Duchy. */
class RequisitionStore(storageClient: StorageClient) :
  Store<RequisitionBlobContext>(storageClient) {

  override val blobKeyPrefix = BLOB_KEY_PREFIX

  override fun deriveBlobKey(context: RequisitionBlobContext): String = context.blobKey
}

/** The context from which a blob key is derived for [RequisitionStore]. */
data class RequisitionBlobContext(
  private val globalComputationId: String,
  private val externalRequisitionId: String,
) {
  val blobKey: String
    get() = "$globalComputationId/$externalRequisitionId"
}
