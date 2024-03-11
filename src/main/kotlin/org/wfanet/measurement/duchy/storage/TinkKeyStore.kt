// Copyright 2024 The Cross-Media Measurement Authors
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

import org.wfanet.measurement.common.crypto.KeyBlobStore
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.storage.StorageClient

private const val BLOB_KEY_PREFIX = "tink-private-key"

class TinkKeyStore(storageClient: StorageClient) : KeyBlobStore(storageClient) {
  override val blobKeyPrefix: String = BLOB_KEY_PREFIX

  override fun deriveBlobKey(context: PrivateKeyStore.KeyId): String = context.blobKey
}
