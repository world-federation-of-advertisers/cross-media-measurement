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

package org.wfanet.panelmatch.common.secrets

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.flowOf
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.common.secrets.testing.AbstractMutableSecretMapTest

private const val BLOB_KEY = "some-blob-key"

@RunWith(JUnit4::class)
class StorageClientSecretMapTest : AbstractMutableSecretMapTest<StorageClientSecretMap>() {
  override suspend fun secretMapOf(vararg items: Pair<String, ByteString>): StorageClientSecretMap {
    val storageClient = InMemoryStorageClient()
    for ((key, value) in items) {
      storageClient.createBlob(key, flowOf(value))
    }

    return StorageClientSecretMap(storageClient)
  }
}
