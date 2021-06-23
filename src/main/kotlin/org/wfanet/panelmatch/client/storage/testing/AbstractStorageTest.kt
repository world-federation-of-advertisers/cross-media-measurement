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

package org.wfanet.panelmatch.client.storage.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.panelmatch.client.storage.Storage

abstract class AbstractStorageTest {
  abstract val privateStorage: Storage
  abstract val sharedStorage: Storage
  @Test
  fun `write and read FileSystemStorage`() = runBlocking {
    val valueToStore = ByteString.copyFromUtf8("random-edp-string-0")
    val key = java.util.UUID.randomUUID().toString()
    privateStorage.write(key, valueToStore)
    assertThat(privateStorage.read(key)).isEqualTo(valueToStore)
  }

  @Test
  fun `get error for invalid key from FileSystemStorage`() = runBlocking {
    val valueToStore = ByteString.copyFromUtf8("random-edp-string-0")
    val key = java.util.UUID.randomUUID().toString()
    val reencryptException =
      assertFailsWith(IllegalArgumentException::class) { privateStorage.read(key) }
  }

  @Test
  fun `get error for rewriting to same key 2x in FileSystemStorage`() = runBlocking {
    val valueToStore1 = ByteString.copyFromUtf8("random-edp-string-1")
    val valueToStore2 = ByteString.copyFromUtf8("random-edp-string-2")
    val key = java.util.UUID.randomUUID().toString()
    privateStorage.write(key, valueToStore1)
    val doubleWriteException =
      assertFailsWith(IllegalArgumentException::class) { privateStorage.write(key, valueToStore1) }
  }

  @Test
  fun `read to one Storage Type and make sure you cannot read it from another Storage Type`() =
      runBlocking {
    val valueToStore1 = ByteString.copyFromUtf8("random-edp-string-1")
    val valueToStore2 = ByteString.copyFromUtf8("random-edp-string-2")
    val key = java.util.UUID.randomUUID().toString()
    privateStorage.write(key, valueToStore1)
    val storedValue = privateStorage.read(key)
    val doubleWriteException =
      assertFailsWith(IllegalArgumentException::class) { sharedStorage.read(key) }
  }
}
