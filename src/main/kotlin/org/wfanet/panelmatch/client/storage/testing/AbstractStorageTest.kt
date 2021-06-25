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
import org.junit.Test
import org.wfanet.panelmatch.client.storage.Storage
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val KEY = "some arbitrary key"
private val VALUE = ByteString.copyFromUtf8("some arbitrary value")

abstract class AbstractStorageTest {
  abstract val privateStorage: Storage
  abstract val sharedStorage: Storage

  @Test
  fun `write and read FileSystemStorage`() = runBlockingTest {
    privateStorage.write(KEY, VALUE)
    assertThat(privateStorage.read(KEY)).isEqualTo(VALUE)
  }

  @Test
  fun `get error for invalid key from FileSystemStorage`() = runBlockingTest {
    assertFailsWith(IllegalArgumentException::class) { privateStorage.read(KEY) }
  }

  @Test
  fun `get error for rewriting to same key 2x in FileSystemStorage`() = runBlockingTest {
    privateStorage.write(KEY, VALUE)
    assertFailsWith(IllegalArgumentException::class) { privateStorage.write(KEY, VALUE) }
  }

  @Test
  fun `shared storage cannot read from private storage`() = runBlockingTest {
    privateStorage.write(KEY, VALUE)
    privateStorage.read(KEY) // Does not throw.
    assertFailsWith(IllegalArgumentException::class) { sharedStorage.read(KEY) }
  }

  @Test
  fun `private storage cannot read from shared storage`() = runBlockingTest {
    sharedStorage.write(KEY, VALUE)
    sharedStorage.read(KEY) // Does not throw.
    assertFailsWith(IllegalArgumentException::class) { privateStorage.read(KEY) }
  }

  @Test
  fun `same key can be written to private storage and shared storage`() = runBlockingTest {
    sharedStorage.write(KEY, VALUE)
    privateStorage.write(KEY, VALUE) // Does not throw.
  }
}
