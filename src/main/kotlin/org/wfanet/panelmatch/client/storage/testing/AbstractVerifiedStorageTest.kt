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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.reduce
import org.junit.Test
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.panelmatch.client.storage.StorageNotFoundException
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toByteString

private const val KEY = "some arbitrary key"
private val VALUE = "some arbitrary value".toByteString()

abstract class AbstractVerifiedStorageTest {
  abstract val privateStorage: VerifiedStorageClient
  abstract val sharedStorage: VerifiedStorageClient

  @Test
  fun `write and read Storage`() = runBlockingTest {
    privateStorage.createBlob(KEY, VALUE)
    assertThat(privateStorage.getBlob(KEY).toByteString()).isEqualTo(VALUE)
  }

  @Test
  fun `get error for invalid key from Storage`() = runBlockingTest {
    assertFailsWith<StorageNotFoundException> { privateStorage.getBlob(KEY) }
  }

  @Test
  fun `get error for rewriting to same key 2x in Storage`() = runBlockingTest {
    assertFailsWith<IllegalArgumentException> {
      privateStorage.verifiedBatchWrite(
        mapOf("a" to KEY, "b" to KEY),
        mapOf(KEY to VALUE.asBufferedFlow(1024))
      )
    }
  }

  @Test
  fun `shared storage cannot read from private storage`() = runBlockingTest {
    privateStorage.createBlob(KEY, VALUE.asBufferedFlow(1024))
    privateStorage.getBlob(KEY).read(1024).reduce { a, b -> a.concat(b) } // Does not throw.
    assertFailsWith<StorageNotFoundException> { sharedStorage.getBlob(KEY) }
  }

  @Test
  fun `private storage cannot read from shared storage`() = runBlockingTest {
    sharedStorage.createBlob(KEY, VALUE.asBufferedFlow(1024))
    sharedStorage.getBlob(KEY).read(1024).reduce { a, b -> a.concat(b) } // Does not throw.
    assertFailsWith<StorageNotFoundException> { privateStorage.getBlob(KEY) }
  }

  @Test
  fun `same key can be written to private storage and shared storage`() = runBlockingTest {
    sharedStorage.createBlob(KEY, VALUE)
    privateStorage.createBlob(KEY, VALUE) // Does not throw.
  }
}
