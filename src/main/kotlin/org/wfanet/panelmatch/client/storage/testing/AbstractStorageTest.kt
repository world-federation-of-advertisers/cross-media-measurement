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
import kotlinx.coroutines.flow.reduce
import org.junit.Test
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageNotFoundException
import org.wfanet.panelmatch.client.storage.verifiedRead
import org.wfanet.panelmatch.client.storage.verifiedWrite
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val KEY = "some arbitrary key"
private val VALUE = ByteString.copyFromUtf8("some arbitrary value")

abstract class AbstractStorageTest {
  abstract val privateStorage: StorageClient
  abstract val sharedStorage: StorageClient

  @Test
  fun `write and read FileSystemStorage`() = runBlockingTest {
    privateStorage.verifiedWrite(KEY, VALUE.asBufferedFlow(1024))
    assertThat(privateStorage.verifiedRead(KEY).read(1024).reduce { a, b -> a.concat(b) })
      .isEqualTo(VALUE)
  }

  @Test
  fun `get error for invalid key from FileSystemStorage`() = runBlockingTest {
    assertFailsWith<StorageNotFoundException> { privateStorage.verifiedRead(KEY) }
  }

  @Test
  fun `get error for rewriting to same key 2x in FileSystemStorage`() = runBlockingTest {
    privateStorage.verifiedWrite(KEY, VALUE.asBufferedFlow(1024))
    assertFailsWith<IllegalArgumentException> {
      privateStorage.verifiedWrite(KEY, VALUE.asBufferedFlow(1024))
    }
  }

  @Test
  fun `shared storage cannot read from private storage`() = runBlockingTest {
    privateStorage.verifiedWrite(KEY, VALUE.asBufferedFlow(1024))
    privateStorage.verifiedRead(KEY).read(1024).reduce { a, b -> a.concat(b) } // Does not throw.
    assertFailsWith<StorageNotFoundException> { sharedStorage.verifiedRead(KEY) }
  }

  @Test
  fun `private storage cannot read from shared storage`() = runBlockingTest {
    sharedStorage.verifiedWrite(KEY, VALUE.asBufferedFlow(1024))
    sharedStorage.verifiedRead(KEY).read(1024).reduce { a, b -> a.concat(b) } // Does not throw.
    assertFailsWith<StorageNotFoundException> { privateStorage.verifiedRead(KEY) }
  }

  @Test
  fun `same key can be written to private storage and shared storage`() = runBlockingTest {
    sharedStorage.verifiedWrite(KEY, VALUE.asBufferedFlow(1024))
    privateStorage.verifiedWrite(KEY, VALUE.asBufferedFlow(1024)) // Does not throw.
  }
}
