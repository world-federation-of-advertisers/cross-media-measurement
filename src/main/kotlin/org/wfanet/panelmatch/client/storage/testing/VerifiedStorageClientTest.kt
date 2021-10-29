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
import org.junit.Test
import org.wfanet.panelmatch.client.storage.StorageNotFoundException
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toByteString

private const val KEY = "some/arbitrary.key"
private val VALUE = "<some-arbitrary-value>".toByteString()

abstract class VerifiedStorageClientTest {
  abstract val storage: VerifiedStorageClient

  @Test
  fun writeThenRead() = runBlockingTest {
    storage.createBlob(KEY, VALUE)
    assertThat(storage.getBlob(KEY).toByteString()).isEqualTo(VALUE)
  }

  @Test
  fun readMissingKeyFails() = runBlockingTest {
    assertFailsWith<StorageNotFoundException> { storage.getBlob(KEY) }
  }

  @Test
  fun writeSameKeyTwice() = runBlockingTest {
    storage.createBlob(KEY, "a-different-value".toByteString())
    storage.createBlob(KEY, VALUE)
    assertThat(storage.getBlob(KEY).toByteString()).isEqualTo(VALUE)
  }
}
