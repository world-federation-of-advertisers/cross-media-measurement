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
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.BlobNotFoundException
import org.wfanet.panelmatch.client.storage.SigningStorageClient
import org.wfanet.panelmatch.client.storage.VerifyingStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val KEY = "some/arbitrary.key"
private val VALUE = "<some-arbitrary-value>".toByteStringUtf8()

abstract class VerifiedStorageClientTest {
  private lateinit var verifyingStorage: VerifyingStorageClient
  private lateinit var signingStorage: SigningStorageClient
  abstract val underlyingClient: StorageClient

  @Before
  fun setUp() {
    verifyingStorage = makeTestVerifyingStorageClient(underlyingClient)
    signingStorage = makeTestSigningStorageClient(underlyingClient)
  }

  @Test
  fun writeThenRead() = runBlockingTest {
    signingStorage.writeBlob(KEY, VALUE)
    assertThat(verifyingStorage.getBlob(KEY).toByteString()).isEqualTo(VALUE)
  }

  @Test
  fun readMissingKeyFails() = runBlockingTest {
    assertFailsWith<BlobNotFoundException> { verifyingStorage.getBlob(KEY) }
  }

  @Test
  fun writeSameKeyTwice() = runBlockingTest {
    signingStorage.writeBlob(KEY, "a-different-value".toByteStringUtf8())
    signingStorage.writeBlob(KEY, VALUE)
    assertThat(verifyingStorage.getBlob(KEY).toByteString()).isEqualTo(VALUE)
  }
}
