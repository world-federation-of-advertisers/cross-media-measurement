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

package org.wfanet.panelmatch.common.storage

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.lang.IllegalStateException
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val KEY = "some-blob-key"
private const val SAFE_CONTENTS = "0123456789"
private const val UNSAFE_CONTENTS = "A0123456789"

@RunWith(JUnit4::class)
class SizeLimitedStorageClientTest {
  private val delegate = InMemoryStorageClient()
  private val storageClient = SizeLimitedStorageClient(10L, delegate)

  private fun writeBlob(vararg elements: String) = runBlocking {
    val flow = elements.map { it.toByteStringUtf8() }.asFlow()
    storageClient.writeBlob(KEY, flow)
  }

  private fun getBlob(): StorageClient.Blob? = runBlocking { storageClient.getBlob(KEY) }

  private fun getBlobFromDelegate(): StorageClient.Blob? = runBlocking { delegate.getBlob(KEY) }

  private fun assertCreateBlobFails(vararg elements: String) = runBlockingTest {
    assertFails { writeBlob(*elements) }
    assertThat(getBlob()).isNull()
    assertThat(getBlobFromDelegate()).isNull()
  }

  @Test
  fun createBlobEnforcesLimit() {
    assertCreateBlobFails(UNSAFE_CONTENTS)
    assertCreateBlobFails(SAFE_CONTENTS, SAFE_CONTENTS)
  }

  @Test
  fun createUnderLimitWorks() = runBlockingTest {
    writeBlob(SAFE_CONTENTS)

    val blob = getBlob()
    assertThat(blob).isNotNull()

    val delegateBlob = getBlobFromDelegate()
    assertThat(delegateBlob).isNotNull()

    assertThat(blob?.size).isEqualTo(SAFE_CONTENTS.length)
    assertThat(delegateBlob?.size).isEqualTo(SAFE_CONTENTS.length)

    assertThat(blob?.read()?.flatten()?.toStringUtf8()).isEqualTo(SAFE_CONTENTS)
    assertThat(delegateBlob?.read()?.flatten()?.toStringUtf8()).isEqualTo(SAFE_CONTENTS)
  }

  @Test
  fun getBlobFailsForTooLargeBlob() = runBlockingTest {
    delegate.writeBlob(KEY, UNSAFE_CONTENTS.toByteStringUtf8())

    assertFailsWith<IllegalStateException> { storageClient.getBlob(KEY) }
  }

  @Test
  fun deleteIsDelegated() = runBlockingTest {
    writeBlob(SAFE_CONTENTS)
    assertThat(getBlob()).isNotNull()
    assertThat(getBlobFromDelegate()).isNotNull()

    getBlob()?.delete()
    assertThat(getBlob()).isNull()
    assertThat(getBlobFromDelegate()).isNull()
  }
}
