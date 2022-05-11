// Copyright 2022 The Cross-Media Measurement Authors
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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.flowOf
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class BlobsTest {

  private val mockBlob: StorageClient.Blob = mock { blob ->
    whenever(blob.read())
      .thenReturn(
        flowOf("cat".toByteStringUtf8(), "dog".toByteStringUtf8(), "fox".toByteStringUtf8())
      )
  }

  @Test
  fun `toByteString concatenates blob contents`() = runBlockingTest {
    assertThat(mockBlob.toByteString()).isEqualTo("catdogfox".toByteStringUtf8())
  }

  @Test
  fun `toStringUtf8 concatenates blob contents`() = runBlockingTest {
    assertThat(mockBlob.toStringUtf8()).isEqualTo("catdogfox")
  }

  @Test
  fun `newInputStream reads entire blob`() = runBlockingTest {
    val inputStream = mockBlob.newInputStream(this)
    val byteString = ByteString.readFrom(inputStream)

    assertThat(byteString).isEqualTo("catdogfox".toByteStringUtf8())
  }
}
