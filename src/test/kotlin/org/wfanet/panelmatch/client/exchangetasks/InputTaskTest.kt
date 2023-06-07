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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.testing.AlwaysReadyThrottler
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class InputTaskTest {
  private val storage = mock<StorageClient>()

  @Test
  fun `wait on input`() = runBlockingTest {
    val blobKey = "mp-crypto-key"
    val task = InputTask(blobKey, AlwaysReadyThrottler, storage)

    whenever(storage.getBlob(any()))
      .thenReturn(null)
      .thenReturn(null)
      .thenReturn(null)
      .thenReturn(null)
      .thenReturn(mock())

    val result: Map<String, Flow<ByteString>> = task.execute(emptyMap())

    assertThat(result).isEmpty()

    verify(storage, times(5)).getBlob(blobKey)
    verifyNoMoreInteractions(storage)
  }
}
