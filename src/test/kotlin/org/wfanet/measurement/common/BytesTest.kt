// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFails
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class) // For `runBlockingTest`.
class BytesTest {
  @Test
  fun `ByteString asBufferedFlow with non-full last part`() = runBlockingTest {
    val flow = ByteString.copyFromUtf8("Hello World").asBufferedFlow(3)
    assertThat(flow.map { it.toStringUtf8() }.toList())
      .containsExactly("Hel", "lo ", "Wor", "ld")
      .inOrder()
  }

  @Test
  fun `ByteString asBufferedFlow on empty ByteString`() = runBlockingTest {
    val flow = ByteString.copyFromUtf8("").asBufferedFlow(3)
    assertThat(flow.toList()).isEmpty()
  }

  @Test
  fun `ByteString asBufferedFlow with invalid buffer size`() = runBlockingTest {
    assertFails {
      ByteString.copyFromUtf8("this should throw").asBufferedFlow(0).toList()
    }
  }
}
