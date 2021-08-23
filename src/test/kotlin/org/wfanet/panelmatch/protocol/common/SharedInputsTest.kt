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

package org.wfanet.panelmatch.protocol.common

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.wfanet.panelmatch.common.toByteString

class SharedInputsTest {

  @Test
  fun `check Shared Inputs serializer and parser`() {
    val someListOfByteString =
      listOf(
        "some plaintext0".toByteString(),
        "some plaintext1".toByteString(),
        "some plaintext2".toByteString(),
        "some plaintext3".toByteString(),
        "some plaintext4".toByteString()
      )
    assertThat(makeSerializedSharedInputs(someListOfByteString)).isNotEqualTo(someListOfByteString)
    assertThat(parseSerializedSharedInputs(makeSerializedSharedInputs(someListOfByteString)))
      .isEqualTo(someListOfByteString)
  }
}
