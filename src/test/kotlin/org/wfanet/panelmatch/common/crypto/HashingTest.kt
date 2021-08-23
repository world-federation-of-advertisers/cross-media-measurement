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

package org.wfanet.panelmatch.common.crypto

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.toByteString

private val DATA = "some-data-to-hash".toByteString()
private val ALT_DATA = "some-other-data-to-hash".toByteString()

@RunWith(JUnit4::class)
class HashingTest {
  @Test
  fun `hash same data without salt yields same value`() {
    val hashedData1 = hashSha256ToSpace(DATA, 1024)
    val hashedData2 = hashSha256ToSpace(DATA, 1024)
    assertThat(hashedData1).isEqualTo(hashedData2)
    assertThat(hashedData1).isLessThan(1024)
  }

  @Test
  fun `hash different data without salt yields different values`() {
    val hashedData1 = hashSha256ToSpace(DATA, 1024)
    val hashedData2 = hashSha256ToSpace(ALT_DATA, 1024)
    assertThat(hashedData1).isNotEqualTo(hashedData2)
    assertThat(hashedData1).isLessThan(1024)
    assertThat(hashedData2).isLessThan(1024)
  }

  @Test
  fun `hash data with small range`() {
    val hashedData = hashSha256ToSpace(DATA, 2)
    assertThat(hashedData).isLessThan(2)
  }
}
