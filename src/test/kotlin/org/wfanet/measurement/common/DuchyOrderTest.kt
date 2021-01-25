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

import kotlin.test.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DuchyOrderTest {
  private val order = DuchyOrder(
    setOf(
      Duchy(BOHEMIA, 200L.toBigInteger()),
      Duchy(SALZBURG, 100L.toBigInteger()),
      Duchy(AUSTRIA, 300L.toBigInteger())
    )
  )

  @Test
  fun `first node first`() {
    assertEquals(listOf(SALZBURG, BOHEMIA, AUSTRIA), order.computationOrder(SHA1_MOD_3_IS_0))
  }

  @Test
  fun `second node first`() {
    assertEquals(listOf(BOHEMIA, AUSTRIA, SALZBURG), order.computationOrder(SHA1_MOD_3_IS_1))
  }

  @Test
  fun `third node first`() {
    assertEquals(listOf(AUSTRIA, SALZBURG, BOHEMIA), order.computationOrder(SHA1_MOD_3_IS_2))
  }

  @Test
  fun `test magic numbers`() {
    assertEquals(0, sha1Mod(SHA1_MOD_3_IS_0, 3.toBigInteger()))
    assertEquals(1, sha1Mod(SHA1_MOD_3_IS_1, 3.toBigInteger()))
    assertEquals(2, sha1Mod(SHA1_MOD_3_IS_2, 3.toBigInteger()))
  }

  companion object {
    private const val AUSTRIA = "Austria"
    private const val BOHEMIA = "Bohemia"
    private const val SALZBURG = "Salzburg"

    // These numbers were created with a bit of guess and check to find ones that matched
    // the desired test cases.
    private const val SHA1_MOD_3_IS_0 = "a"
    private const val SHA1_MOD_3_IS_1 = "c"
    private const val SHA1_MOD_3_IS_2 = "b"
  }
}
