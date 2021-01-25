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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class) // For `runBlockingTest`.
class RenewedFlowTest {
  @Test
  fun `flow repeats`() = runBlockingTest {
    var i = 1
    val result = renewedFlow<Int>(1000, 0) {
      flowOf(i++, i++)
    }.take(6).toList()

    assertEquals(result, listOf(1, 2, 3, 4, 5, 6))
  }

  @Test
  fun `timeout interrupts flow`() = runBlockingTest {
    val result = renewedFlow<Int>(45, 0) {
      flow {
        var i = 1
        // Loop infinitely to show that this is interrupted.
        while (true) {
          delay(10)
          emit(i++)
        }
      }
    }.take(10).toList()

    assertEquals(result, listOf(1, 2, 3, 4, 1, 2, 3, 4, 1, 2))
  }
}
