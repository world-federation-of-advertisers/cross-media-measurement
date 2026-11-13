// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(JUnit4::class)
class MillWorkLoopTest {
  @Test
  fun `run drains available work without polling delays`() = runTest {
    val claimTimes = mutableListOf<Long>()
    backgroundScope.launch {
      MillWorkLoop.run(POLLING_INTERVAL) {
        claimTimes += testScheduler.currentTime
        claimTimes.size <= 100
      }
    }

    runCurrent()

    assertThat(claimTimes).containsExactlyElementsIn(List(101) { 0L }).inOrder()
    advanceTimeBy(5_000)
    runCurrent()
    assertThat(claimTimes.last()).isEqualTo(5_000L)
    assertThat(claimTimes).hasSize(102)
  }

  @Test
  fun `run waits after every empty claim`() = runTest {
    val claimTimes = mutableListOf<Long>()
    backgroundScope.launch {
      MillWorkLoop.run(POLLING_INTERVAL) {
        claimTimes += testScheduler.currentTime
        false
      }
    }

    runCurrent()
    advanceTimeBy(4_999)
    runCurrent()
    assertThat(claimTimes).containsExactly(0L)

    advanceTimeBy(1)
    runCurrent()
    advanceTimeBy(5_000)
    runCurrent()
    assertThat(claimTimes).containsExactly(0L, 5_000L, 10_000L).inOrder()
  }

  @Test
  fun `run waits after an exception and resumes draining work`() = runTest {
    val claimTimes = mutableListOf<Long>()
    backgroundScope.launch {
      MillWorkLoop.run(POLLING_INTERVAL) {
        claimTimes += testScheduler.currentTime
        when (claimTimes.size) {
          1 -> throw IllegalStateException("Computations service unavailable")
          2 -> true
          else -> false
        }
      }
    }

    runCurrent()
    advanceTimeBy(4_999)
    runCurrent()
    assertThat(claimTimes).containsExactly(0L)

    advanceTimeBy(1)
    runCurrent()
    assertThat(claimTimes).containsExactly(0L, 5_000L, 5_000L).inOrder()
  }

  @Test
  fun `run propagates cancellation from a claim`() = runTest {
    var claims = 0
    val job =
      backgroundScope.launch {
        MillWorkLoop.run(POLLING_INTERVAL) {
          claims++
          throw CancellationException("Stopping mill")
        }
      }

    job.join()
    advanceTimeBy(5_000)
    runCurrent()

    assertThat(job.isCancelled).isTrue()
    assertThat(claims).isEqualTo(1)
  }

  @Test
  fun `run yields between successful claims so it can be cancelled`() = runTest {
    var claims = 0
    val job =
      backgroundScope.launch {
        MillWorkLoop.run(POLLING_INTERVAL) {
          claims++
          true
        }
      }
    backgroundScope.launch { job.cancel() }

    runCurrent()

    assertThat(job.isCancelled).isTrue()
    assertThat(claims).isEqualTo(1)
  }

  @Test
  fun `run rejects nonpositive polling intervals`() = runTest {
    assertFailsWith<IllegalArgumentException> { MillWorkLoop.run(Duration.ZERO) { false } }
    assertFailsWith<IllegalArgumentException> { MillWorkLoop.run(Duration.ofSeconds(-1)) { false } }
  }

  companion object {
    private val POLLING_INTERVAL = Duration.ofSeconds(5)
  }
}
