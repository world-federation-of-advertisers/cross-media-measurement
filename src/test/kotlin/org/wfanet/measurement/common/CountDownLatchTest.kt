// Copyright 2020 The Measurement System Authors
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
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class) // For `runBlockingTest`.
class CountDownLatchTest {
  @Test fun `latch count is equal to initial count`() {
    val latch = CountDownLatch(10)

    assertThat(latch.count).isEqualTo(10)
  }

  @Test fun `countDown decrements count`() {
    val latch = CountDownLatch(10)

    latch.countDown()

    assertThat(latch.count).isEqualTo(9)
  }

  @Test fun `countDown is no-op when current count is zero`() {
    val latch = CountDownLatch(1)

    latch.countDown()
    latch.countDown()

    assertThat(latch.count).isEqualTo(0)
  }

  @Test fun `await suspends until count is zero`() = runBlockingTest {
    val latch = CountDownLatch(10)

    val job = launch { latch.await() }
    assertFalse(job.isCompleted)

    repeat(9) { latch.countDown() }
    assertThat(latch.count).isEqualTo(1)
    assertFalse(job.isCompleted)

    latch.countDown()
    assertThat(latch.count).isEqualTo(0)
    assertTrue(job.isCompleted)
  }

  @Test fun `await resumes immediately when initial count is zero`() = runBlockingTest {
    val latch = CountDownLatch(0)

    val job = launch { latch.await() }
    assertTrue(job.isCompleted)
  }

  @Test fun `await resumes multiple coroutines when count reaches zero`() = runBlockingTest {
    val latch = CountDownLatch(1)

    val job1 = launch { latch.await() }
    val job2 = launch { latch.await() }
    latch.countDown()

    assertTrue(job1.isCompleted)
    assertTrue(job2.isCompleted)
  }
}
