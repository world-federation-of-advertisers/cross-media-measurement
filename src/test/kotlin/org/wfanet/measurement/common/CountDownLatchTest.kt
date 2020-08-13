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

    repeat(9) { latch.countDown()}
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
