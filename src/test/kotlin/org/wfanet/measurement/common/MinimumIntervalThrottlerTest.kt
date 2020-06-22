package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runBlockingTest
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnit4::class)
class MinimumIntervalThrottlerTest {
  @Test
  fun attempt() {
    val clock = TestClockWithNamedInstants(Instant.ofEpochSecond(12345))
    val throttler = MinimumIntervalThrottler(clock, Duration.ofSeconds(10))

    assertTrue(throttler.attempt(), "Attempt at ${clock.instant()}")

    clock.tickSeconds("too early 1", 3) // 3 seconds since event
    assertFalse(throttler.attempt(), "Attempt at ${clock.instant()}")

    clock.tickSeconds("too early 2", 3) // 6 seconds since event
    assertFalse(throttler.attempt(), "Attempt at ${clock.instant()}")

    clock.tickSeconds("acceptable time", 5) // 11 seconds since event
    assertTrue(throttler.attempt(), "Attempt at ${clock.instant()}")

    clock.tickSeconds("too early", 9) // 9 seconds since event
    assertFalse(throttler.attempt(), "Attempt at ${clock.instant()}")

    repeat(5) { i ->
      clock.tickSeconds("acceptable time ${i + 2}", 11) // 11 seconds since event
      assertTrue(throttler.attempt(), "Attempt at ${clock.instant()}")
    }
  }

  @Test
  fun onReady() = runBlocking<Unit> {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(3))
    assertTrue(throttler.attempt())

    val latch = CountDownLatch(1)

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    withTimeout(Duration.ofSeconds(4).toMillis()) {
      runBlockingTest {
        throttler.onReady { latch.countDown() }
      }
    }

    assertEquals(latch.count, 0)
  }

  @Test
  fun `onReady blocks attempt`() = runBlocking<Unit> {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1))

    val m1 = Mutex(locked = true)
    val m2 = Mutex(locked = true)

    val job = launch { throttler.onReady { m2.unlock(); m1.lock() } }

    // Block until job should have acquired a lock over the throttler.
    m2.lock()

    // Repeatedly ensure that we can't attempt on the throttler.
    repeat(10) {
      delay(2)  // To ensure the throttler could be ready.
      assertFalse(throttler.attempt())
    }

    // Allow job to finish and check that attempt now works.
    m1.unlock()
    job.join()
    assertTrue(throttler.attempt())
  }

  @Test
  @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
  fun fifo() = runBlockingTest {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1))

    val order = mutableListOf<String>()

    val m = Mutex(locked = true)

    val job1 = launch { delay(200); throttler.onReady { order.add("job1") } }
    val job2 = launch { delay(100); throttler.onReady { order.add("job2") } }
    val job3 = launch { throttler.onReady { m.withLock { order.add("job3") } } }

    delay(1000)  // To ensure all jobs are running and blocked on something.
    m.unlock()

    job1.join()
    job2.join()
    job3.join()

    assertThat(order)
      .containsExactly("job3", "job2", "job1")
      .inOrder()
  }
}
