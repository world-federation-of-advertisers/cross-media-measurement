package org.wfanet.measurement.common

import kotlin.test.assertEquals
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
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
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
