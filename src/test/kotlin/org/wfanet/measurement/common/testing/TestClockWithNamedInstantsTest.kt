package org.wfanet.measurement.common.testing

import java.lang.IllegalArgumentException
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TestClockWithNamedInstantsTest {

  @Test
  fun `error when reusing a name`() {
    val clock = TestClockWithNamedInstants(Instant.now())
    clock.tickSeconds("name")
    assertFailsWith<IllegalArgumentException> { clock.tickSeconds("name") }
  }

  @Test
  fun `error when seconds is not greater than zero`() {
    val clock = TestClockWithNamedInstants(Instant.now())
    assertFailsWith<IllegalArgumentException> { clock.tickSeconds("name", seconds = 0) }
    assertFailsWith<IllegalArgumentException> { clock.tickSeconds("name", seconds = -1) }
  }

  @Test
  fun `clock instant is last inserted`() {
    val instant = Instant.ofEpochSecond(100)
    val instantPlus20 = instant.plusSeconds(20)
    val instantPlus40 = instant.plusSeconds(40)
    val clock = TestClockWithNamedInstants(instant)
    assertEquals(instant, clock.last())
    assertEquals(clock.instant(), clock.last())
    clock.tickSeconds("plus20", 20)
    assertEquals(instantPlus20, clock.last())
    assertEquals(clock.instant(), clock.last())
    // The value is incremented from the last insert, so plus40 is same as instant.plusSeconds(40)
    clock.tickSeconds("plus40", 20)
    assertEquals(clock.instant(), clock.last())
    assertEquals(instantPlus40, clock.last())
    assertEquals(instant, clock["start"])
    assertEquals(instantPlus20, clock["plus20"])
    assertEquals(instantPlus40, clock["plus40"])
  }
}
