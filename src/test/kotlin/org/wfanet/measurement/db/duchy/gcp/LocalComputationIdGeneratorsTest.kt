package org.wfanet.measurement.db.duchy.gcp

import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class LocalComputationIdGeneratorsTest {
  @Test
  fun `gen local id`() {
    val testTime = 0x0FFFFFFF_1230ABCD
    val gen = HalfOfGlobalBitsAndTimeStampIdGenerator(
      Clock.fixed(Instant.ofEpochMilli(testTime), ZoneId.systemDefault())
    )
    val globalId = 0x778899F5_FFFFFFFF
    assertEquals(
      0x1230ABCD_778899F5,
      gen.localId(globalId)
    )
  }
}
