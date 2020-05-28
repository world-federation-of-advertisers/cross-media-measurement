package org.wfanet.measurement.common

import java.time.Clock
import java.util.Random
import kotlin.math.abs

class RandomIdGeneratorImpl(val clock: Clock) : RandomIdGenerator {
  override fun generate(): Long =
    abs((Random().nextLong() shl 32) or (clock.millis() and 0xFFFFFFFF))
}
