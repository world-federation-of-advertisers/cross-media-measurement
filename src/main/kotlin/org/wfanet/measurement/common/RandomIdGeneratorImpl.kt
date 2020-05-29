package org.wfanet.measurement.common

import java.time.Clock
import java.util.Random
import kotlin.math.abs

class RandomIdGeneratorImpl(private val clock: Clock) : RandomIdGenerator {
  override fun generateInternalId(): InternalId = InternalId(generateLong())

  override fun generateExternalId(): ExternalId = ExternalId(generateLong())

  private fun generateLong(): Long =
    abs((Random().nextLong() shl 32) or (clock.millis() and 0xFFFFFFFF))
}
