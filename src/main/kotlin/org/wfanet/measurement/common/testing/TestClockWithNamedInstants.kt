package org.wfanet.measurement.common.testing

import java.time.Clock
import java.time.Instant
import java.time.ZoneId

/**
 * An implementation of [Clock] that returns a predetermined underlying [Instant] which may
 * be changed. Each change of the [Instant] with this [Clock] is named so it can be referred
 * to in a test.
 */
class TestClockWithNamedInstants(start: Instant) : Clock() {
  private val ticks = linkedMapOf("start" to start)
  override fun instant(): Instant = last()
  operator fun get(name: String): Instant = ticks[name] ?: error("No named test instant for $name")
  fun last() = ticks.toList().last().second

  /**
   * Adds a tick event to the clock and ticks the clock forward by some fixed amount of seconds.
   */
  fun tickSeconds(name: String, seconds: Long = 1) {
    require(seconds > 0) { "Must tick by positive seconds" }
    require(name !in ticks) { "May not reuse name of instant $name" }
    ticks[name] = last().plusSeconds(seconds)
  }

  override fun withZone(zone: ZoneId?): Clock = error("Not implemented")
  override fun getZone() = error("Not implemented")
}
