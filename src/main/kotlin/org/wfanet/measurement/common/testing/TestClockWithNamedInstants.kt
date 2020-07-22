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
