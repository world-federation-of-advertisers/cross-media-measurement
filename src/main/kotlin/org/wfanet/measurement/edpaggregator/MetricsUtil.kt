/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator

import io.opentelemetry.api.metrics.DoubleHistogram
import kotlin.time.TimeSource

object MetricsUtil {
  /**
   * Measures the execution time of [block] and records it to this histogram in seconds.
   *
   * @param block the code block to measure.
   * @return the result of the block execution.
   */
  inline fun <T> DoubleHistogram.measured(block: () -> T): T {
    val timer = TimeSource.Monotonic.markNow()
    return try {
      block()
    } finally {
      this.record(timer.elapsedNow().inWholeNanoseconds / 1_000_000_000.0)
    }
  }
}
