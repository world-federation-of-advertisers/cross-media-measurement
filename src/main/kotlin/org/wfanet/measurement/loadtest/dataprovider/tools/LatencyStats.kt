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

package org.wfanet.measurement.loadtest.dataprovider.tools

import com.google.common.math.Quantiles
import com.google.common.math.Stats
import java.io.PrintStream
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

data class LatencyStats(
  val count: Int,
  val mean: Duration,
  val standardDeviation: Duration,
  val p50: Duration,
  val p95: Duration,
) {
  fun print(printStream: PrintStream) {
    with(printStream) {
      println("Latency stats")
      println("Count: $count")
      println("Mean: $mean")
      println("Standard deviation: $standardDeviation")
      println("50th percentile: $p50")
      println("95th percentile: $p95")
    }
  }
}

class LatencyStatsAccumulator {
  private val latenciesMillis = mutableListOf<Double>()

  fun record(value: Duration) {
    latenciesMillis.add(value.inWholeMilliseconds.toDouble())
  }

  fun snapshot(): LatencyStats {
    val latenciesArray = latenciesMillis.toDoubleArray()
    val stats = Stats.of(*latenciesArray)
    val mean: Duration = stats.mean().milliseconds
    val standardDeviation: Duration = stats.populationStandardDeviation().milliseconds
    val p50: Duration =
      Quantiles.percentiles().index(50).computeInPlace(*latenciesArray).milliseconds
    val p95: Duration =
      Quantiles.percentiles().index(95).computeInPlace(*latenciesArray).milliseconds

    return LatencyStats(latenciesArray.size, mean, standardDeviation, p50, p95)
  }
}
