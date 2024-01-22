/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Message
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchConfigKt
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.distribution
import org.wfanet.anysketch.exponentialDistribution
import org.wfanet.anysketch.oracleDistribution
import org.wfanet.anysketch.sketchConfig
import org.wfanet.anysketch.uniformDistribution
import org.wfanet.measurement.api.v2alpha.LiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ReachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.loadtest.config.VidSampling

class SketchGenerator(
  private val eventQuery: EventQuery<Message>,
  private val sketchConfig: SketchConfig,
  private val vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
) {

  /** Generates a [Sketch] for the specified [eventGroupSpecs]. */
  fun generate(eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>): Sketch {
    return with(SketchProtos.toAnySketch(sketchConfig)) {
      for (eventGroupSpec in eventGroupSpecs) {
        populate(eventGroupSpec)
      }
      toSketch()
    }
  }

  private fun AnySketch.populate(eventGroupSpec: EventQuery.EventGroupSpec) {
    eventQuery
      .getUserVirtualIds(eventGroupSpec)
      .filter {
        VidSampling.sampler.vidIsInSamplingBucket(
          it,
          vidSamplingInterval.start,
          vidSamplingInterval.width,
        )
      }
      .forEach { insert(it, mapOf("frequency" to 1L)) }
  }

  private fun AnySketch.toSketch(): Sketch {
    return SketchProtos.fromAnySketch(this, sketchConfig)
  }
}

fun LiquidLegionsSketchParams.toSketchConfig(): SketchConfig {
  return sketchConfig {
    indexes +=
      SketchConfigKt.indexSpec {
        name = "Index"
        distribution = distribution {
          exponential = exponentialDistribution {
            rate = decayRate
            numValues = maxSize
          }
        }
      }
    values +=
      SketchConfigKt.valueSpec {
        name = "SamplingIndicator"
        aggregator = SketchConfig.ValueSpec.Aggregator.UNIQUE
        distribution = distribution {
          uniform = uniformDistribution { numValues = samplingIndicatorSize }
        }
      }

    values +=
      SketchConfigKt.valueSpec {
        name = "Frequency"
        aggregator = SketchConfig.ValueSpec.Aggregator.SUM
        distribution = distribution { oracle = oracleDistribution { key = "frequency" } }
      }
  }
}

fun ReachOnlyLiquidLegionsSketchParams.toSketchConfig(): SketchConfig {
  return sketchConfig {
    indexes +=
      SketchConfigKt.indexSpec {
        name = "Index"
        distribution = distribution {
          exponential = exponentialDistribution {
            rate = decayRate
            numValues = maxSize
          }
        }
      }
  }
}
