/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.protobuf.Message
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.util.logging.Logger
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

/**
 * Frequency vector sink that receives filtered events and builds frequency vectors.
 *
 * Each sink corresponds to a specific filter specification and maintains its own frequency vector.
 * Thread-safe for concurrent access. Also tracks total uncapped impressions for direct
 * measurements.
 */
class FrequencyVectorSink<T : Message>(
  private val filterProcessor: FilterProcessor<T>,
  private val frequencyVector: StripedByteFrequencyVector,
  private val vidIndexMap: VidIndexMap,
) {
  private val zeroVidAttributes =
    Attributes.of(FILTER_SPEC_ATTR, filterProcessor.filterSpec.toString())
  private val zeroVidCounter =
    Instrumentation.meter
      .counterBuilder("edpa.results_fulfiller.zero_vids_skipped")
      .setDescription("Count of events skipped due to zero VID")
      .build()

  /**
   * Processes a batch of events and updates frequency vector for matched events.
   *
   * @param batch
   */
  fun processBatch(batch: EventBatch<T>) {
    filterProcessor.processBatch(batch).events.forEach { event ->
      if (event.vid == 0L) {
        zeroVidCounter.add(1, zeroVidAttributes)
        logger.warning(
          "Skipping event with zero VID for filterSpec=${filterProcessor.filterSpec.toString()}"
        )
        return@forEach
      }
      val index = vidIndexMap[event.vid]
      frequencyVector.increment(index)
    }
  }

  /** Returns the filter spec */
  fun getFilterSpec(): FilterSpec {
    return filterProcessor.filterSpec
  }

  /** Returns the frequency vector. */
  fun getFrequencyVector(): StripedByteFrequencyVector {
    return frequencyVector
  }

  /**
   * Returns the total count of impressions without any frequency capping applied.
   *
   * This is useful for direct measurement fulfillment when frequency_cap_per_user == -1, indicating
   * no frequency caps should be applied.
   */
  fun getTotalUncappedImpressions(): Long {
    return frequencyVector.getTotalUncappedImpressions()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(FrequencyVectorSink::class.java.name)
    private val FILTER_SPEC_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.results_fulfiller.filter_spec")
  }
}
