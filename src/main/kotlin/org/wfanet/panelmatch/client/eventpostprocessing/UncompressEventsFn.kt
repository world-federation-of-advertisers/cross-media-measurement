// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.eventpostprocessing

import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.panelmatch.client.CombinedEvents
import org.wfanet.panelmatch.client.common.plaintextOf
import org.wfanet.panelmatch.client.privatemembership.DecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.Plaintext
import org.wfanet.panelmatch.client.privatemembership.decryptedEventDataSet
import org.wfanet.panelmatch.common.compression.Compressor

/**
 * Decompresses input [DecryptedEventDataSet]s.
 *
 * TODO: consider using different input/output types (e.g. CompressedDecryptedEventDataSet).
 */
class UncompressEventsFn(private val compressorView: PCollectionView<Compressor>) :
  DoFn<DecryptedEventDataSet, DecryptedEventDataSet>() {
  /**
   * The ratio of the size of the uncompressed data to compressed data. This is multiplied by 1000
   * because distributions are over longs.
   */
  private val decompressionRatios =
    Metrics.distribution(this::class.java, "compression-ratios-x1000")

  /** How many events are in each [CombinedEvents]. */
  private val combinedEventsCounts =
    Metrics.distribution(this::class.java, "combined-events-counts")

  /** How many events are in each [DecryptedEventDataSet]. */
  private val eventDataSetEventCounts =
    Metrics.distribution(this::class.java, "event-data-set-event-counts")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val eventSet: DecryptedEventDataSet = context.element()
    val compressor = context.sideInput(compressorView)
    val uncompressedEvents = mutableListOf<Plaintext>()

    for (compressedCombinedEvents in eventSet.decryptedEventDataList) {
      val payload = compressedCombinedEvents.payload
      val decompression = compressor.uncompress(payload)
      if (!compressedCombinedEvents.payload.isEmpty) {
        decompressionRatios.update(1000L * decompression.size() / payload.size())
      }
      val combinedEvents = CombinedEvents.parseFrom(decompression)
      combinedEventsCounts.update(combinedEvents.serializedEventsCount.toLong())
      uncompressedEvents += combinedEvents.serializedEventsList.map(::plaintextOf)
    }

    eventDataSetEventCounts.update(uncompressedEvents.size.toLong())

    context.output(
      decryptedEventDataSet {
        queryId = eventSet.queryId
        decryptedEventData += uncompressedEvents
      }
    )
  }
}
