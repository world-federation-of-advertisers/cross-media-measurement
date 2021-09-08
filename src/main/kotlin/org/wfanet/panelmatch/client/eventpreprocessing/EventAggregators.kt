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

package org.wfanet.panelmatch.client.eventpreprocessing

import com.google.protobuf.ByteString
import org.apache.beam.sdk.transforms.Sample
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.panelmatch.client.eventpreprocessing.EventAggregatorTrainer.TrainedEventAggregator
import org.wfanet.panelmatch.common.beam.groupByKey
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.parDoWithSideInput
import org.wfanet.panelmatch.common.beam.values

/**
 * First use [Sample.any] -- which is not guaranteed to be uniform -- to significantly over-sample
 * then use [Sample.fixedSizeGlobally], which is uniform random but inefficient on lots of data.
 *
 * We coarsely sample 10 times as much data as is necessary; this should be adjusted experimentally.
 */
private const val OVERSAMPLING_FACTOR = 10L

/** The results of training an [EventAggregator] and then applying it to a [PCollection]. */
data class AggregatedEvents(
  val events: PCollection<KV<ByteString, ByteString>>,
  val dictionary: PCollectionView<ByteString>
)

/** Trains an [EventAggregator] and applies it to a [PCollection]. */
fun EventAggregatorTrainer.aggregateByKey(
  events: PCollection<KV<ByteString, ByteString>>
): AggregatedEvents {
  val trainedEventAggregator: PCollection<TrainedEventAggregator> =
    events
      .values()
      .apply("Rough Sample", Sample.any(OVERSAMPLING_FACTOR * preferredSampleSize))
      .apply("Uniform Sample", Sample.fixedSizeGlobally(preferredSampleSize))
      .map { train(it) }
      .setCoder(TrainedEventsAggregatorCoder.of())

  val eventAggregatorView =
    trainedEventAggregator.map { it.eventAggregator }.apply(View.asSingleton())

  val aggregatedEvents: PCollection<KV<ByteString, ByteString>> =
    events.groupByKey().parDoWithSideInput(eventAggregatorView) { keyAndEvents, eventAggregator ->
      yield(kvOf(keyAndEvents.key, eventAggregator.combine(keyAndEvents.value)))
    }

  val dictionaryView = trainedEventAggregator.map { it.dictionary }.apply(View.asSingleton())

  return AggregatedEvents(aggregatedEvents, dictionaryView)
}
