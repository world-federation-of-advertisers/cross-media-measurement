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

import java.io.InputStream
import java.io.OutputStream
import org.apache.beam.sdk.coders.AtomicCoder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder
import org.wfanet.panelmatch.client.eventpreprocessing.EventAggregatorTrainer.TrainedEventAggregator
import org.wfanet.panelmatch.common.beam.kvOf

/** Coder for [TrainedEventAggregator]. */
internal class TrainedEventsAggregatorCoder : AtomicCoder<TrainedEventAggregator>() {
  private val serializableCoder = SerializableCoder.of(EventAggregator::class.java)
  private val byteStringCoder = ByteStringCoder.of()
  private val kvCoder = KvCoder.of(serializableCoder, byteStringCoder)

  override fun encode(value: TrainedEventAggregator, outStream: OutputStream) {
    kvCoder.encode(kvOf(value.eventAggregator, value.dictionary), outStream)
  }

  override fun decode(inStream: InputStream): TrainedEventAggregator {
    val kv = kvCoder.decode(inStream)
    return TrainedEventAggregator(checkNotNull(kv.key), checkNotNull(kv.value))
  }

  companion object {
    fun of(): TrainedEventsAggregatorCoder {
      return TrainedEventsAggregatorCoder()
    }
  }
}
