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

import com.google.common.base.Stopwatch
import com.google.protobuf.ByteString
import java.util.concurrent.TimeUnit
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.wfanet.panelmatch.client.PreprocessEventsRequest
import org.wfanet.panelmatch.client.PreprocessEventsResponse

/**
 * Takes in a MutableList<KV<ByteString,ByteString>>, packs them into PreprocessedEventRequest
 * protos, encrypts the identifier and event data using several encryption schemes, and unpacks them
 * from PreprocessedEventResponse protos and emits them as KV<Long,ByteString> pairs
 */
class EncryptionEventsDoFn(
  private val encryptEvents:
    SerializableFunction<PreprocessEventsRequest, PreprocessEventsResponse>,
  private val getIdentifierHashPepper: SerializableFunction<Void?, ByteString>,
  private val getHkdfPepper: SerializableFunction<Void?, ByteString>,
  private val getCryptoKey: SerializableFunction<Void?, ByteString>,
) : DoFn<MutableList<KV<ByteString, ByteString>>, KV<Long, ByteString>>() {
  private val jniCallTimeDistribution =
    Metrics.distribution(BatchingDoFn::class.java, "jni-call-time-micros")

  @ProcessElement
  fun process(c: ProcessContext) {
    val list: MutableList<KV<ByteString, ByteString>> = c.element()
    val request =
      PreprocessEventsRequest.newBuilder()
        .apply {
          cryptoKey = getCryptoKey.apply(null as Void?)
          identifierHashPepper = getIdentifierHashPepper.apply(null as Void?)
          hkdfPepper = getHkdfPepper.apply(null as Void?)
          for (pair in list) {
            addUnprocessedEventsBuilder().apply {
              id = pair.key
              data = pair.value
            }
          }
        }
        .build()
    val stopWatch: Stopwatch = Stopwatch.createStarted()
    val response: PreprocessEventsResponse = encryptEvents.apply(request)
    stopWatch.stop()
    jniCallTimeDistribution.update(stopWatch.elapsed(TimeUnit.MICROSECONDS))

    for (events in response.processedEventsList) {
      c.output(KV.of(events.encryptedId, events.encryptedData))
    }
  }
}
