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
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.eventpreprocessing.EventCompressorTrainer.TrainedEventCompressor
import org.wfanet.panelmatch.common.compression.BrotliCompressor

/** [EventCompressorTrainer] that uses Brotli compression (via JNI). */
class BrotliEventCompressorTrainer : EventCompressorTrainer {
  // TODO(@efoxepstein): experimentally adjust this value
  override val preferredSampleSize: Int = 1000

  override fun train(eventsSample: Iterable<ByteString>): TrainedEventCompressor {
    val dictionary: ByteString = eventsSample.flatten()

    return TrainedEventCompressor(BrotliCompressor(dictionary), dictionary)
  }
}
