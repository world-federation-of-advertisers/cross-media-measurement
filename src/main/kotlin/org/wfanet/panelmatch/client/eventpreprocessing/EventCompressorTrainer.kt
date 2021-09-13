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
import java.io.Serializable
import org.wfanet.panelmatch.common.compression.Compressor

/**
 * [Compressor] factory that supports training.
 *
 * This is to enable dictionary-based compression. The output dictionary should be provided to
 * whichever library needs to decompress.
 */
interface EventCompressorTrainer : Serializable {
  data class TrainedEventCompressor(val compressor: Compressor, val dictionary: ByteString)

  /** Hint suggesting how many elements should be in the sample. */
  val preferredSampleSize: Int

  /** Builds a dictionary and an [Compressor] that uses it. */
  fun train(eventsSample: Iterable<ByteString>): TrainedEventCompressor
}
