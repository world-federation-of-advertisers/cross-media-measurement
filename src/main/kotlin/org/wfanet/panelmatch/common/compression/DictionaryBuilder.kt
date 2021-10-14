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

package org.wfanet.panelmatch.common.compression

import com.google.protobuf.ByteString
import java.io.Serializable

/**
 * [Compressor] factory that supports training.
 *
 * This is to enable dictionary-based compression. The output dictionary should be provided to
 * whichever library needs to decompress.
 */
interface DictionaryBuilder : Serializable {
  /** Hint suggesting how many elements should be in the sample. */
  val preferredSampleSize: Int

  /** Factory for the underlying [Compressor]. */
  val factory: CompressorFactory

  /** Builds a dictionary and a [CompressorFactory] that uses it. */
  fun buildDictionary(eventsSample: Iterable<ByteString>): Dictionary
}
