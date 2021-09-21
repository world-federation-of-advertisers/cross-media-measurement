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

package org.wfanet.panelmatch.client.common

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.compression.FactoryBasedCompressor
import org.wfanet.panelmatch.common.compression.NoOpCompressor

/**
 * Trivial trainer for [NoOpCompressor].
 *
 * WARNING: since this does no compression, you likely do not want to use it in production.
 */
class UncompressedEventCompressorTrainer : EventCompressorTrainer {
  override val preferredSampleSize: Int = 0

  override fun train(eventsSample: Iterable<ByteString>): FactoryBasedCompressor {
    return FactoryBasedCompressor(ByteString.EMPTY, NoOpCompressorFactory())
  }
}

private class NoOpCompressorFactory : CompressorFactory() {
  override fun build(dictionary: ByteString): Compressor {
    return NoOpCompressor()
  }
}
