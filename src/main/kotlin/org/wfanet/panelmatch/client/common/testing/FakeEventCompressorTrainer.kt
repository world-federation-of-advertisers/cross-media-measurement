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

package org.wfanet.panelmatch.client.common.testing

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.client.common.EventCompressorTrainer
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.compression.FactoryBasedCompressor
import org.wfanet.panelmatch.common.compression.testing.FakeCompressor
import org.wfanet.panelmatch.common.toByteString

class FakeEventCompressorTrainer : EventCompressorTrainer {
  override val preferredSampleSize: Int = 3

  override fun train(eventsSample: Iterable<ByteString>): FactoryBasedCompressor {
    val sortedJoinedSample = eventsSample.map { it.toStringUtf8() }.sorted().joinToString(", ")
    val dictionary = "Dictionary: $sortedJoinedSample".toByteString()
    return FactoryBasedCompressor(dictionary, FakeCompressorFactory())
  }
}

private class FakeCompressorFactory : CompressorFactory() {
  override fun build(dictionary: ByteString): Compressor {
    return FakeCompressor()
  }
}
