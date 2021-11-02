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
import com.google.protobuf.kotlin.toByteStringUtf8
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.compression.Dictionary
import org.wfanet.panelmatch.common.compression.DictionaryBuilder
import org.wfanet.panelmatch.common.compression.dictionary

class FakeDictionaryBuilder : DictionaryBuilder {
  override val preferredSampleSize: Int = 3

  override val factory: CompressorFactory = FakeCompressorFactory()

  override fun buildDictionary(eventsSample: Iterable<ByteString>): Dictionary {
    val sortedJoinedSample = eventsSample.map { it.toStringUtf8() }.sorted().joinToString(", ")
    val dictionary = "Dictionary: $sortedJoinedSample".toByteStringUtf8()
    return dictionary { contents = dictionary }
  }
}
