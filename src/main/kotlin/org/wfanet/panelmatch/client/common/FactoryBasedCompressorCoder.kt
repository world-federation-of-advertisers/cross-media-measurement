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

import java.io.InputStream
import java.io.OutputStream
import org.apache.beam.sdk.coders.AtomicCoder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.compression.FactoryBasedCompressor

/** Apache Beam Coder for [FactoryBasedCompressor]. */
class FactoryBasedCompressorCoder : AtomicCoder<FactoryBasedCompressor>() {
  private val byteStringCoder = ByteStringCoder.of()
  private val serializableCoder = SerializableCoder.of(CompressorFactory::class.java)
  private val kvCoder = KvCoder.of(byteStringCoder, serializableCoder)

  override fun encode(value: FactoryBasedCompressor, outStream: OutputStream) {
    kvCoder.encode(kvOf(value.dictionary, value.factory), outStream)
  }

  override fun decode(inStream: InputStream): FactoryBasedCompressor {
    val kv = kvCoder.decode(inStream)
    return FactoryBasedCompressor(checkNotNull(kv.key), checkNotNull(kv.value))
  }

  companion object {
    fun of(): FactoryBasedCompressorCoder {
      return FactoryBasedCompressorCoder()
    }
  }
}
