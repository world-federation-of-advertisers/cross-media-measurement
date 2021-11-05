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

package org.wfanet.panelmatch.client.common.compression

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.common.compression.CompressionParameters
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.brotliCompressionParameters
import org.wfanet.panelmatch.common.compression.compressionParameters

/** CompressionParameters that uses Brotli and a built-in dictionary. */
val DEFAULT_COMPRESSION_PARAMETERS: CompressionParameters by lazy {
  compressionParameters { brotli = brotliCompressionParameters { dictionary = DEFAULT_DICTIONARY } }
}

private val DEFAULT_DICTIONARY: ByteString by lazy {
  val stream = {}::class.java.getResourceAsStream("/data/brotli-dictionary")
  stream.use { ByteString.readFrom(it) }
}
