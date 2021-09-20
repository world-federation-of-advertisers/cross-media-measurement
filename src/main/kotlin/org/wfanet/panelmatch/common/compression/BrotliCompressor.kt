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
import org.wfanet.panelmatch.common.loadLibraryFromResource
import org.wfanet.panelmatch.common.wrapJniException

class BrotliCompressor(private val dictionary: ByteString) : Compressor {
  override fun compress(events: ByteString): ByteString {
    val request = compressRequest {
      dictionary = this@BrotliCompressor.dictionary
      uncompressedData += events
    }
    val serializedResponse = wrapJniException {
      Brotli.brotliCompressWrapper(request.toByteArray())
    }
    val response = CompressResponse.parseFrom(serializedResponse)
    return response.compressedDataList.single()
  }

  override fun uncompress(compressedEvents: ByteString): ByteString {
    val request = decompressRequest {
      dictionary = this@BrotliCompressor.dictionary
      compressedData += compressedEvents
    }
    val serializedResponse = wrapJniException {
      Brotli.brotliDecompressWrapper(request.toByteArray())
    }
    val response = DecompressResponse.parseFrom(serializedResponse)
    return response.decompressedDataList.single()
  }

  companion object {
    init {
      loadLibraryFromResource(
        libraryName = "brotli",
        resourcePathPrefix = "/main/swig/wfanet/panelmatch/common/compression"
      )
    }
  }
}
