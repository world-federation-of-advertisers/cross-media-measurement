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

data class FactoryBasedCompressor(val dictionary: ByteString, val factory: CompressorFactory) :
  Compressor {
  override fun compress(events: ByteString): ByteString {
    return factory.build(dictionary).compress(events)
  }

  override fun uncompress(compressedEvents: ByteString): ByteString {
    return factory.build(dictionary).uncompress(compressedEvents)
  }
}
