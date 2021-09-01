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

package org.wfanet.panelmatch.common.crypto.testing

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.common.crypto.SymmetricCryptor
import org.wfanet.panelmatch.common.toByteString

/** Does no real crypto. It only xors [data] with [privateKey]. */
class ConcatSymmetricCryptor : SymmetricCryptor {

  private val SEPARATOR = " encrypted by "
  override fun encrypt(privateKey: ByteString, data: ByteString): ByteString {
    return data.concat(SEPARATOR.toByteString()).concat(privateKey)
  }

  override fun decrypt(privateKey: ByteString, data: ByteString): ByteString {
    val suffix = SEPARATOR + privateKey.toStringUtf8()
    val dataString = data.toStringUtf8()
    require(dataString.endsWith(suffix))
    return dataString.removeSuffix(suffix).toByteString()
  }
}
