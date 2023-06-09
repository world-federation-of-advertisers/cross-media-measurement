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

package org.wfanet.panelmatch.common.secrets

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.nio.file.Path
import java.util.Base64

/**
 * SecretSet that reads in a file and treats it as a two-column CSV file.
 *
 * The substring before the first comma (',') is treated as the map keys, and the remainder of the
 * string after the first comma is interpreted as a base64-encoded ByteString.
 */
class CsvSecretMap(path: Path) : SecretMap {
  private val contents: Map<String, ByteString> =
    path.toFile().readLines().associate {
      it.substringBefore(',') to Base64.getDecoder().decode(it.substringAfter(',')).toByteString()
    }

  override suspend fun get(key: String): ByteString? {
    return contents[key]
  }
}
