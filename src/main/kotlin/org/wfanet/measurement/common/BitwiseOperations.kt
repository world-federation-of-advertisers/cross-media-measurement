/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import kotlin.experimental.xor

infix fun ByteString.xor(other: ByteString): ByteString {
  require(this.size() == other.size()) {
    "ByteString sizes do not match. size_1=${this.size()}, size_2=${other.size()}"
  }

  val result = ByteArray(this.size())
  for (i in result.indices) {
    result[i] = this.byteAt(i) xor other.byteAt(i)
  }
  return result.toByteString()
}
