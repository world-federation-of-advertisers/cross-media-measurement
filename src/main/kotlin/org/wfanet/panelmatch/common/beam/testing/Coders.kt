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

package org.wfanet.panelmatch.common.beam.testing

import com.google.protobuf.ByteString
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import org.apache.beam.sdk.coders.Coder

data class CoderResult<T>(val encodedData: ByteString, val result: T)

fun <T> Coder<T>.roundtrip(item: T): CoderResult<T> {
  val output = ByteArrayOutputStream()

  encode(item, output)
  val bytes = output.toByteArray()
  val input = ByteArrayInputStream(bytes)

  return CoderResult(ByteString.copyFrom(bytes), decode(input))
}
