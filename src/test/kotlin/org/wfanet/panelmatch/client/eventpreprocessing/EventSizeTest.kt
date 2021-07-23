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

package org.wfanet.panelmatch.client.eventpreprocessing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import org.apache.beam.sdk.values.KV
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class EventSizeTest {
  @Test
  fun emptyByteStrings() {
    val test: KV<ByteString, ByteString> = KV.of(ByteString.EMPTY, ByteString.EMPTY)
    val result: Int = EventSize.apply(test)
    assertThat(result).isEqualTo(0)
  }
  fun keyEmpty() {
    val test: KV<ByteString, ByteString> = byteStringKvOf(ByteString.EMPTY.toString(), "1")
    val result: Int = EventSize.apply(test)
    assertThat(result).isEqualTo(1)
  }
  fun valueEmpty() {
    val test: KV<ByteString, ByteString> = byteStringKvOf("123", ByteString.EMPTY.toString())
    val result: Int = EventSize.apply(test)
    assertThat(result).isEqualTo(3)
  }
  fun string() {
    val test: KV<ByteString, ByteString> = byteStringKvOf("12345", "67891")
    val result: Int = EventSize.apply(test)
    assertThat(result).isEqualTo(10)
  }
  fun stringSpaces() {
    val test: KV<ByteString, ByteString> = byteStringKvOf("123 5", " 67891")
    val result: Int = EventSize.apply(test)
    assertThat(result).isEqualTo(10)
  }
  fun byteStringKvOf(key: String, value: String): KV<ByteString, ByteString> {
    return KV.of(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value))
  }
}
