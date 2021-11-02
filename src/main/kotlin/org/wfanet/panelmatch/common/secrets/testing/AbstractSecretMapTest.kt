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

package org.wfanet.panelmatch.common.secrets.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.testing.runBlockingTest

private val ITEM1 = "key1" to "value1".toByteStringUtf8()
private val ITEM2 = "key2" to "value2".toByteStringUtf8()

abstract class AbstractSecretMapTest<T : SecretMap> {
  abstract suspend fun secretMapOf(vararg items: Pair<String, ByteString>): T

  protected val map: T by lazy { runBlocking { secretMapOf(ITEM1, ITEM2) } }

  @Test
  fun itemPresent() = runBlockingTest {
    assertThat(map.get(ITEM1.first)).isEqualTo(ITEM1.second)
    assertThat(map.get(ITEM2.first)).isEqualTo(ITEM2.second)
  }

  @Test fun itemMissing() = runBlockingTest { assertThat(map.get("some-missing-key")).isNull() }
}
