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
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val NEW_KEY = "some-new-key"
private val VALUE = "some-value".toByteStringUtf8()

abstract class AbstractMutableSecretMapTest<T : MutableSecretMap> : AbstractSecretMapTest<T>() {

  @Test
  fun put() = runBlockingTest {
    assertThat(map.get(NEW_KEY)).isNull()

    map.put(NEW_KEY, VALUE)
    assertThat(map.get(NEW_KEY)).isEqualTo(VALUE)

    val value2 = VALUE.concat(VALUE)
    map.put(NEW_KEY, value2)
    assertThat(map.get(NEW_KEY)).isEqualTo(value2)
  }
}
