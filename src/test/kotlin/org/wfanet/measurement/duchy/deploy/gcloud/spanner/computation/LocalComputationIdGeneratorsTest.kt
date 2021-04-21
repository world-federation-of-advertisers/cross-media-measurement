// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import java.lang.Long.parseUnsignedLong
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class LocalComputationIdGeneratorsTest {
  @Test
  fun `gen local id`() {
    val testTime = 0x0FFFFFFF_1230ABCD
    val gen =
      GlobalBitsPlusTimeStampIdGenerator(
        Clock.fixed(Instant.ofEpochMilli(testTime), ZoneId.systemDefault())
      )
    val globalId = "123"
    assertEquals(
      parseUnsignedLong("B3D50C48FFFFFFF0", 16) or globalId.hashCode().toLong(),
      gen.localId(globalId)
    )
  }
}
