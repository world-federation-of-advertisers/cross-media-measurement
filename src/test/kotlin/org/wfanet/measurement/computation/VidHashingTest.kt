// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class VidHashingTest {
  @Test
  fun `hash returns the salted FarmHash fingerprint that orders frequency vectors`() {
    // Golden values: DataProviders and the TrusTEE v2 Mill must order VIDs identically.
    assertThat(VidHashing.hash(1L)).isEqualTo(7291877513898648417L)
    assertThat(VidHashing.hash(12345L)).isEqualTo(-589574862140868813L)
  }

  @Test
  fun `hash depends on the salt`() {
    assertThat(VidHashing.hash(1L, ByteString.copyFromUtf8("other salt")))
      .isNotEqualTo(VidHashing.hash(1L))
  }
}
