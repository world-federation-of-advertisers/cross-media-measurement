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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.Date
import java.time.LocalDate
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DateUtilTest {

  @Test
  fun `conversion between protobuf Date and Java LocalDate`() {
    val localDate = LocalDate.of(2021, 3, 12)
    val date =
      Date.newBuilder()
        .apply {
          year = 2021
          month = 3
          day = 12
        }
        .build()
    assertThat(localDate.toProtoDate()).isEqualTo(date)
    assertThat(date.toLocalDate()).isEqualTo(localDate)
  }
}
