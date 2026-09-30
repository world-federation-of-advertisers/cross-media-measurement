// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DataAvailabilitySyncTaskIdsTest {
  @Test
  fun `resource ID is stable for an object version`() {
    assertThat(DataAvailabilitySyncTaskIds.resourceId(DONE_URI, 123L))
      .isEqualTo("das-740cbdb87edfe38410c67f9020f0ce4d")
    assertThat(DataAvailabilitySyncTaskIds.requestId(DONE_URI, 123L))
      .isEqualTo("7231b8f8-57ea-4fa1-a7de-7b0bb1b7f546")
  }

  @Test
  fun `resource ID changes with generation`() {
    assertThat(DataAvailabilitySyncTaskIds.resourceId(DONE_URI, 123L))
      .isNotEqualTo(DataAvailabilitySyncTaskIds.resourceId(DONE_URI, 124L))
  }

  @Test
  fun `URI scheme is canonicalized`() {
    assertThat(DataAvailabilitySyncTaskIds.resourceId("GS://bucket/path/done", 123L))
      .isEqualTo(DataAvailabilitySyncTaskIds.resourceId(DONE_URI, 123L))
  }

  @Test
  fun `invalid URI is rejected`() {
    assertFailsWith<IllegalArgumentException> {
      DataAvailabilitySyncTaskIds.resourceId("https://bucket/path/done", 123L)
    }
  }

  companion object {
    private const val DONE_URI = "gs://bucket/path/done"
  }
}
