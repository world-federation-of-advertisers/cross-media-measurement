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

package org.wfanet.measurement.kingdom.deploy.common

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.ErrorDetail

@RunWith(JUnit4::class)
class ErrorDetailTest {
  @Test
  fun `error detail correctly ships load`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        failGrpcWithDetail(
          Status.NOT_FOUND,
          ErrorDetail.ErrorCode.API_KEY_NOT_FOUND,
          this.javaClass.`package`.name,
          mapOf("api_key" to "123456", "retry" to "false")
        ) { "API Key Not Found" }
      }
    val detail = getErrorDetail(exception)
    assertNotNull(detail)
    assertThat(detail.code).isEqualTo(ErrorDetail.ErrorCode.API_KEY_NOT_FOUND)
    assertThat(detail.info.reason).isEqualTo("API_KEY_NOT_FOUND")
    assertThat(detail.info.domain).isEqualTo("org.wfanet.measurement.kingdom.deploy.common")
    val metadata = detail.info.metadataMap
    assertNotNull(metadata)
    assertThat(metadata).isEqualTo(mapOf("api_key" to "123456", "retry" to "false"))
  }
}
