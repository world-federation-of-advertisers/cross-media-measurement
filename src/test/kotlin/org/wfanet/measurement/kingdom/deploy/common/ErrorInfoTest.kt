// Copyright 2022 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.kingdom.ErrorCode

@RunWith(JUnit4::class)
class ErrorInfoTest {
  @Test
  fun `error info correctly ships load`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        failGrpcWithInfo(
          Status.NOT_FOUND,
          ErrorCode.API_KEY_NOT_FOUND,
          mapOf("api_key" to "123456", "retry" to "false")
        ) { "API Key Not Found" }
      }
    val info = getErrorInfo(exception)
    assertNotNull(info)
    assertThat(info.reason).isEqualTo("API_KEY_NOT_FOUND")
    assertThat(info.domain).isEqualTo("com.google.rpc.ErrorInfo")
    val metadata = info.metadataMap
    assertNotNull(metadata)
    assertThat(metadata).isEqualTo(mapOf("api_key" to "123456", "retry" to "false"))
  }
}
