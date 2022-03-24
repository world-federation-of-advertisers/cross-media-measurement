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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.ErrorCode

private const val EXTERNAL_ACCOUNT_ID = 123L
private const val ERROR_DOMAIN = "com.google.rpc.ErrorInfo"

@RunWith(JUnit4::class)
class KingdomInternalExceptionTest {
  @Test
  fun `error context can be written by thrower and read by receiver`() {
    val errorContext = ErrorContext()
    errorContext.externalAccountId = EXTERNAL_ACCOUNT_ID
    val exception =
      assertFailsWith<StatusRuntimeException> {
        val internalException = KingdomInternalException(ErrorCode.ACCOUNT_NOT_FOUND, errorContext)
        internalException.throwStatusRuntimeException(Status.NOT_FOUND) { "Account not found" }
      }
    val info = exception.getErrorInfo()
    assertNotNull(info)
    assertThat(info.reason).isEqualTo("ACCOUNT_NOT_FOUND")
    assertThat(info.domain).isEqualTo(ERROR_DOMAIN)
    val metadata = info.metadataMap
    assertNotNull(metadata)
    assertThat(metadata).isEqualTo(mapOf("external_account_id" to EXTERNAL_ACCOUNT_ID.toString()))
  }
}
