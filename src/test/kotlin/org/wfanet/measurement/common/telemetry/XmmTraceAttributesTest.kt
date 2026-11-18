/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.telemetry

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class XmmTraceAttributesTest {
  @Test
  fun `errorType includes enclosing class for nested exception`() {
    assertThat(XmmTraceAttributes.errorType(XmmTestException.Nested()))
      .isEqualTo("XmmTestException.Nested")
  }

  @Test
  fun `errorCode returns status from wrapped gRPC exception`() {
    val error = Exception(Status.UNAVAILABLE.asRuntimeException())

    assertThat(XmmTraceAttributes.errorCode(error)).isEqualTo("grpc.UNAVAILABLE")
  }

  @Test
  fun `generic keys retain stable names`() {
    assertThat(XmmTraceAttributes.WORK_ITEM_GENERATION.key).isEqualTo("xmm.work_item.generation")
    assertThat(XmmTraceAttributes.LIFECYCLE_STAGE.key).isEqualTo("xmm.lifecycle.stage")
  }
}

private sealed class XmmTestException : Exception() {
  class Nested : XmmTestException()
}
