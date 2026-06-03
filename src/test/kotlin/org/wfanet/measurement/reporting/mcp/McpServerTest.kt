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

package org.wfanet.measurement.reporting.mcp

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.mcp.testing.FakeReportingPublicApiClient

@RunWith(JUnit4::class)
class McpServerTest {

  @Test
  fun createMcpServerDoesNotThrow() {
    val server = createMcpServer(FakeReportingPublicApiClient.create()) { "test-token" }
    assertThat(server).isNotNull()
  }

  @Test
  fun createMcpServerWithDifferentTokensProducesSeparateInstances() {
    val apiClient = FakeReportingPublicApiClient.create()
    val server1 = createMcpServer(apiClient) { "token-1" }
    val server2 = createMcpServer(apiClient) { "token-2" }
    assertThat(server1).isNotSameInstanceAs(server2)
  }
}
