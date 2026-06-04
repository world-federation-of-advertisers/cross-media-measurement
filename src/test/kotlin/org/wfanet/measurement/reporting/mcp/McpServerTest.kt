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
import com.google.common.truth.Truth.assertWithMessage
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.mcp.testing.FakeReportingPublicApiClient

@RunWith(JUnit4::class)
class McpServerTest {

  @Test
  fun registersAllExpectedTools() {
    val server = createMcpServer(FakeReportingPublicApiClient.create()) { "test-token" }
    assertThat(server.tools.keys)
      .containsExactly(
        "get_event_group",
        "list_event_groups",
        "create_basic_report",
        "get_basic_report",
        "list_basic_reports",
        "create_reporting_set",
        "get_reporting_set",
        "list_reporting_sets",
        "get_impression_qualification_filter",
        "list_impression_qualification_filters",
      )
  }

  @Test
  fun allToolsHaveDescriptions() {
    val server = createMcpServer(FakeReportingPublicApiClient.create()) { "test-token" }
    for ((name, tool) in server.tools) {
      assertWithMessage("tool '$name' should have a description")
        .that(tool.tool.description)
        .isNotEmpty()
    }
  }

  @Test
  fun createMcpServerWithDifferentTokensProducesSeparateInstances() {
    val apiClient = FakeReportingPublicApiClient.create()
    val server1 = createMcpServer(apiClient) { "token-1" }
    val server2 = createMcpServer(apiClient) { "token-2" }
    assertThat(server1).isNotSameInstanceAs(server2)
  }
}
