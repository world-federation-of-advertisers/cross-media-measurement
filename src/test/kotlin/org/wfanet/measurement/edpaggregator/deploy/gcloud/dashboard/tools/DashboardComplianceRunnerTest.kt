/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.tools

import com.google.common.truth.Truth.assertThat
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.CheckResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.EdpConfig

@RunWith(JUnit4::class)
class DashboardComplianceRunnerTest {
  private fun pass(message: String) = CheckResult("check", true, message)

  private fun fail(message: String) = CheckResult("check", false, message)

  @Test
  fun `report counts passed and failed across sections`() {
    val report =
      DashboardComplianceRunner.Report(
        listOf(
          DashboardComplianceRunner.Section("A", listOf(pass("a1"), fail("a2"))),
          DashboardComplianceRunner.Section("B", listOf(pass("b1"))),
        )
      )

    assertThat(report.passed).isEqualTo(2)
    assertThat(report.failed).isEqualTo(1)
    assertThat(report.results).hasSize(3)
    assertThat(report.allPassed).isFalse()
  }

  @Test
  fun `report allPassed is true when every result passes`() {
    val report =
      DashboardComplianceRunner.Report(
        listOf(DashboardComplianceRunner.Section("A", listOf(pass("a1"), pass("a2"))))
      )

    assertThat(report.allPassed).isTrue()
    assertThat(report.failed).isEqualTo(0)
  }

  @Test
  fun `empty report is allPassed with zero counts`() {
    val report = DashboardComplianceRunner.Report(emptyList())

    assertThat(report.allPassed).isTrue()
    assertThat(report.passed).isEqualTo(0)
    assertThat(report.results).isEmpty()
  }

  @Test
  fun `parseEdps parses semicolon-separated name colon resourceId pairs`() {
    val edps = DashboardComplianceRunner.parseEdps("meta:AbCdEf_12345;google:GhIjKl_67890")

    assertThat(edps)
      .containsExactly(EdpConfig("meta", "AbCdEf_12345"), EdpConfig("google", "GhIjKl_67890"))
      .inOrder()
  }

  @Test
  fun `parseEdps trims whitespace and skips blank entries`() {
    val edps = DashboardComplianceRunner.parseEdps(" meta:AbC ; ; google:GhI ;")

    assertThat(edps).containsExactly(EdpConfig("meta", "AbC"), EdpConfig("google", "GhI")).inOrder()
  }

  @Test
  fun `parseEdps keeps additional colons in the resourceId`() {
    val edps = DashboardComplianceRunner.parseEdps("meta:AbC:extra")

    assertThat(edps).containsExactly(EdpConfig("meta", "AbC:extra"))
  }

  @Test
  fun `parseEdps throws a descriptive error when an entry has no colon`() {
    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        DashboardComplianceRunner.parseEdps("meta:AbC;metaonly")
      }
    assertThat(exception).hasMessageThat().contains("metaonly")
  }

  @Test
  fun `parseEdps throws when resourceId is empty`() {
    assertThrows(IllegalArgumentException::class.java) {
      DashboardComplianceRunner.parseEdps("meta:")
    }
  }

  @Test
  fun `parseDashboardConfig returns edps and region from valid config`() {
    val json =
      """
      {
        "bigquery_region": "us-central1",
        "deletion_protection": false,
        "operators": ["user:alice@example.com"],
        "edps": [
          {"name": "meta", "resource_id": "J3-pzhqS9Lo"},
          {"name": "edp7", "resource_id": "T5RryPMNong"}
        ]
      }
      """
        .trimIndent()

    val resolved = DashboardComplianceRunner.parseDashboardConfig(json)

    assertThat(resolved.region).isEqualTo("us-central1")
    assertThat(resolved.edps)
      .containsExactly(EdpConfig("meta", "J3-pzhqS9Lo"), EdpConfig("edp7", "T5RryPMNong"))
      .inOrder()
  }

  @Test
  fun `parseDashboardConfig ignores unknown fields`() {
    val json =
      """{"bigquery_region": "us-central1", "future_field": "x", "edps": [{"name": "meta", "resource_id": "abc"}]}"""

    val resolved = DashboardComplianceRunner.parseDashboardConfig(json)

    assertThat(resolved.region).isEqualTo("us-central1")
    assertThat(resolved.edps).containsExactly(EdpConfig("meta", "abc"))
  }

  @Test
  fun `parseDashboardConfig throws when edps is empty`() {
    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        DashboardComplianceRunner.parseDashboardConfig(
          """{"bigquery_region": "us-central1", "edps": []}"""
        )
      }
    assertThat(exception).hasMessageThat().contains("edps")
  }

  @Test
  fun `parseDashboardConfig throws when bigquery_region is missing`() {
    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        DashboardComplianceRunner.parseDashboardConfig(
          """{"edps": [{"name": "meta", "resource_id": "abc"}]}"""
        )
      }
    assertThat(exception).hasMessageThat().contains("bigquery_region")
  }

  @Test
  fun `parseDashboardConfig throws on malformed json`() {
    assertThrows(IllegalArgumentException::class.java) {
      DashboardComplianceRunner.parseDashboardConfig("{not valid json")
    }
  }

  @Test
  fun `parseDashboardConfig throws when an edp is missing resource_id`() {
    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        DashboardComplianceRunner.parseDashboardConfig(
          """{"bigquery_region": "us-central1", "edps": [{"name": "meta"}]}"""
        )
      }
    assertThat(exception).hasMessageThat().contains("resource_id")
  }

  @Test
  fun `parseDashboardConfig throws when an edp is missing name`() {
    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        DashboardComplianceRunner.parseDashboardConfig(
          """{"bigquery_region": "us-central1", "edps": [{"resource_id": "abc"}]}"""
        )
      }
    assertThat(exception).hasMessageThat().contains("name")
  }
}
