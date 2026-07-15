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
}
