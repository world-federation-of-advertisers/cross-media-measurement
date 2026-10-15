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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DashboardIsolationChecksTest {
  @Test
  fun `unlinkedAccountsPipelineHealthy is false when stale source rows exist but dashboard is empty`() {
    assertThat(DashboardIsolationChecks.unlinkedAccountsPipelineHealthy(5, 0)).isFalse()
  }

  @Test
  fun `unlinkedAccountsPipelineHealthy is true when the dashboard table is populated`() {
    assertThat(DashboardIsolationChecks.unlinkedAccountsPipelineHealthy(5, 5)).isTrue()
  }

  @Test
  fun `unlinkedAccountsPipelineHealthy is true when there are no stale source rows`() {
    assertThat(DashboardIsolationChecks.unlinkedAccountsPipelineHealthy(0, 0)).isTrue()
    assertThat(DashboardIsolationChecks.unlinkedAccountsPipelineHealthy(0, 5)).isTrue()
  }

  @Test
  fun `isEmptyResultHealthy is true only for exempt tables`() {
    assertThat(DashboardIsolationChecks.isEmptyResultHealthy("unlinked_accounts")).isTrue()
    assertThat(DashboardIsolationChecks.isEmptyResultHealthy("requisition_overview")).isFalse()
  }
}
