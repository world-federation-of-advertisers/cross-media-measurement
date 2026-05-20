/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.privacybudgetmanager

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.privacybudgetmanager.LandscapeProcessor.MappingNode
import org.wfanet.measurement.privacybudgetmanager.testing.InMemoryAuditLog
import org.wfanet.measurement.privacybudgetmanager.testing.InMemoryLedger

@RunWith(JUnit4::class)
class PrivacyBudgetManagerTest {
  @Test
  fun `Privacy Budget Manager initializes successfully for no inactive landscapes`() {
    val auditLog = InMemoryAuditLog()
    val activeLandscape = privacyLandscape {}
    val ledger = InMemoryLedger()

    val pbm =
      PrivacyBudgetManager(
        auditLog,
        listOf(MappingNode(activeLandscape, null)),
        ledger,
        LandscapeProcessor(),
        MAXIMUM_PRIVACY_USAGE_PER_BUCKET,
        MAXIMUM_DELTA_PER_BUCKET,
        TestEvent.getDescriptor(),
      )
  }

  companion object {
    private const val MAXIMUM_PRIVACY_USAGE_PER_BUCKET = 1.0f
    private const val MAXIMUM_DELTA_PER_BUCKET = 1.0e-9f
  }
}
