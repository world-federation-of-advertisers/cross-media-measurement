/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscape
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscapeMapping
import org.wfanet.measurement.privacybudgetmanager.privacyLandscape
import org.wfanet.measurement.privacybudgetmanager.testing.TestInMemoryAuditLog
import org.wfanet.measurement.privacybudgetmanager.testing.TestInMemoryLedger

@RunWith(JUnit4::class)
class PrivacyBudgetManagerTest {
  @Test
  fun `Privacy Budget Manager initializes successfully for no inactive landscapes`() {
    val auditLog = TestInMemoryAuditLog()
    val activeLandscape = privacyLandscape {}
    val ledger = TestInMemoryLedger()

    val pbm =
      PrivacyBudgetManager(
        auditLog,
        activeLandscape,
        emptyList<PrivacyLandscape>(),
        emptyList<PrivacyLandscapeMapping>(),
        ledger,
      )
  }
}
