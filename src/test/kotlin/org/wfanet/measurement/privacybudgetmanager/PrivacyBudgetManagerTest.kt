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
import org.wfanet.measurement.privacybudgetmanager.testing.TestInMemoryAuditLog
import org.wfanet.measurement.privacybudgetmanager.testing.TestInMemoryBackingStore

@RunWith(JUnit4::class)
class PrivacyBudgetManagerTest {

  // TODO(uakyol) : Delete this test as the PBM implementation is completed.
  @Test
  fun `Privacy Budget Manager Compiles but throws not implemented`() {
    
    val auditLog = TestInMemoryAuditLog()
    val activePrivacyLandscape = AuditLog()
    val backingStore = TestInMemoryBackingStore()

    val pbm =
      PrivacyBudgetManager(
        auditLog,
        activePrivacyLandscape,
        emptyList(),
        emptyList(),
        backingStore,
      )
  }
}
