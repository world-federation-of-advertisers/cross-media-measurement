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

package org.wfanet.measurement.loadtest.config

import com.google.protobuf.Message
import org.projectnessie.cel.Program
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters.compileProgram
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.InMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketMapper
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.loadtest.config.LoadTestEventKt.privacy

class TestPrivacyBucketMapper : PrivacyBucketMapper {

  override val operativeFields = setOf("privacy.filterable")

  /** This mapper does not charge any bucket [filterExpression] is ignored. */
  override fun toPrivacyFilterProgram(filterExpression: String): Program =
    try {
      compileProgram(LoadTestEvent.getDescriptor(), "privacy.filterable == true", operativeFields)
    } catch (e: EventFilterValidationException) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER,
        e,
      )
    }

  /** To not charge any buckets, [privacyBucketGroup] is ignored and set to be not filterable. */
  override fun toEventMessage(privacyBucketGroup: PrivacyBucketGroup): Message {
    return loadTestEvent { privacy = privacy { filterable = false } }
  }
}

object PrivacyBudgets {
  /** Builds a [PrivacyBudgetManager] with [InMemoryBackingStore]. */
  fun createNoOpPrivacyBudgetManager(): PrivacyBudgetManager {
    return PrivacyBudgetManager(
      PrivacyBucketFilter(TestPrivacyBucketMapper()),
      InMemoryBackingStore(),
      10.0f,
      1000.0f,
    )
  }
}
