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

package org.wfanet.measurement.edpaggregator

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig.TeeWorkload
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfigKt.kmsConfig
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfigKt.workloadKmsConfig
import org.wfanet.measurement.config.edpaggregator.eventDataProviderConfig

@RunWith(JUnit4::class)
class WorkloadKmsConfigsTest {
  @Test
  fun `select uses the requested job identity instead of the legacy identity`() {
    val config = eventDataProviderConfig {
      kmsConfig = kmsConfig { serviceAccount = "legacy@example.com" }
      workloadKmsConfigs += workloadKmsConfig {
        workload = TeeWorkload.RESULTS_FULFILLER
        kmsConfig = kmsConfig {
          serviceAccount = "fulfiller@example.com"
          kekUri = "fake-kms://frequency-vector"
        }
      }
      workloadKmsConfigs += workloadKmsConfig {
        workload = TeeWorkload.TRUSTEE_MILL
        kmsConfig = kmsConfig {
          serviceAccount = "mill@example.com"
          kekUri = "fake-kms://frequency-vector"
        }
      }
    }
    assertThat(WorkloadKmsConfigs.select(config, TeeWorkload.RESULTS_FULFILLER).serviceAccount)
      .isEqualTo("fulfiller@example.com")
    assertThat(WorkloadKmsConfigs.select(config, TeeWorkload.TRUSTEE_MILL).serviceAccount)
      .isEqualTo("mill@example.com")
  }

  @Test
  fun `select rejects a missing job rather than falling back to shared credentials`() {
    val config = eventDataProviderConfig {
      kmsConfig = kmsConfig { serviceAccount = "legacy@example.com" }
      workloadKmsConfigs += workloadKmsConfig { workload = TeeWorkload.TRUSTEE_MILL }
    }
    assertFailsWith<IllegalArgumentException> {
      WorkloadKmsConfigs.select(config, TeeWorkload.RESULTS_FULFILLER)
    }
  }

  @Test
  fun `select rejects duplicate job credentials`() {
    val entry = workloadKmsConfig { workload = TeeWorkload.TRUSTEE_MILL }
    val config = eventDataProviderConfig {
      workloadKmsConfigs += entry
      workloadKmsConfigs += entry
    }
    assertFailsWith<IllegalArgumentException> {
      WorkloadKmsConfigs.select(config, TeeWorkload.TRUSTEE_MILL)
    }
  }

  @Test
  fun `select rejects unknown job entries`() {
    val entry = workloadKmsConfig { workloadValue = 100 }
    val config = eventDataProviderConfig { workloadKmsConfigs += entry }
    assertFailsWith<IllegalArgumentException> {
      WorkloadKmsConfigs.select(config, TeeWorkload.TRUSTEE_MILL)
    }
  }

  @Test
  fun `select preserves legacy configurations with no job entries`() {
    val config = eventDataProviderConfig {
      kmsConfig = kmsConfig { serviceAccount = "legacy@example.com" }
    }
    assertThat(WorkloadKmsConfigs.select(config, TeeWorkload.VID_LABELER).serviceAccount)
      .isEqualTo("legacy@example.com")
  }
}
