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

import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig.KmsConfig
import org.wfanet.measurement.config.edpaggregator.EventDataProviderConfig.TeeWorkload

/** Selects the credentials and output KEK for one attested TEE job. */
object WorkloadKmsConfigs {
  /**
   * Returns the legacy config only when no job identities have been configured.
   *
   * @throws IllegalArgumentException for unspecified, missing, duplicate, or unknown job entries.
   */
  fun select(config: EventDataProviderConfig, workload: TeeWorkload): KmsConfig {
    require(
      workload != TeeWorkload.TEE_WORKLOAD_UNSPECIFIED && workload != TeeWorkload.UNRECOGNIZED
    )
    if (config.workloadKmsConfigsCount == 0) {
      return config.kmsConfig
    }
    val entries = config.workloadKmsConfigsList
    require(
      entries.all {
        it.workload != TeeWorkload.TEE_WORKLOAD_UNSPECIFIED &&
          it.workload != TeeWorkload.UNRECOGNIZED
      }
    ) {
      "Invalid workload identity for ${config.dataProvider}"
    }
    require(entries.map { it.workload }.distinct().size == entries.size) {
      "Duplicate workload identity for ${config.dataProvider}"
    }
    val entry =
      requireNotNull(entries.singleOrNull { it.workload == workload }) {
        "No $workload KMS identity for ${config.dataProvider}"
      }
    require(entry.hasKmsConfig()) { "No KMS config for $workload" }
    require(entry.kmsConfig.kekUri.isNotEmpty()) { "No output KEK for $workload" }
    return entry.kmsConfig
  }
}
