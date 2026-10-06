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

package org.wfanet.measurement.edpaggregator.service

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a data availability synchronization task. */
data class DataAvailabilitySyncTaskKey(
  override val parentKey: RawImpressionUploadKey,
  val dataAvailabilitySyncTaskId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionUploadId: String,
    dataAvailabilitySyncTaskId: String,
  ) : this(
    RawImpressionUploadKey(dataProviderId, rawImpressionUploadId),
    dataAvailabilitySyncTaskId,
  )

  val dataProviderId: String
    get() = parentKey.dataProviderId

  val rawImpressionUploadId: String
    get() = parentKey.rawImpressionUploadId

  override fun toName(): String =
    parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_UPLOAD to rawImpressionUploadId,
        IdVariable.DATA_AVAILABILITY_SYNC_TASK to dataAvailabilitySyncTaskId,
      )
    )

  companion object FACTORY : ResourceKey.Factory<DataAvailabilitySyncTaskKey> {
    const val PATTERN =
      "${RawImpressionUploadKey.PATTERN}/dataAvailabilitySyncTasks/{data_availability_sync_task}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): DataAvailabilitySyncTaskKey? {
      val idVars = parser.parseIdVars(resourceName) ?: return null
      return DataAvailabilitySyncTaskKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_UPLOAD),
        idVars.getValue(IdVariable.DATA_AVAILABILITY_SYNC_TASK),
      )
    }
  }
}
