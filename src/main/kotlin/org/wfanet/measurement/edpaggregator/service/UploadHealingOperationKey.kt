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

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of an UploadHealingOperation. */
data class UploadHealingOperationKey(
  override val parentKey: DataProviderKey,
  val uploadHealingOperationId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    uploadHealingOperationId: String,
  ) : this(DataProviderKey(dataProviderId), uploadHealingOperationId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String =
    parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.UPLOAD_HEALING_OPERATION to uploadHealingOperationId,
      )
    )

  companion object FACTORY : ResourceKey.Factory<UploadHealingOperationKey> {
    const val PATTERN =
      "${DataProviderKey.PATTERN}/uploadHealingOperations/{upload_healing_operation}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): UploadHealingOperationKey? {
      val idVars = parser.parseIdVars(resourceName) ?: return null
      return UploadHealingOperationKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.UPLOAD_HEALING_OPERATION),
      )
    }
  }
}

/** [ResourceKey] of an UploadHealingStep. */
data class UploadHealingStepKey(
  override val parentKey: UploadHealingOperationKey,
  val uploadHealingStepId: String,
) : ChildResourceKey {
  val dataProviderId: String
    get() = parentKey.dataProviderId

  val uploadHealingOperationId: String
    get() = parentKey.uploadHealingOperationId

  override fun toName(): String =
    parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.UPLOAD_HEALING_OPERATION to uploadHealingOperationId,
        IdVariable.UPLOAD_HEALING_STEP to uploadHealingStepId,
      )
    )

  companion object FACTORY : ResourceKey.Factory<UploadHealingStepKey> {
    const val PATTERN = "${UploadHealingOperationKey.PATTERN}/steps/{upload_healing_step}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): UploadHealingStepKey? {
      val idVars = parser.parseIdVars(resourceName) ?: return null
      return UploadHealingStepKey(
        UploadHealingOperationKey(
          idVars.getValue(IdVariable.DATA_PROVIDER),
          idVars.getValue(IdVariable.UPLOAD_HEALING_OPERATION),
        ),
        idVars.getValue(IdVariable.UPLOAD_HEALING_STEP),
      )
    }
  }
}
