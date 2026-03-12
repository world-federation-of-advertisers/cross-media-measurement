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

/** [ResourceKey] of a RawImpressionMetadataUpload. */
data class RawImpressionUploadKey(
  override val parentKey: DataProviderKey,
  val rawImpressionMetadataUploadId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionMetadataUploadId: String,
  ) : this(DataProviderKey(dataProviderId), rawImpressionMetadataUploadId)

  val dataProviderId: String
    get() = parentKey.dataProviderId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_METADATA_UPLOAD to rawImpressionMetadataUploadId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<RawImpressionUploadKey> {
    const val PATTERN =
      "${DataProviderKey.PATTERN}/rawImpressionMetadataUploads/{raw_impression_metadata_upload}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RawImpressionUploadKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RawImpressionUploadKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_METADATA_UPLOAD),
      )
    }
  }
}

/** [ResourceKey] of a RawImpressionMetadataUpload batch. */
data class RawImpressionBatchKey(
  override val parentKey: RawImpressionUploadKey,
  val batchId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionMetadataUploadId: String,
    batchId: String,
  ) : this(
    RawImpressionUploadKey(dataProviderId, rawImpressionMetadataUploadId),
    batchId,
  )

  val dataProviderId: String
    get() = parentKey.dataProviderId

  val rawImpressionMetadataUploadId: String
    get() = parentKey.rawImpressionMetadataUploadId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_METADATA_UPLOAD to rawImpressionMetadataUploadId,
        IdVariable.BATCH to batchId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<RawImpressionBatchKey> {
    const val PATTERN = "${RawImpressionUploadKey.PATTERN}/batches/{batch}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RawImpressionBatchKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RawImpressionBatchKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_METADATA_UPLOAD),
        idVars.getValue(IdVariable.BATCH),
      )
    }
  }
}

/** [ResourceKey] of a RawImpressionMetadata file. */
data class RawImpressionMetadataKey(
  override val parentKey: RawImpressionBatchKey,
  val fileId: String,
) : ChildResourceKey {
  constructor(
    dataProviderId: String,
    rawImpressionMetadataUploadId: String,
    batchId: String,
    fileId: String,
  ) : this(
    RawImpressionBatchKey(dataProviderId, rawImpressionMetadataUploadId, batchId),
    fileId,
  )

  val dataProviderId: String
    get() = parentKey.dataProviderId

  val rawImpressionMetadataUploadId: String
    get() = parentKey.parentKey.rawImpressionMetadataUploadId

  val batchId: String
    get() = parentKey.batchId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.DATA_PROVIDER to dataProviderId,
        IdVariable.RAW_IMPRESSION_METADATA_UPLOAD to rawImpressionMetadataUploadId,
        IdVariable.BATCH to batchId,
        IdVariable.FILE to fileId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<RawImpressionMetadataKey> {
    const val PATTERN = "${RawImpressionBatchKey.PATTERN}/files/{file}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): RawImpressionMetadataKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return RawImpressionMetadataKey(
        idVars.getValue(IdVariable.DATA_PROVIDER),
        idVars.getValue(IdVariable.RAW_IMPRESSION_METADATA_UPLOAD),
        idVars.getValue(IdVariable.BATCH),
        idVars.getValue(IdVariable.FILE),
      )
    }
  }
}
