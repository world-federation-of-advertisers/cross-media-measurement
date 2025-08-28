// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.api.v1alpha

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata

/** Converts an internal [InternalRequisitionMetadata] to a public [RequisitionMetadata]. */
fun InternalRequisitionMetadata.toRequisitionMetadata(): RequisitionMetadata {
  val source = this
  return requisitionMetadata {
    name =
      RequisitionMetadataKey(
          externalIdToApiId(source.externalDataProviderId),
          externalIdToApiId(source.externalRequisitionMetadataId),
        )
        .toName()
    cmmsRequisition = source.cmmsRequisition
    blobUri = source.blobUri
    blobTypeUrl = source.blobTypeUrl
    groupId = source.groupId
    cmmsCreateTime = source.cmmsCreateTime
    report = source.report
    workItem = source.workItem
    state = source.state.toState()
    createTime = source.createTime
    updateTime = source.updateTime
    refusalMessage = source.refusalMessage
    etag = source.etag
  }
}

/**
 * Converts a public [RequisitionMetadata] to an internal [InternalRequisitionMetadata] for
 * creation.
 */
fun RequisitionMetadata.toInternal(dataProviderKey: DataProviderKey): InternalRequisitionMetadata {
  val source = this
  return internalRequisitionMetadata {
    this.externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
    cmmsRequisition = source.cmmsRequisition
    blobUri = source.blobUri
    blobTypeUrl = source.blobTypeUrl
    groupId = source.groupId
    cmmsCreateTime = source.cmmsCreateTime
    report = source.report
    workItem = source.workItem
    refusalMessage = source.refusalMessage
  }
}

/**
 * Converts an internal [InternalRequisitionMetadata.State] to a public [RequisitionMetadata.State].
 */
internal fun InternalRequisitionMetadata.State.toState(): RequisitionMetadata.State {
  return when (this) {
    InternalRequisitionMetadata.State.STORED -> RequisitionMetadata.State.STORED
    InternalRequisitionMetadata.State.QUEUED -> RequisitionMetadata.State.QUEUED
    InternalRequisitionMetadata.State.PROCESSING -> RequisitionMetadata.State.PROCESSING
    InternalRequisitionMetadata.State.FULFILLED -> RequisitionMetadata.State.FULFILLED
    InternalRequisitionMetadata.State.REFUSED -> RequisitionMetadata.State.REFUSED
    InternalRequisitionMetadata.State.UNRECOGNIZED,
    InternalRequisitionMetadata.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
