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
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as InternalState
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata

/** Converts an internal [InternalRequisitionMetadata] to a public [RequisitionMetadata]. */
fun InternalRequisitionMetadata.toRequisitionMetadata(): RequisitionMetadata {
  val source = this
  return requisitionMetadata {
    name =
      RequisitionMetadataKey(source.dataProviderResourceId, source.requisitionMetadataResourceId)
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
    this.dataProviderResourceId = dataProviderKey.dataProviderId
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

/** Converts an [InternalState] to a public [RequisitionMetadata.State]. */
internal fun InternalState.toState(): RequisitionMetadata.State {
  return when (this) {
    InternalState.REQUISITION_METADATA_STATE_STORED -> RequisitionMetadata.State.STORED
    InternalState.REQUISITION_METADATA_STATE_QUEUED -> RequisitionMetadata.State.QUEUED
    InternalState.REQUISITION_METADATA_STATE_PROCESSING -> RequisitionMetadata.State.PROCESSING
    InternalState.REQUISITION_METADATA_STATE_FULFILLED -> RequisitionMetadata.State.FULFILLED
    InternalState.REQUISITION_METADATA_STATE_REFUSED -> RequisitionMetadata.State.REFUSED
    InternalState.UNRECOGNIZED,
    InternalState.REQUISITION_METADATA_STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
