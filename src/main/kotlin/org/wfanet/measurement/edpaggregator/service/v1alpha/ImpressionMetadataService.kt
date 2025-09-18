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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataKey
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataNotFoundException
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.CreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata as InternalImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub as InternalImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as InternalImpressionMetadataState
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState
import org.wfanet.measurement.internal.edpaggregator.createImpressionMetadataRequest as internalCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.deleteImpressionMetadataRequest as internalDeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getImpressionMetadataRequest as internalGetImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.impressionMetadata as internalImpressionMetadata

class ImpressionMetadataService(
  private val internalImpressionMetadataStub: InternalImpressionMetadataServiceCoroutineStub
) : ImpressionMetadataServiceCoroutineImplBase() {
  override suspend fun getImpressionMetadata(
    request: GetImpressionMetadataRequest
  ): ImpressionMetadata {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val key =
      ImpressionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalImpressionMetadata =
      try {
        internalImpressionMetadataStub.getImpressionMetadata(
          internalGetImpressionMetadataRequest {
            dataProviderResourceId = key.dataProviderId
            impressionMetadataResourceId = key.impressionMetadataId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND ->
            ImpressionMetadataNotFoundException(request.name, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toImpressionMetadata()
  }

  override suspend fun createImpressionMetadata(
    request: CreateImpressionMetadataRequest
  ): ImpressionMetadata {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasImpressionMetadata()) {
      throw RequiredFieldNotSetException("impression_metadata")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val impressionMetadatakey =
      if (request.impressionMetadata.name.isNotEmpty()) {
        ImpressionMetadataKey.fromName(request.impressionMetadata.name)
          ?: throw InvalidFieldValueException("impression_metadata.name")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } else {
        null
      }

    ModelLineKey.fromName(request.impressionMetadata.modelLine)
      ?: throw InvalidFieldValueException("model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalCreateRequest = internalCreateImpressionMetadataRequest {
      impressionMetadata =
        request.impressionMetadata.toInternal(dataProviderKey, impressionMetadatakey)
      requestId = request.requestId
    }

    val internalResponse: InternalImpressionMetadata =
      try {
        internalImpressionMetadataStub.createImpressionMetadata(internalCreateRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS ->
            ImpressionMetadataAlreadyExistsException(request.impressionMetadata.name, e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toImpressionMetadata()
  }

  override suspend fun deleteImpressionMetadata(request: DeleteImpressionMetadataRequest): Empty {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val key =
      ImpressionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    try {
      internalImpressionMetadataStub.deleteImpressionMetadata(
        internalDeleteImpressionMetadataRequest {
          dataProviderResourceId = key.dataProviderId
          impressionMetadataResourceId = key.impressionMetadataId
        }
      )
    } catch (e: StatusException) {
      throw when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND ->
          ImpressionMetadataNotFoundException(request.name, e)
            .asStatusRuntimeException(e.status.code)
        InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_BLOB_URI,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
        InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
        InternalErrors.Reason.INVALID_FIELD_VALUE,
        InternalErrors.Reason.ETAG_MISMATCH,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
    return Empty.getDefaultInstance()
  }
}

/** Converts an internal [InternalImpressionMetadata] to a public [ImpressionMetadata]. */
fun InternalImpressionMetadata.toImpressionMetadata(): ImpressionMetadata {
  val source = this
  return impressionMetadata {
    name =
      "dataProviders/${source.dataProviderResourceId}/impressionMetadata/${source.impressionMetadataResourceId}"
    blobUri = source.blobUri
    blobTypeUrl = source.blobTypeUrl
    eventGroupReferenceId = source.eventGroupReferenceId
    modelLine = source.cmmsModelLine
    interval = source.interval
    state = source.state.toState()
    createTime = source.createTime
    updateTime = source.updateTime
    etag = source.etag
  }
}

/**
 * Converts a public [ImpressionMetadata] to an internal [InternalImpressionMetadata] for creation.
 */
fun ImpressionMetadata.toInternal(
  dataProviderKey: DataProviderKey,
  impressionMetadataKey: ImpressionMetadataKey?,
): InternalImpressionMetadata {
  val source = this
  return internalImpressionMetadata {
    dataProviderResourceId = dataProviderKey.dataProviderId
    if (impressionMetadataKey != null) {
      impressionMetadataResourceId = impressionMetadataKey.impressionMetadataId
    }
    blobUri = source.blobUri
    blobTypeUrl = source.blobTypeUrl
    eventGroupReferenceId = source.eventGroupReferenceId
    cmmsModelLine = source.modelLine
    interval = source.interval
  }
}

/**
 * Converts an internal [InternalImpressionMetadataState] to a public [ImpressionMetadata.State].
 */
internal fun InternalImpressionMetadataState.toState(): ImpressionMetadata.State {
  return when (this) {
    InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_ACTIVE ->
      ImpressionMetadata.State.ACTIVE
    InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_DELETED ->
      ImpressionMetadata.State.DELETED
    ImpressionMetadataState.UNRECOGNIZED,
    InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_UNSPECIFIED ->
      error("Unrecognized state")
  }
}

/**
 * Converts a public [ImpressionMetadata.State] to an internal [InternalImpressionMetadataState].
 */
internal fun ImpressionMetadata.State.toInternal(): InternalImpressionMetadataState {
  return when (this) {
    ImpressionMetadata.State.ACTIVE ->
      InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_ACTIVE
    ImpressionMetadata.State.DELETED ->
      InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_DELETED
    ImpressionMetadata.State.UNRECOGNIZED,
    ImpressionMetadata.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
