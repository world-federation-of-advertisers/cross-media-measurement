/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as State

object Errors {
  const val DOMAIN = "edpaggregator.halo-cmm.org"

  enum class Reason {
    IMPRESSION_METADATA_NOT_FOUND,
    IMPRESSION_METADATA_ALREADY_EXISTS,
    REQUISITION_METADATA_NOT_FOUND,
    REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
    REQUISITION_METADATA_ALREADY_EXISTS,
    REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
    REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
    REQUISITION_METADATA_STATE_INVALID,
    DATA_PROVIDER_MISMATCH,
    ETAG_MISMATCH,
    REQUIRED_FIELD_NOT_SET,
    INVALID_FIELD_VALUE,
  }

  enum class Metadata(val key: String) {
    PARENT("parent"),
    DATA_PROVIDER("dataProvider"),
    REQUISITION_METADATA("requisitionMetadata"),
    CMMS_REQUISITION("cmmsRequisition"),
    BLOB_URI("blobUri"),
    REQUISITION_METADATA_STATE("requisitionMetadataState"),
    EXPECTED_REQUISITION_METADATA_STATES("expectedRequisitionMetadataStates"),
    REQUEST_ETAG("requestEtag"),
    ETAG("etag"),
    IMPRESSION_METADATA("impressionMetadata"),
    FIELD_NAME("fieldName"),
  }
}

sealed class ServiceException(
  private val reason: Errors.Reason,
  message: String,
  private val metadata: Map<Errors.Metadata, String>,
  cause: Throwable?,
) : Exception(message, cause) {
  override val message: String
    get() = super.message!!

  fun asStatusRuntimeException(code: Status.Code): StatusRuntimeException {
    val source = this
    val errorInfo = errorInfo {
      domain = Errors.DOMAIN
      reason = source.reason.name
      metadata.putAll(source.metadata.mapKeys { it.key.key })
    }
    return CommonErrors.buildStatusRuntimeException(code, message, errorInfo, this)
  }

  abstract class Factory<T : ServiceException> {
    protected abstract val reason: Errors.Reason

    protected abstract fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): T

    fun fromInternal(cause: StatusException): T {
      val errorInfo = requireNotNull(cause.errorInfo)
      require(errorInfo.domain == InternalErrors.DOMAIN)
      require(errorInfo.reason == reason.name)
      return fromInternal(InternalErrors.parseMetadata(errorInfo), cause)
    }
  }
}

class RequisitionMetadataNotFoundException(
  dataProviderResourceName: String,
  requisitionMetadataResourceName: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_NOT_FOUND,
    "RequisitionMetadata with Requisition Metadata Resource Name $requisitionMetadataResourceName for DataProvider with resource Name $dataProviderResourceName not found",
    mapOf(
      Errors.Metadata.DATA_PROVIDER to dataProviderResourceName,
      Errors.Metadata.REQUISITION_METADATA to requisitionMetadataResourceName,
    ),
    cause,
  ) {
  companion object : Factory<RequisitionMetadataNotFoundException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.REQUISITION_METADATA_NOT_FOUND

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): RequisitionMetadataNotFoundException {
      return RequisitionMetadataNotFoundException(
        internalMetadata.getValue(InternalErrors.Metadata.DATA_PROVIDER_RESOURCE_ID),
        internalMetadata.getValue(InternalErrors.Metadata.REQUISITION_METADATA_RESOURCE_ID),
        cause,
      )
    }
  }
}

class RequisitionMetadataNotFoundByCmmsRequisitionException(
  dataProviderResourceName: String,
  cmmsRequisition: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
    "RequisitionMetadata with CMMS Requisition $cmmsRequisition for DataProvider with resource Name $dataProviderResourceName not found",
    mapOf(
      Errors.Metadata.DATA_PROVIDER to dataProviderResourceName,
      Errors.Metadata.CMMS_REQUISITION to cmmsRequisition,
    ),
    cause,
  ) {
  companion object : Factory<RequisitionMetadataNotFoundByCmmsRequisitionException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): RequisitionMetadataNotFoundByCmmsRequisitionException {
      val dataProviderKey =
        DataProviderKey(
          internalMetadata.getValue(InternalErrors.Metadata.DATA_PROVIDER_RESOURCE_ID)
        )
      return RequisitionMetadataNotFoundByCmmsRequisitionException(
        dataProviderKey.toName(),
        internalMetadata.getValue(InternalErrors.Metadata.CMMS_REQUISITION),
        cause,
      )
    }
  }
}

class RequisitionMetadataAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
    "RequisitionMetadata already exists",
    emptyMap(),
    cause,
  )

class RequisitionMetadataAlreadyExistsByBlobUriException(
  dataProviderResourceName: String,
  blobUri: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
    "RequisitionMetadata with blob uri $blobUri for DataProvider with resource Name $dataProviderResourceName already exists",
    mapOf(
      Errors.Metadata.DATA_PROVIDER to dataProviderResourceName,
      Errors.Metadata.BLOB_URI to blobUri,
    ),
    cause,
  ) {
  companion object : Factory<RequisitionMetadataAlreadyExistsByBlobUriException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): RequisitionMetadataAlreadyExistsByBlobUriException {
      val dataProviderKey =
        DataProviderKey(
          internalMetadata.getValue(InternalErrors.Metadata.DATA_PROVIDER_RESOURCE_ID)
        )
      return RequisitionMetadataAlreadyExistsByBlobUriException(
        dataProviderKey.toName(),
        internalMetadata.getValue(InternalErrors.Metadata.BLOB_URI),
        cause,
      )
    }
  }
}

class RequisitionMetadataAlreadyExistsByCmmsRequisitionException(
  dataProviderResourceName: String,
  cmmsRequisition: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
    "RequisitionMetadata with cmms requisition $cmmsRequisition for DataProvider with resource Name $dataProviderResourceName already exists",
    mapOf(
      Errors.Metadata.DATA_PROVIDER to dataProviderResourceName,
      Errors.Metadata.CMMS_REQUISITION to cmmsRequisition,
    ),
    cause,
  ) {
  companion object : Factory<RequisitionMetadataAlreadyExistsByCmmsRequisitionException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): RequisitionMetadataAlreadyExistsByCmmsRequisitionException {
      return RequisitionMetadataAlreadyExistsByCmmsRequisitionException(
        internalMetadata.getValue(InternalErrors.Metadata.DATA_PROVIDER_RESOURCE_ID),
        internalMetadata.getValue(InternalErrors.Metadata.CMMS_REQUISITION),
        cause,
      )
    }
  }
}

class RequisitionMetadataInvalidStateException(
  dataProviderResourceName: String,
  requisitionMetadataResourceName: String,
  actualState: State,
  expectedStates: Set<State>,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_STATE_INVALID,
    "RequisitionMetadata is in state $actualState, expected one of $expectedStates",
    mapOf(
      Errors.Metadata.DATA_PROVIDER to dataProviderResourceName,
      Errors.Metadata.REQUISITION_METADATA to requisitionMetadataResourceName,
      Errors.Metadata.REQUISITION_METADATA_STATE to actualState.name,
      Errors.Metadata.EXPECTED_REQUISITION_METADATA_STATES to
        expectedStates.joinToString(",") { it.name },
    ),
    cause,
  )

class DataProviderMismatchException(
  parentDataProviderResourceName: String,
  actualDataProviderResourceName: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.DATA_PROVIDER_MISMATCH,
    "DataProvider $actualDataProviderResourceName does not match parent DataProvider $parentDataProviderResourceName",
    mapOf(
      Errors.Metadata.PARENT to parentDataProviderResourceName,
      Errors.Metadata.DATA_PROVIDER to actualDataProviderResourceName,
    ),
    cause,
  )

class EtagMismatchException(requestEtag: String, etag: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.ETAG_MISMATCH,
    "Request etag $requestEtag does not match actual etag $etag",
    mapOf(Errors.Metadata.REQUEST_ETAG to requestEtag, Errors.Metadata.ETAG to etag),
    cause,
  ) {
  companion object : Factory<EtagMismatchException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.ETAG_MISMATCH

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): EtagMismatchException {
      return EtagMismatchException(
        internalMetadata.getValue(InternalErrors.Metadata.REQUEST_ETAG),
        internalMetadata.getValue(InternalErrors.Metadata.ETAG),
        cause,
      )
    }
  }
}

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "$fieldName not set",
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )

class InvalidFieldValueException(
  fieldName: String,
  cause: Throwable? = null,
  buildMessage: (fieldName: String) -> String = { "Invalid value for field $fieldName" },
) :
  ServiceException(
    Errors.Reason.INVALID_FIELD_VALUE,
    buildMessage(fieldName),
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )

class ImpressionMetadataNotFoundException(
  impressionMetadataResourceName: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.IMPRESSION_METADATA_NOT_FOUND,
    "ImpressionMetadata $impressionMetadataResourceName not found",
    mapOf(Errors.Metadata.IMPRESSION_METADATA to impressionMetadataResourceName),
    cause,
  ) {
  companion object : Factory<ImpressionMetadataNotFoundException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): ImpressionMetadataNotFoundException {
      val impressionMetadataKey =
        ImpressionMetadataKey(
          internalMetadata.getValue(InternalErrors.Metadata.DATA_PROVIDER_RESOURCE_ID),
          internalMetadata.getValue(InternalErrors.Metadata.IMPRESSION_METADATA_RESOURCE_ID),
        )
      return ImpressionMetadataNotFoundException(impressionMetadataKey.toName(), cause)
    }
  }
}

class ImpressionMetadataAlreadyExistsException(blobUri: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
    "ImpressionMetadata with blobUri $blobUri already exists",
    mapOf(Errors.Metadata.BLOB_URI to blobUri),
    cause,
  ) {
  companion object : Factory<ImpressionMetadataAlreadyExistsException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): ImpressionMetadataAlreadyExistsException {
      return ImpressionMetadataAlreadyExistsException(
        internalMetadata.getValue(InternalErrors.Metadata.BLOB_URI),
        cause,
      )
    }
  }
}
