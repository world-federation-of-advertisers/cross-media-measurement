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
package org.wfanet.measurement.edpaggregator.service.internal

import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState

object Errors {
  const val DOMAIN = "internal.edpaggregator.halo-cmm.org"

  enum class Reason {
    DATA_PROVIDER_MISMATCH,
    IMPRESSION_METADATA_NOT_FOUND,
    IMPRESSION_METADATA_ALREADY_EXISTS,
    IMPRESSION_METADATA_STATE_INVALID,
    REQUISITION_METADATA_NOT_FOUND,
    REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
    REQUISITION_METADATA_ALREADY_EXISTS,
    REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
    REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
    REQUISITION_METADATA_STATE_INVALID,
    ETAG_MISMATCH,
    REQUIRED_FIELD_NOT_SET,
    INVALID_FIELD_VALUE,
  }

  enum class Metadata(val key: String) {
    EXPECTED_DATA_PROVIDER_RESOURCE_ID("expectedDataProviderResourceId"),
    DATA_PROVIDER_RESOURCE_ID("dataProviderResourceId"),
    IMPRESSION_METADATA_RESOURCE_ID("impressionMetadataResourceId"),
    REQUISITION_METADATA_RESOURCE_ID("requisitionMetadataResourceId"),
    CMMS_REQUISITION("cmmsRequisition"),
    BLOB_URI("blobUri"),
    IMPRESSION_METADATA_STATE("impressionMetadataState"),
    REQUISITION_METADATA_STATE("requisitionMetadataState"),
    EXPECTED_REQUISITION_METADATA_STATES("expectedRequisitionMetadataStates"),
    REQUEST_ETAG("requestEtag"),
    ETAG("etag"),
    FIELD_NAME("fieldName");

    companion object {
      private val METADATA_BY_KEY by lazy { entries.associateBy { it.key } }

      fun fromKey(key: String): Metadata = METADATA_BY_KEY.getValue(key)
    }
  }

  /**
   * Returns the [Reason] extracted from [exception], or `null` if [exception] is not this type of
   * error.
   */
  fun getReason(exception: StatusException): Reason? {
    val errorInfo = exception.errorInfo ?: return null
    return getReason(errorInfo)
  }

  /**
   * Returns the [Reason] extracted from [errorInfo], or `null` if [errorInfo] is not this type of
   * error.
   */
  fun getReason(errorInfo: ErrorInfo): Reason? {
    if (errorInfo.domain != DOMAIN) {
      return null
    }

    return Reason.valueOf(errorInfo.reason)
  }

  fun parseMetadata(errorInfo: ErrorInfo): Map<Metadata, String> {
    require(errorInfo.domain == DOMAIN) { "Error domain is not $DOMAIN" }
    return errorInfo.metadataMap.mapKeys { Metadata.fromKey(it.key) }
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
}

class DataProviderMismatchException(
  expectedDataProviderResourceId: String,
  actualDataProviderResourceId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.DATA_PROVIDER_MISMATCH,
    "Expect DataProvider with resource ID $expectedDataProviderResourceId but got $actualDataProviderResourceId",
    mapOf(
      Errors.Metadata.EXPECTED_DATA_PROVIDER_RESOURCE_ID to expectedDataProviderResourceId,
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to actualDataProviderResourceId,
    ),
    cause,
  )

class ImpressionMetadataNotFoundException(
  dataProviderResourceId: String,
  impressionMetadataResourceId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.IMPRESSION_METADATA_NOT_FOUND,
    "ImpressionMetadata with resource ID $impressionMetadataResourceId for DataProvider with resource ID $dataProviderResourceId not found",
    mapOf(
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to dataProviderResourceId,
      Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID to impressionMetadataResourceId,
    ),
    cause,
  )

class ImpressionMetadataAlreadyExistsException(blobUri: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
    "ImpressionMetadata already exists",
    mapOf(Errors.Metadata.BLOB_URI to blobUri),
    cause,
  )

class ImpressionMetadataStateInvalidException(
  dataProviderResourceId: String,
  impressionMetadataResourceId: String,
  actualState: ImpressionMetadataState,
  expectedStates: Set<ImpressionMetadataState>,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.IMPRESSION_METADATA_STATE_INVALID,
    "ImpressionMetadata is in state $actualState, expected one of $expectedStates",
    mapOf(
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to dataProviderResourceId,
      Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID to impressionMetadataResourceId,
      Errors.Metadata.IMPRESSION_METADATA_STATE to actualState.name,
    ),
    cause,
  )

class RequisitionMetadataNotFoundException(
  dataProviderResourceId: String,
  requisitionMetadataResourceId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_NOT_FOUND,
    "RequisitionMetadata with Requisition Metadata Resource ID $requisitionMetadataResourceId for DataProvider with resource ID $dataProviderResourceId not found",
    mapOf(
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to dataProviderResourceId,
      Errors.Metadata.REQUISITION_METADATA_RESOURCE_ID to requisitionMetadataResourceId,
    ),
    cause,
  )

class RequisitionMetadataNotFoundByCmmsRequisitionException(
  dataProviderResourceId: String,
  cmmsRequisition: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
    "RequisitionMetadata with CMMS Requisition $cmmsRequisition for DataProvider with resource ID $dataProviderResourceId not found",
    mapOf(
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to dataProviderResourceId,
      Errors.Metadata.CMMS_REQUISITION to cmmsRequisition,
    ),
    cause,
  )

class RequisitionMetadataAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
    "RequisitionMetadata already exists",
    emptyMap(),
    cause,
  )

class RequisitionMetadataAlreadyExistsByCmmsRequisitionException(
  dataProviderResourceId: String,
  cmmsRequisition: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
    "RequisitionMetadata with CMMS Requisition $cmmsRequisition for DataProvider with resource ID $dataProviderResourceId already exists",
    mapOf(
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to dataProviderResourceId,
      Errors.Metadata.CMMS_REQUISITION to cmmsRequisition,
    ),
    cause,
  )

class RequisitionMetadataAlreadyExistsByBlobUriException(
  dataProviderResourceId: String,
  blobUri: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
    "RequisitionMetadata with blob uri $blobUri for DataProvider with resource ID $dataProviderResourceId already exists",
    mapOf(
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to dataProviderResourceId,
      Errors.Metadata.BLOB_URI to blobUri,
    ),
    cause,
  )

class RequisitionMetadataInvalidStateException(
  dataProviderResourceId: String,
  requisitionMetadataResourceId: String,
  actualState: RequisitionMetadataState,
  expectedStates: Set<RequisitionMetadataState>,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REQUISITION_METADATA_STATE_INVALID,
    "RequisitionMetadata is in state $actualState, expected one of $expectedStates",
    mapOf(
      Errors.Metadata.DATA_PROVIDER_RESOURCE_ID to dataProviderResourceId,
      Errors.Metadata.REQUISITION_METADATA_RESOURCE_ID to requisitionMetadataResourceId,
      Errors.Metadata.REQUISITION_METADATA_STATE to actualState.name,
      Errors.Metadata.EXPECTED_REQUISITION_METADATA_STATES to
        expectedStates.joinToString(",") { it.name },
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
  companion object {
    /**
     * Checks whether [requestEtag] matches [etag].
     *
     * @throws EtagMismatchException if the etags do not match
     */
    fun check(requestEtag: String, etag: String) {
      if (requestEtag != etag) {
        throw EtagMismatchException(requestEtag, etag)
      }
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
