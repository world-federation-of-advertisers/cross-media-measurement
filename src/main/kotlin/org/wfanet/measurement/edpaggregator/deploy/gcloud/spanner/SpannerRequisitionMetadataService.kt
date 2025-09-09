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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RequisitionMetadataResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByBlobUri
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByCmmsRequisition
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRequisitionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRequisitionMetadataAction
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRequisitionMetadataState
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.RequisitionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.RequisitionMetadataNotFoundException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.CreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.FetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.FulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.GetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.LookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.QueueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.StartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.copy

class SpannerRequisitionMetadataService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RequisitionMetadataServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRequisitionMetadata(
    request: CreateRequisitionMetadataRequest
  ): RequisitionMetadata {
    validateRequisitionMetadata(request.requisitionMetadata)

    val initialState =
      if (request.requisitionMetadata.refusalMessage.isNotEmpty())
        State.REQUISITION_METADATA_STATE_REFUSED
      else State.REQUISITION_METADATA_STATE_STORED

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createRequisitionMetadata"))
    val requisitionMetadata: RequisitionMetadata =
      try {
        transactionRunner.run { txn ->
          if (request.requestId.isNotEmpty()) {
            try {
              UUID.fromString(request.requestId)
            } catch (_: IllegalArgumentException) {
              throw InvalidFieldValueException("request_id")
                .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
            }
            val existing: RequisitionMetadataResult? =
              txn.getRequisitionMetadataByCreateRequestId(
                request.requisitionMetadata.dataProviderResourceId,
                request.requestId,
              )
            if (existing != null) {
              return@run existing.requisitionMetadata
            }
          }

          val requisitionMetadataId = idGenerator.generateId()
          val requisitionMetadataResourceId =
            request.requisitionMetadata.requisitionMetadataResourceId.ifEmpty {
              externalIdToApiId(idGenerator.generateId())
            }
          val actionId = idGenerator.generateId()

          txn.insertRequisitionMetadata(
            requisitionMetadataId,
            requisitionMetadataResourceId,
            initialState,
            request.requisitionMetadata,
            request.requestId,
          )
          txn.insertRequisitionMetadataAction(
            request.requisitionMetadata.dataProviderResourceId,
            requisitionMetadataId,
            actionId,
            State.REQUISITION_METADATA_STATE_UNSPECIFIED,
            initialState,
          )
          request.requisitionMetadata.copy {
            state = initialState
            this.requisitionMetadataResourceId = requisitionMetadataResourceId
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RequisitionMetadataAlreadyExistsException(e)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    return if (requisitionMetadata.hasCreateTime()) {
      requisitionMetadata
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      requisitionMetadata.copy {
        createTime = commitTimestamp
        updateTime = commitTimestamp
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    }
  }

  private fun validateRequisitionMetadata(requisitionMetadata: RequisitionMetadata) {
    if (requisitionMetadata.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requisitionMetadata.cmmsRequisition.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_requisition")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requisitionMetadata.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("blob_uri")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requisitionMetadata.blobTypeUrl.isEmpty()) {
      throw RequiredFieldNotSetException("blob_type_url")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requisitionMetadata.groupId.isEmpty()) {
      throw RequiredFieldNotSetException("group_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requisitionMetadata.report.isEmpty()) {
      throw RequiredFieldNotSetException("report")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  override suspend fun getRequisitionMetadata(
    request: GetRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requisitionMetadataResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("requisition_metadata_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        txn
          .getRequisitionMetadataByResourceId(
            request.dataProviderResourceId,
            request.requisitionMetadataResourceId,
          )
          .requisitionMetadata
      }
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun lookupRequisitionMetadata(
    request: LookupRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (request.lookupKeyCase) {
          LookupRequisitionMetadataRequest.LookupKeyCase.CMMS_REQUISITION ->
            txn.getRequisitionMetadataByCmmsRequisition(
              request.dataProviderResourceId,
              request.cmmsRequisition,
            )
          LookupRequisitionMetadataRequest.LookupKeyCase.BLOB_URI ->
            txn.getRequisitionMetadataByBlobUri(request.dataProviderResourceId, request.blobUri)
          LookupRequisitionMetadataRequest.LookupKeyCase.LOOKUPKEY_NOT_SET ->
            throw RequiredFieldNotSetException("lookup_key")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }.requisitionMetadata
      }
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun fetchLatestCmmsCreateTime(
    request: FetchLatestCmmsCreateTimeRequest
  ): Timestamp {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val sql =
      """
      SELECT CmmsCreateTime FROM RequisitionMetadata
      WHERE DataProviderResourceId = @dataProviderResourceId
      ORDER BY CmmsCreateTime DESC
      LIMIT 1
      """
        .trimIndent()

    val statement =
      statement(sql) { bind("dataProviderResourceId").to(request.dataProviderResourceId) }

    val struct = databaseClient.singleUse().executeQuery(statement).firstOrNull()
    return struct?.getTimestamp("CmmsCreateTime")?.toProto() ?: Timestamp.getDefaultInstance()
  }

  override suspend fun queueRequisitionMetadata(
    request: QueueRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.workItem.isEmpty()) {
      throw RequiredFieldNotSetException("work_item")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val requisitionMetadata =
      transitionState(
        request.dataProviderResourceId,
        request.requisitionMetadataResourceId,
        request.etag,
        State.REQUISITION_METADATA_STATE_QUEUED,
      ) {
        set("WorkItem").to(request.workItem)
      }
    return requisitionMetadata.copy { workItem = request.workItem }
  }

  override suspend fun startProcessingRequisitionMetadata(
    request: StartProcessingRequisitionMetadataRequest
  ): RequisitionMetadata {
    return transitionState(
      request.dataProviderResourceId,
      request.requisitionMetadataResourceId,
      request.etag,
      State.REQUISITION_METADATA_STATE_PROCESSING,
    )
  }

  override suspend fun fulfillRequisitionMetadata(
    request: FulfillRequisitionMetadataRequest
  ): RequisitionMetadata {
    return transitionState(
      request.dataProviderResourceId,
      request.requisitionMetadataResourceId,
      request.etag,
      State.REQUISITION_METADATA_STATE_FULFILLED,
    )
  }

  override suspend fun refuseRequisitionMetadata(
    request: RefuseRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.refusalMessage.isEmpty()) {
      throw RequiredFieldNotSetException("refusal_message")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val requisitionMetadata =
      transitionState(
        request.dataProviderResourceId,
        request.requisitionMetadataResourceId,
        request.etag,
        State.REQUISITION_METADATA_STATE_REFUSED,
      ) {
        set("RefusalMessage").to(request.refusalMessage)
      }
    return requisitionMetadata.copy { refusalMessage = request.refusalMessage }
  }

  /** Returns the copy of original RequisitionMetadata with updated state and updateTime. */
  private suspend fun transitionState(
    dataProviderResourceId: String,
    requisitionMetadataResourceId: String,
    requestEtag: String,
    nextState: State,
    block: (Mutation.WriteBuilder.() -> Unit)? = null,
  ): RequisitionMetadata {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requisitionMetadataResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("requisition_metadata_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=queueRequisitionMetadata"))
    return try {
      val updatedRequisitionMetadata =
        transactionRunner.run { txn ->
          val result =
            txn.getRequisitionMetadataByResourceId(
              dataProviderResourceId,
              requisitionMetadataResourceId,
            )
          val requisitionMetadata = result.requisitionMetadata

          if (requestEtag != requisitionMetadata.etag) {
            throw EtagMismatchException(requestEtag, requisitionMetadata.etag)
              .asStatusRuntimeException(Status.Code.ABORTED)
          }
          txn.updateRequisitionMetadataState(
            dataProviderResourceId,
            result.requisitionMetadataId,
            nextState,
            block,
          )
          txn.insertRequisitionMetadataAction(
            dataProviderResourceId,
            result.requisitionMetadataId,
            idGenerator.generateId(),
            requisitionMetadata.state,
            nextState,
          )
          requisitionMetadata
        }
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      updatedRequisitionMetadata.copy {
        state = nextState
        updateTime = commitTimestamp
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }
}
