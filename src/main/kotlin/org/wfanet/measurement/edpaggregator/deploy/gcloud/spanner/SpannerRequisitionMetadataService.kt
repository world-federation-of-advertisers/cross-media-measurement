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
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByBlobUri
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByCmmsRequisition
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRequisitionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRequisitionMetadataAction
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
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState
import org.wfanet.measurement.internal.edpaggregator.StartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.copy
import java.util.UUID

class SpannerRequisitionMetadataService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RequisitionMetadataServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRequisitionMetadata(
    request: CreateRequisitionMetadataRequest
  ): RequisitionMetadata {
    grpcRequire(request.requisitionMetadata.dataProviderResourceId.isNotEmpty()) {
      "DataProviderResourceId is not specified."
    }

    val transactionRunner = databaseClient.readWriteTransaction(Options.tag("action=createRequisitionMetadata"))
    return try {
      transactionRunner.run { txn ->
        if (request.requestId.isNotEmpty()) {
          try {
            UUID.fromString(request.requestId)
          } catch (_: IllegalArgumentException) {
            failGrpc(Status.INVALID_ARGUMENT) { "request id is not a valid UUID." }
          }
          val existing =
            txn.getRequisitionMetadataByCreateRequestId(
              request.requisitionMetadata.dataProviderResourceId,
              request.requestId,
            )
          if (existing != null) {
            return@run existing.requisitionMetadata
          }
        }

        // TODO: Avoid conflicts for requisition metadata id and resource name.
        val requisitionMetadataId = idGenerator.generateId()
        val requisitionMetadataResourceId =
            request.requisitionMetadata.requisitionMetadataResourceId.ifEmpty {
              externalIdToApiId(idGenerator.generateId())
            }
        val actionId = idGenerator.generateId()

        val initialState =
          if (request.requisitionMetadata.refusalMessage.isNotEmpty()) RequisitionMetadataState.REQUISITION_METADATA_STATE_REFUSED
          else RequisitionMetadataState.REQUISITION_METADATA_STATE_STORED

        txn.insertRequisitionMetadata(
          requisitionMetadataId,
          requisitionMetadataResourceId,
          request.requisitionMetadata,
        )
        txn.insertRequisitionMetadataAction(
          request.requisitionMetadata.dataProviderResourceId,
          requisitionMetadataId,
          actionId,
          RequisitionMetadataState.REQUISITION_METADATA_STATE_UNSPECIFIED,
          initialState,
        )

        val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
        request.requisitionMetadata.copy {
          state = initialState
          this.requisitionMetadataResourceId = requisitionMetadataResourceId
          createTime = commitTimestamp
          updateTime = commitTimestamp
        }
      }
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        failGrpc(Status.ALREADY_EXISTS) { "RequisitionMetadata already exists" }
      }
      throw e
    }
  }

  override suspend fun getRequisitionMetadata(
    request: GetRequisitionMetadataRequest
  ): RequisitionMetadata {
    grpcRequire(request.dataProviderResourceId.isNotEmpty()) {
      "DataProviderResourceId is not specified."
    }
    grpcRequire(request.requisitionMetadataResourceId.isNotEmpty()) {
      "RequisitionMetadataResourceId is not specified."
    }

    return try {
      databaseClient.singleUse().use { txn ->
        txn.getRequisitionMetadataByResourceId(
          request.dataProviderResourceId,
          request.requisitionMetadataResourceId,
        )
      }
    } catch (e: )
  }

  override suspend fun lookupRequisitionMetadata(
    request: LookupRequisitionMetadataRequest
  ): RequisitionMetadata {
    val result =
      client.singleUse().run {
        when (request.lookupKeyCase) {
          LookupRequisitionMetadataRequest.LookupKeyCase.CMMS_REQUISITION ->
            getRequisitionMetadataByCmmsRequisition(
              request.dataProviderResourceId,
              request.cmmsRequisition,
            )
          LookupRequisitionMetadataRequest.LookupKeyCase.BLOB_URI ->
            getRequisitionMetadataByBlobUri(request.dataProviderResourceId, request.blobUri)
          else -> failGrpc(Status.INVALID_ARGUMENT) { "Lookup key not specified" }
        }
      }
    return result?.requisitionMetadata
      ?: failGrpc(Status.NOT_FOUND) { "RequisitionMetadata not found" }
  }

  override suspend fun fetchLatestCmmsCreateTime(
    request: FetchLatestCmmsCreateTimeRequest
  ): Timestamp {
    grpcRequire(request.dataProviderResourceId.isNotEmpty()) {
      "DataProviderResourceId is not specified."
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

    val struct = client.singleUse().executeQuery(statement).firstOrNull()
    return struct?.getTimestamp("CmmsCreateTime")?.toProto() ?: Timestamp.getDefaultInstance()
  }

  override suspend fun queueRequisitionMetadata(
    request: QueueRequisitionMetadataRequest
  ): RequisitionMetadata {
    grpcRequire(request.workItem.isNotEmpty()) { "WorkItem is not specified." }
    return transitionState(
      request.dataProviderResourceId,
      request.requisitionMetadataResourceId,
      request.etag,
      State.STORED,
      State.QUEUED,
    ) {
      set("WorkItem").to(request.workItem)
    }
  }

  override suspend fun startProcessingRequisitionMetadata(
    request: StartProcessingRequisitionMetadataRequest
  ): RequisitionMetadata {
    return transitionState(
      request.dataProviderResourceId,
      request.requisitionMetadataResourceId,
      request.etag,
      State.QUEUED,
      State.PROCESSING,
    )
  }

  override suspend fun fulfillRequisitionMetadata(
    request: FulfillRequisitionMetadataRequest
  ): RequisitionMetadata {
    return transitionState(
      request.dataProviderResourceId,
      request.requisitionMetadataResourceId,
      request.etag,
      State.PROCESSING,
      State.FULFILLED,
    )
  }

  override suspend fun refuseRequisitionMetadata(
    request: RefuseRequisitionMetadataRequest
  ): RequisitionMetadata {
    grpcRequire(request.refusalMessage.isNotEmpty()) { "RefusalMessage is not specified." }
    // Refusal can happen from STORED or PROCESSING state.
    val current =
      getRequisitionMetadata(
        GetRequisitionMetadataRequest.newBuilder()
          .setDataProviderResourceId(request.dataProviderResourceId)
          .setRequisitionMetadataResourceId(request.requisitionMetadataResourceId)
          .build()
      )
    grpcRequire(current.state == State.STORED || current.state == State.PROCESSING) {
      "Cannot refuse a requisition in state ${current.state}"
    }

    return transitionState(
      request.dataProviderResourceId,
      request.requisitionMetadataResourceId,
      request.etag,
      current.state,
      State.REFUSED,
    ) {
      set("RefusalMessage").to(request.refusalMessage)
    }
  }

  private suspend fun transitionState(
    dataProviderResourceId: String,
    requisitionMetadataResourceId: String,
    etag: String,
    from: State,
    to: State,
    block: (Mutation.WriteBuilder.() -> Unit)? = null,
  ): RequisitionMetadata {
    grpcRequire(dataProviderResourceId.isNotEmpty()) { "DataProviderResourceId is not specified." }
    grpcRequire(requisitionMetadataResourceId.isNotEmpty()) {
      "RequisitionMetadataResourceId is not specified."
    }
    grpcRequire(etag.isNotEmpty()) { "Etag is not specified." }

    return client.readWriteTransaction().execute { transactionContext ->
      val result =
        transactionContext.getRequisitionMetadataByResourceId(
          dataProviderResourceId,
          requisitionMetadataResourceId,
        )
          ?: failGrpc(Status.NOT_FOUND) { "RequisitionMetadata not found" }

      val (metadata, requisitionMetadataId) = result

      grpcRequire(metadata.state == from) {
        "RequisitionMetadata is in state ${metadata.state}, expected $from"
      }

      val expectedEtag = Timestamps.toString(metadata.updateTime)
      grpcRequire(etag == expectedEtag) { "Etag mismatch. Expected $expectedEtag, got $etag" }

      val actionId = idGenerator.generateInternalId()
      transactionContext.updateRequisitionMetadata(
        dataProviderResourceId,
        requisitionMetadataId,
        to,
        block,
      )
      transactionContext.insertRequisitionMetadataAction(
        dataProviderResourceId,
        requisitionMetadataId,
        actionId,
        from,
        to,
      )

      transactionContext.getRequisitionMetadataByResourceId(
        dataProviderResourceId,
        requisitionMetadataResourceId,
      )!!
        .requisitionMetadata
    }
  }
}
