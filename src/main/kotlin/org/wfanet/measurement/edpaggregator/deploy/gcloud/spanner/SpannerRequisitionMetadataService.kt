/*
 * Copyright 2024 The Cross-Media Measurement Authors
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
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Value
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.idGenerator
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RequisitionMetadataReader
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
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata.State
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.StartProcessingRequisitionMetadataRequest

class SpannerRequisitionMetadataService(
  private val idGenerator: org.wfanet.measurement.common.IdGenerator,
  private val client: AsyncDatabaseClient,
) : RequisitionMetadataServiceCoroutineImplBase() {

  override suspend fun createRequisitionMetadata(
    request: CreateRequisitionMetadataRequest
  ): RequisitionMetadata {
    grpcRequire(request.requisitionMetadata.dataProviderResourceId.isNotEmpty()) {
      "DataProviderResourceId is not specified."
    }
    grpcRequire(request.requisitionMetadata.requisitionMetadataResourceId.isNotEmpty()) {
      "RequisitionMetadataResourceId is not specified."
    }

    try {
      return client.readWriteTransaction().execute { transactionContext ->
        val reader = RequisitionMetadataReader(transactionContext)
        if (request.requestId.isNotEmpty()) {
          val existing =
            reader.readByCreateRequestId(
              request.requisitionMetadata.dataProviderResourceId,
              request.requestId,
            )
          if (existing != null) {
            return@execute existing.requisitionMetadata
          }
        }

        val requisitionMetadataId = idGenerator.generateInternalId()
        val actionId = idGenerator.generateInternalId()

        val initialState =
          if (request.requisitionMetadata.refusalMessage.isNotEmpty()) State.REFUSED
          else State.STORED

        val populatedMetadata =
          request.requisitionMetadata.toBuilder().setState(initialState).build()

        transactionContext.insertRequisitionMetadata(
          requisitionMetadataId,
          populatedMetadata,
          request.requestId.ifEmpty { null },
        )
        transactionContext.insertRequisitionMetadataAction(
          populatedMetadata.dataProviderResourceId,
          requisitionMetadataId,
          actionId,
          State.REQUISITION_METADATA_STATE_UNSPECIFIED,
          initialState,
        )
        reader
          .readByResourceId(
            populatedMetadata.dataProviderResourceId,
            populatedMetadata.requisitionMetadataResourceId,
          )!!
          .requisitionMetadata
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
    return RequisitionMetadataReader(client.singleUse())
      .readByResourceId(request.dataProviderResourceId, request.requisitionMetadataResourceId)
      ?.requisitionMetadata ?: failGrpc(Status.NOT_FOUND) { "RequisitionMetadata not found" }
  }

  override suspend fun lookupRequisitionMetadata(
    request: LookupRequisitionMetadataRequest
  ): RequisitionMetadata {
    val reader = RequisitionMetadataReader(client.singleUse())
    val result =
      when (request.lookupKeyCase) {
        LookupRequisitionMetadataRequest.LookupKeyCase.CMMS_REQUISITION ->
          reader.readByCmmsRequisition(request.dataProviderResourceId, request.cmmsRequisition)
        LookupRequisitionMetadataRequest.LookupKeyCase.BLOB_URI ->
          reader.readByBlobUri(request.dataProviderResourceId, request.blobUri)
        else -> failGrpc(Status.INVALID_ARGUMENT) { "Lookup key not specified" }
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
      val reader = RequisitionMetadataReader(transactionContext)
      val result =
        reader.readByResourceId(dataProviderResourceId, requisitionMetadataResourceId)
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

      reader
        .readByResourceId(dataProviderResourceId, requisitionMetadataResourceId)!!
        .requisitionMetadata
    }
  }
}

private fun AsyncDatabaseClient.TransactionContext.insertRequisitionMetadata(
  requisitionMetadataId: Long,
  requisitionMetadata: RequisitionMetadata,
  createRequestId: String?,
) {
  buffer(
    Mutation.newInsertBuilder("RequisitionMetadata")
      .set("DataProviderResourceId")
      .to(requisitionMetadata.dataProviderResourceId)
      .set("RequisitionMetadataId")
      .to(requisitionMetadataId)
      .set("RequisitionMetadataResourceId")
      .to(requisitionMetadata.requisitionMetadataResourceId)
      .set("CreateRequestId")
      .to(createRequestId)
      .set("CmmsRequisition")
      .to(requisitionMetadata.cmmsRequisition)
      .set("BlobUri")
      .to(requisitionMetadata.blobUri)
      .set("GroupId")
      .to(requisitionMetadata.groupId)
      .set("CmmsCreateTime")
      .to(requisitionMetadata.cmmsCreateTime)
      .set("Report")
      .to(requisitionMetadata.report)
      .set("State")
      .to(requisitionMetadata.stateValue.toLong())
      .set("WorkItem")
      .to(requisitionMetadata.workItem.ifEmpty { null })
      .set("CreateTime")
      .to(Value.COMMIT_TIMESTAMP)
      .set("UpdateTime")
      .to(Value.COMMIT_TIMESTAMP)
      .set("RefusalMessage")
      .to(requisitionMetadata.refusalMessage.ifEmpty { null })
      .build()
  )
}

private fun AsyncDatabaseClient.TransactionContext.updateRequisitionMetadata(
  dataProviderResourceId: String,
  requisitionMetadataId: Long,
  state: State,
  block: (Mutation.WriteBuilder.() -> Unit)? = null,
) {
  val mutation =
    Mutation.newUpdateBuilder("RequisitionMetadata")
      .set("DataProviderResourceId")
      .to(dataProviderResourceId)
      .set("RequisitionMetadataId")
      .to(requisitionMetadataId)
      .set("State")
      .to(state.number.toLong())
      .set("UpdateTime")
      .to(Value.COMMIT_TIMESTAMP)

  block?.invoke(mutation)

  buffer(mutation.build())
}

private fun AsyncDatabaseClient.TransactionContext.insertRequisitionMetadataAction(
  dataProviderResourceId: String,
  requisitionMetadataId: Long,
  actionId: Long,
  previousState: State,
  currentState: State,
) {
  buffer(
    Mutation.newInsertBuilder("RequisitionMetadataActions")
      .set("DataProviderResourceId")
      .to(dataProviderResourceId)
      .set("RequisitionMetadataId")
      .to(requisitionMetadataId)
      .set("ActionId")
      .to(actionId)
      .set("CreateTime")
      .to(Value.COMMIT_TIMESTAMP)
      .set("PreviousState")
      .to(previousState.number.toLong())
      .set("CurrentState")
      .to(currentState.number.toLong())
      .build()
  )
}
