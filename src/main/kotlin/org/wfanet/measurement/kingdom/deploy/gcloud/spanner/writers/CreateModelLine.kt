package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException

class CreateModelLine(private val modelLine: ModelLine) : SpannerWriter<ModelLine, ModelLine>() {

  override suspend fun TransactionScope.runTransaction(): ModelLine {

    val modelSuiteId: InternalId = readModelSuiteId(ExternalId(modelLine.externalModelSuiteId))

    val internalModelLineId = idGenerator.generateInternalId()
    val externalModelLineId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelLines") {
      set("ModelSuiteId" to modelSuiteId)
      set("ModelLineId" to internalModelLineId)
      set("ExternalModelLineId" to externalModelLineId)
      if (modelLine.displayName.isNotBlank()) {
        set("DisplayName" to modelLine.displayName)
      }
      if (modelLine.description.isNotBlank()) {
        set("Description" to modelLine.description)
      }
      set("ActiveStartTime" to modelLine.activeStartTime.toGcloudTimestamp())
      if (modelLine.hasActiveEndTime()) {
        set("ActiveEndTime" to modelLine.activeEndTime.toGcloudTimestamp())
      }
      set("Type" to modelLine.type)
      if (modelLine.externalHoldbackModelLineId != null) {
        set("HoldbackModelLine" to readModelLineId(ExternalId(modelLine.externalModelLineId)))
      }
    }

    return modelLine.copy { this.externalModelLineId = externalModelLineId.value }
  }

  private suspend fun TransactionScope.readModelSuiteId(
    externalModelSuiteId: ExternalId
  ): InternalId {
    val column = "ModelSuiteId"
    return transactionContext
      .readRowUsingIndex(
        "ModelSuites",
        "ExternalModelSuiteId",
        Key.of(externalModelSuiteId.value),
        column
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw ModelSuiteNotFoundException(externalModelSuiteId) {
        "ModelSuite with external ID $externalModelSuiteId not found"
      }
  }

  private suspend fun TransactionScope.readModelLineId(
    externalModelLineId: ExternalId
  ): InternalId {
    val column = "ModelLineId"
    return transactionContext
      .readRowUsingIndex(
        "ModelLines",
        "ExternalModelLineId",
        Key.of(externalModelLineId.value),
        column
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw ModelLineNotFoundException(externalModelLineId) {
        "ModelSuite with external ID $externalModelLineId not found"
      }
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
