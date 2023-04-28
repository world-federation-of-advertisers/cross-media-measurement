package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.copy

class CreateModelRelease(private val modelRelease: ModelRelease) :
  SpannerWriter<ModelRelease, ModelRelease>() {

  override suspend fun TransactionScope.runTransaction(): ModelRelease {
    val internalModelReleaseId = idGenerator.generateInternalId()
    val externalModelReleaseId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelReleases") {
      set("ModelReleaseId" to internalModelReleaseId)
      set("ExternalModelReleaseId" to externalModelReleaseId)
    }

    return modelRelease.copy { this.externalModelReleaseId = externalModelReleaseId.value }
  }

  override fun ResultScope<ModelRelease>.buildResult(): ModelRelease {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
