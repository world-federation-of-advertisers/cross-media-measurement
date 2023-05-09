package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class SetModelLineHoldbackModelLine(private val request: SetModelLineHoldbackModelLineRequest) :
  SpannerWriter<ModelLine, ModelLine>() {

  override suspend fun TransactionScope.runTransaction(): ModelLine {
    val modelLineResult =
      ModelLineReader()
        .readByExternalModelLineId(
          transactionContext,
          ExternalId(request.externalModelProviderId),
          ExternalId(request.externalModelSuiteId),
          ExternalId(request.externalModelLineId)
        )
        ?: throw ModelLineNotFoundException(
          ExternalId(request.externalModelProviderId),
          ExternalId(request.externalModelSuiteId),
          ExternalId(request.externalModelLineId)
        )

    if (modelLineResult.modelLine.type != ModelLine.Type.PROD) {
      throw ModelLineTypeIllegalException(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId),
        modelLineResult.modelLine.type
      ) {
        "Only ModelLine with type == PROD can have a Holdback ModelLine."
      }
    }

    val holdbackModelLineResult =
      ModelLineReader()
        .readByExternalModelLineId(
          transactionContext,
          ExternalId(request.externalHoldbackModelProviderId),
          ExternalId(request.externalHoldbackModelSuiteId),
          ExternalId(request.externalHoldbackModelLineId)
        )
        ?: throw ModelLineNotFoundException(
          ExternalId(request.externalHoldbackModelProviderId),
          ExternalId(request.externalHoldbackModelSuiteId),
          ExternalId(request.externalHoldbackModelLineId)
        )

    if (holdbackModelLineResult.modelLine.type != ModelLine.Type.HOLDBACK) {
      throw ModelLineTypeIllegalException(
        ExternalId(holdbackModelLineResult.modelLine.externalModelProviderId),
        ExternalId(holdbackModelLineResult.modelLine.externalModelSuiteId),
        ExternalId(holdbackModelLineResult.modelLine.externalModelLineId),
        holdbackModelLineResult.modelLine.type
      ) {
        "Only ModelLine with type == HOLDBACK can be set as Holdback ModelLine."
      }
    }

    transactionContext.bufferUpdateMutation("ModelLines") {
      set("ModelLineId" to modelLineResult.modelLineId.value)
      set("ModelSuiteId" to modelLineResult.modelSuiteId.value)
      set("ModelProviderId" to modelLineResult.modelProviderId.value)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("HoldbackModelLine" to holdbackModelLineResult.modelLineId.value)
    }

    return modelLineResult.modelLine.copy {
      externalHoldbackModelLineId = request.externalHoldbackModelLineId
    }
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
