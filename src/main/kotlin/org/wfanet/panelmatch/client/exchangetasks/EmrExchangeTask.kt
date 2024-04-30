package org.wfanet.panelmatch.client.exchangetasks

import com.google.protobuf.ByteString
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.StorageType
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.exchangetasks.emr.EmrExchangeTaskService

class EmrExchangeTask(
  private val emrExchangeTaskService: EmrExchangeTaskService,
  private val exchangeId: String,
  private val exchangeStepIndex: Int,
  private val exchangeStepAttempt: CanonicalExchangeStepAttemptKey,
  private val exchangeDate: LocalDate,
): ExchangeTask {
  override suspend fun execute(input: Map<String, StorageClient.Blob>): Map<String, Flow<ByteString>> {
    emrExchangeTaskService.runPanelExchangeStepOnEmrApp(
      exchangeId,
      exchangeStepIndex,
      exchangeStepAttempt,
      exchangeDate,
    )

    // TODO: Once the job is completed return the output of the job (output manifest)

    TODO("Not yet implemented")
  }
}
