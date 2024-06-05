package org.wfanet.panelmatch.client.exchangetasks

import com.google.protobuf.ByteString
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.exchangetasks.emr.EmrExchangeTaskService

class EmrExchangeTask(
  private val emrExchangeTaskService: EmrExchangeTaskService,
  private val exchangeId: String,
  private val exchangeStepName: String,
  private val exchangeStepIndex: Int,
  private val exchangeStepAttempt: CanonicalExchangeStepAttemptKey,
  private val exchangeDate: LocalDate,
): ExchangeTask {
  override suspend fun execute(input: Map<String, StorageClient.Blob>): Map<String, Flow<ByteString>> {
    try {
      emrExchangeTaskService.runPanelExchangeStepOnEmrApp(
        exchangeId,
        exchangeStepName,
        exchangeStepIndex,
        exchangeStepAttempt,
        exchangeDate,
      )

      return emptyMap()
    } catch (e: Exception) {
      throw ExchangeTaskFailedException.ofPermanent(e)
    }
  }

  override fun skipReadInput(): Boolean = true
}
