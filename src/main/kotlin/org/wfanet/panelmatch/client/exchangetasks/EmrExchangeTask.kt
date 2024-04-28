package org.wfanet.panelmatch.client.exchangetasks

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.exchangetasks.emr.EmrExchangeTaskService

class EmrExchangeTask(
  private val emrExchangeTaskService: EmrExchangeTaskService,
): ExchangeTask {
  override suspend fun execute(input: Map<String, StorageClient.Blob>): Map<String, Flow<ByteString>> {
    // TODO: Create input for the EMR serverless job

    emrExchangeTaskService.runPanelExchangeStepOnEmrApp(
      "",
      0,
      "",
      "",
      "",
    )

    // TODO: Once the job is completed return the output of the job (output manifest)

    TODO("Not yet implemented")
  }
}
