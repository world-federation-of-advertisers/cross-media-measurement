package org.wfanet.panelmatch.client.exchangetasks.emr

import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.storage.toStringUtf8

private const val EMR_EXCHANGE_TASK_APP_NAME = "panel-exchange-beam-task"

class EmrExchangeTaskService(
  val exchangeTaskAppIdPath: String,
  val storageClient: StorageClient,
  val emrServerlessClientService: EmrServerlessClientService
) {

  private var appId: String

  init {
    val exchangeTaskAppIdBlob = runBlocking { storageClient.getBlob(exchangeTaskAppIdPath) }

    appId = if (exchangeTaskAppIdBlob != null) {
      runBlocking { exchangeTaskAppIdBlob.toStringUtf8() }
    } else {
      val id = emrServerlessClientService.createApplication(EMR_EXCHANGE_TASK_APP_NAME)
      runBlocking { storageClient.writeBlob(exchangeTaskAppIdPath, id.toByteStringUtf8()) }
      id
    }
  }

  suspend fun runPanelExchangeStepOnEmrApp(
    exchangeWorkflowBlobKey: String,
    exchangeStepIndex: Int,
    exchangeStepAttemptResourceId: String,
    exchangeDate: String,
    storageType: String,
  ) {
    val started = emrServerlessClientService.startOrStopApplication(appId, true)

    if (!started) {
      throw RuntimeException("Panel exchange app was not started successfully")
    }

    emrServerlessClientService.startAndWaitJobRunCompletion(appId, listOf(
      "--exchange-workflow-blob-key=$exchangeWorkflowBlobKey",
      "--step-index=$exchangeStepIndex",
      "--exchange-step-attempt-resource-id=$exchangeStepAttemptResourceId",
      "--exchange-date=$exchangeDate",
      "--storage-type=$storageType",
    ))

    val stopped = emrServerlessClientService.startOrStopApplication(appId, false)

    if (!stopped) {
      throw RuntimeException("Panel exchange app was not stopped successfully")
    }
  }
}
