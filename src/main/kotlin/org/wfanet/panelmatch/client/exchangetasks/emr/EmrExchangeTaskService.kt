package org.wfanet.panelmatch.client.exchangetasks.emr

import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.common.storage.toStringUtf8

private const val EMR_EXCHANGE_TASK_APP_NAME = "panel-exchange-beam-task"

class EmrExchangeTaskService(
  val exchangeTaskAppIdPath: String,
  val exchangeWorkflowPrefix: String,
  val storageClient: StorageClient,
  val storageType: PlatformCase,
  val emrServerlessClientService: EmrServerlessClientService
) {

  private var appId: String

  init {
    val exchangeTaskAppIdBlob = runBlocking { storageClient.getBlob(exchangeTaskAppIdPath) }

    if (exchangeTaskAppIdBlob != null) {
      runBlocking { appId = exchangeTaskAppIdBlob.toStringUtf8() }
    } else {
      appId = emrServerlessClientService.createApplication(EMR_EXCHANGE_TASK_APP_NAME)
      runBlocking { storageClient.writeBlob(exchangeTaskAppIdPath, appId.toByteStringUtf8()) }
    }
  }

  suspend fun runPanelExchangeStepOnEmrApp(
    exchangeWorkflowId: String,
    exchangeStepIndex: Int,
    exchangeStepAttempt: CanonicalExchangeStepAttemptKey,
    exchangeDate: LocalDate,
  ) {
    val started = emrServerlessClientService.startOrStopApplication(appId, true)

    if (!started) {
      throw RuntimeException("Panel exchange app was not started successfully")
    }

    emrServerlessClientService.startAndWaitJobRunCompletion(appId, listOf(
      "--exchange-workflow-blob-key=$exchangeWorkflowPrefix/$exchangeWorkflowId",
      "--step-index=$exchangeStepIndex",
      "--exchange-step-attempt-resource-id=${exchangeStepAttempt.toName()}",
      "--exchange-date=${exchangeDate.format(DateTimeFormatter.ISO_LOCAL_DATE)}",
      "--storage-type=${storageType.name}",
    ))

    val stopped = emrServerlessClientService.startOrStopApplication(appId, false)

    if (!stopped) {
      throw RuntimeException("Panel exchange app was not stopped successfully")
    }
  }
}
