// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.exchangetasks.remote.aws

import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.exchangetasks.remote.RemoteTaskOrchestrator
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.common.storage.toStringUtf8

private const val EMR_EXCHANGE_TASK_APP_NAME = "panel-exchange-beam-task"

class EmrRemoteTaskOrchestrator(
  val exchangeTaskAppIdPath: String,
  val exchangeWorkflowPrefix: String,
  val storageClient: StorageClient,
  val storageType: PlatformCase,
  val storageBucket: String,
  val storageRegion: String,
  val emrServerlessClient: EmrServerlessClient,
) : RemoteTaskOrchestrator {
  private val appId: String by lazy {
    runBlocking {
      val exchangeTaskAppIdBlob = storageClient.getBlob(exchangeTaskAppIdPath)

      if (exchangeTaskAppIdBlob != null) {
        exchangeTaskAppIdBlob.toStringUtf8()
      } else {
        val id = emrServerlessClient.createApplication(EMR_EXCHANGE_TASK_APP_NAME)
        storageClient.writeBlob(exchangeTaskAppIdPath, id.toByteStringUtf8())
        id
      }
    }
  }

  override suspend fun orchestrateTask(
    exchangeWorkflowId: String,
    exchangeStepIndex: Int,
    exchangeStepAttempt: CanonicalExchangeStepAttemptKey,
    exchangeDate: LocalDate,
  ) {
    if (!emrServerlessClient.startApplication(appId)) {
      throw Exception("Panel exchange app was not started successfully")
    }

    val jobRunId =
      "${exchangeStepAttempt.exchangeStepId}-${exchangeStepAttempt.exchangeId}-${exchangeDate.format(DateTimeFormatter.ISO_LOCAL_DATE)}"

    if (
      !emrServerlessClient.startAndWaitJobRunCompletion(
        jobRunId,
        appId,
        listOf(
          "--exchange-workflow-blob-key=$exchangeWorkflowPrefix/$exchangeWorkflowId",
          "--step-index=$exchangeStepIndex",
          "--exchange-step-attempt-resource-id=${exchangeStepAttempt.toName()}",
          "--exchange-date=${exchangeDate.format(DateTimeFormatter.ISO_LOCAL_DATE)}",
          "--storage-type=${storageType.name}",
          "--s3-region=${storageRegion}",
          "--s3-storage-bucket=${storageBucket}",
          "--google-cloud-storage-bucket=",
          "--google-cloud-storage-project=",
        ),
      )
    ) {
      throw Exception("Panel exchange step was not executed successfully")
    }

    if (!emrServerlessClient.stopApplication(appId)) {
      throw Exception("Panel exchange app was not stopped successfully")
    }
  }
}
