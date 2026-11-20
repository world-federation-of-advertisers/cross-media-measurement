/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups

import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import java.util.UUID
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemInvocation
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemRunner
import org.wfanet.measurement.storage.SelectedStorageClient

/** Validated source identity for an EventGroupSync task. */
internal data class EventGroupSyncWorkItem(
  val workItem: WorkItem,
  val params: WorkItem.WorkItemParams,
  val appParams: EventGroupSyncParams,
) {
  val source: WorkItem.WorkItemParams.DataPathParams
    get() = params.dataPathParams

  /** Requires a GCS source in the configured input object's bucket and directory. */
  fun validateGcsSource(configuredBlobUri: String) {
    val uri = SelectedStorageClient.parseBlobUri(source.dataPath)
    val configured = SelectedStorageClient.parseBlobUri(configuredBlobUri)
    val directory = configured.key.substringBeforeLast('/', "")
    val inputPrefix = if (directory.isEmpty()) "" else "$directory/"
    require(
      uri.scheme == "gs" && uri.bucket == configured.bucket && uri.key.startsWith(inputPrefix)
    ) {
      "EventGroupSync source is outside the configured input directory"
    }
  }

  companion object {
    fun parse(workItem: WorkItem): EventGroupSyncWorkItem {
      val id = workItem.name.removePrefix("workItems/")
      require(workItem.name == "workItems/$id" && ResourceIds.RFC_1034_REGEX.matches(id)) {
        "name must be a WorkItem resource name"
      }
      require(workItem.generation > 0L) { "WorkItem generation must be positive" }
      require(workItem.workItemParams.`is`(WorkItem.WorkItemParams::class.java)) {
        "work_item_params must contain WorkItemParams"
      }
      val params = workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
      require(params.appParams.`is`(EventGroupSyncParams::class.java)) {
        "app_params must contain EventGroupSyncParams"
      }
      val appParams = params.appParams.unpack(EventGroupSyncParams::class.java)
      requireNotNull(DataProviderKey.fromName(appParams.dataProvider)) {
        "data_provider must be a DataProvider resource name"
      }
      require(params.hasDataPathParams() && params.dataPathParams.dataPath.isNotEmpty()) {
        "data_path is required"
      }
      require(params.dataPathParams.hasGeneration() && params.dataPathParams.generation > 0L) {
        "source object generation must be positive"
      }
      return EventGroupSyncWorkItem(workItem, params, appParams)
    }
  }
}

/** Claims one attempt, renews its lease, and records completion before acknowledging delivery. */
internal class EventGroupSyncWorkItemProcessor(
  private val workItems: WorkItemsCoroutineStub,
  private val attempts: WorkItemAttemptsCoroutineStub,
  private val synchronize: suspend (EventGroupSyncWorkItem, suspend () -> Unit) -> Unit,
  private val uuidGenerator: () -> UUID,
  private val activeAttemptDelay: suspend () -> Unit,
  private val renewalDelay: suspend () -> Unit,
  private val updateRetryDelay: suspend (Int) -> Unit,
) {
  private val runner =
    WorkItemRunner(
      workItems,
      attempts,
      attemptIdGenerator = { "event-group-sync-${uuidGenerator()}" },
      activeAttemptDelay = activeAttemptDelay,
      renewalDelay = renewalDelay,
      updateRetryDelay = updateRetryDelay,
      requireAttemptLease = true,
      terminalWorkItemStates = setOf(WorkItem.State.SUCCEEDED, WorkItem.State.FAILED),
    )

  suspend fun process(input: EventGroupSyncWorkItem) {
    createInvocation(input, close = {}).run()
  }

  fun createInvocation(
    input: EventGroupSyncWorkItem,
    close: suspend () -> Unit,
  ): WorkItemInvocation {
    val handler =
      object : WorkItemRunner.Handler() {
        override val workItem = input.workItem

        override suspend fun execute(attempt: WorkItemRunner.Attempt): WorkItemRunner.Action {
          synchronize(input, attempt::ensureActive)
          return WorkItemRunner.Action.Complete
        }

        override fun failureAction(error: Exception): WorkItemRunner.Action? =
          if (
            generateSequence<Throwable>(error) { it.cause }.any { it is IllegalArgumentException }
          )
            WorkItemRunner.Action.Fail(errorMessage(error))
          else null

        override fun errorMessage(error: Exception): String = error::class.java.simpleName.take(200)
      }
    return WorkItemInvocation(
      runner,
      handler,
      executeInContext = { block ->
        val parentContext =
          Tracing.withW3CTraceContext(input.params.traceContextMap) { Context.current() }
        withContext(parentContext.asContextElement()) { block() }
      },
      close = close,
    )
  }
}
