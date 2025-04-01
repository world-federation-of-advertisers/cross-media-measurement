/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.datawatcher

import java.util.UUID
import java.util.logging.Logger
import kotlin.text.matches
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/*
 * Watcher to observe blob creation events and take the appropriate action for each.
 * @param workItemsStub - the Google Pub Sub Sink to call
 * @param dataWatcherConfigs - a list of [DataWatcherConfig]
 */
class DataWatcher(
  private val workItemsStub: WorkItemsCoroutineStub,
  private val dataWatcherConfigs: List<DataWatcherConfig>,
) {
  suspend fun receivePath(path: String) {
    for (config in dataWatcherConfigs) {
      val regex = config.sourcePathRegex.toRegex()
      if (regex.matches(path)) {
        logger.info("Matched path: $path")
        when (config.sinkConfigCase) {
          DataWatcherConfig.SinkConfigCase.CONTROL_PLANE_CONFIG -> {
            val queueConfig = config.controlPlaneConfig
            val workItemId = UUID.randomUUID().toString()
            val workItemParams = workItemParams {
              this.config = queueConfig.appConfig
              this.dataPath = path
            }
            val request = createWorkItemRequest {
              this.workItemId = workItemId
              this.workItem = workItem {
                queue = queueConfig.queue
                this.workItemParams = workItemParams
              }
            }
            workItemsStub.createWorkItem(request)
          }
          DataWatcherConfig.SinkConfigCase.WEB_HOOK_CONFIG ->
            TODO("Web Hook Sink not currently supported")
          DataWatcherConfig.SinkConfigCase.SINKCONFIG_NOT_SET ->
            error("Invalid sink config: ${config.sinkConfigCase}")
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
