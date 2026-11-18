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

package org.wfanet.measurement.securecomputation.datawatcher

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.edpaggregator.VidLabelingDispatchWorkItems
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatcherParams

/** Validates DataWatcher configuration. */
object DataWatcherConfigValidator {
  fun validate(config: DataWatcherConfig) {
    for (watchedPath in config.watchedPathsList) {
      require(watchedPath.eventTypesList.all { it.isNotBlank() }) {
        "DataWatcher path '${watchedPath.identifier}' has a blank event type filter."
      }
      if (!watchedPath.hasControlPlaneQueueSink()) continue
      val sink = watchedPath.controlPlaneQueueSink
      require(!sink.appParams.`is`(VidLabelingDispatchWorkItemParams::class.java)) {
        "Dispatch configuration must use VidLabelingDispatcherParams"
      }
      if (
        sink.queue == VidLabelingDispatchWorkItems.QUEUE ||
          sink.appParams.`is`(VidLabelingDispatcherParams::class.java)
      ) {
        require(sink.queue == VidLabelingDispatchWorkItems.QUEUE) { "Invalid VID dispatch queue" }
        require(sink.serializationKey.isEmpty()) { "VID dispatch uses model-line sequencing" }
        require(sink.appParams.`is`(VidLabelingDispatcherParams::class.java)) {
          "Dispatch configuration must use VidLabelingDispatcherParams"
        }
        val params = sink.appParams.unpack(VidLabelingDispatcherParams::class.java)
        requireNotNull(DataProviderKey.fromName(params.dataProvider)) {
          "data_provider must identify a DataProvider"
        }
      }
      require(!sink.appParams.`is`(ResultsFulfillerParams::class.java)) {
        "DataWatcher path '${watchedPath.identifier}' uses legacy ResultsFulfiller dispatch; " +
          "remove it and use RequisitionFetcher 'work_item_dispatch'."
      }
    }
  }
}
