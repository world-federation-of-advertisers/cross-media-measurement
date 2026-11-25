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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling.testing

import org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling.VidLabelingDispatcherWorkItemProcessor
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher.UploadOutcome
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

/** Runs the production dispatcher processor against in-process clients and storage. */
class VidLabelingDispatchWorkItemTestRunner(
  items: WorkItemsCoroutineStub,
  attempts: WorkItemAttemptsCoroutineStub,
  dispatch:
    suspend (
      VidLabelingDispatchWorkItemParams, WorkItem.WorkItemParams.DataPathParams, suspend () -> Unit,
    ) -> UploadOutcome,
) {
  private val processor =
    VidLabelingDispatcherWorkItemProcessor(
      items,
      attempts,
      { input, guard -> dispatch(input.appParams, input.source, guard) },
    )

  suspend fun process(item: WorkItem) {
    processor.process(item)
  }
}
