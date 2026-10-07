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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.protobuf.Any
import com.google.protobuf.InvalidProtocolBufferException
import com.google.type.date
import java.time.LocalDate
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams.DataPathParams.StorageEventType
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/** Builds deterministic DataAvailabilitySync WorkItems for VID-labeled output. */
object DataAvailabilitySyncWorkItems {
  const val QUEUE = "data-availability-sync-queue"

  fun createRequest(
    dataProvider: String,
    rawImpressionUpload: String,
    rawImpressionUploadModelLine: String,
    modelLine: String,
    eventDate: LocalDate,
    doneBlobUri: String,
    doneBlobGeneration: Long,
    traceContext: Map<String, String>,
  ): CreateWorkItemRequest {
    require(doneBlobGeneration > 0L) { "doneBlobGeneration must be positive" }
    val workItemId =
      WorkItemIds.forDataAvailabilitySync(
        VidLabelingTraceAttributes.gcsObjectPathHash(doneBlobUri),
        doneBlobGeneration,
      )
    return createWorkItemRequest {
      this.workItemId = workItemId
      workItem = workItem {
        queue = QUEUE
        serializationKey = "data-availability-sync:$dataProvider"
        workItemParams =
          Any.pack(
            workItemParams {
              appParams =
                Any.pack(
                  dataAvailabilitySyncParams {
                    this.dataProvider = dataProvider
                    this.rawImpressionUpload = rawImpressionUpload
                    this.rawImpressionUploadModelLine = rawImpressionUploadModelLine
                    this.modelLine = modelLine
                    this.eventDate = date {
                      year = eventDate.year
                      month = eventDate.monthValue
                      day = eventDate.dayOfMonth
                    }
                  }
                )
              dataPathParams = dataPathParams {
                dataPath = doneBlobUri
                generation = doneBlobGeneration
                eventType = StorageEventType.FINALIZED
              }
              this.traceContext.putAll(traceContext)
            }
          )
      }
    }
  }

  /**
   * Returns whether [existing] has the same immutable availability identity as [request].
   *
   * Trace context is intentionally excluded: a retry or recovery attempt has a new trace, while the
   * source upload, model line, event date, exact done object, queue, and serialization key must
   * remain identical.
   */
  fun hasSameIdentity(existing: WorkItem, request: CreateWorkItemRequest): Boolean {
    val existingIdentity = existing.toIdentity() ?: return false
    return existingIdentity == request.workItem.toIdentity()
  }

  private fun WorkItem.toIdentity(): Identity? {
    if (!workItemParams.`is`(WorkItem.WorkItemParams::class.java)) return null
    val params =
      try {
        workItemParams.unpack(WorkItem.WorkItemParams::class.java)
      } catch (_: InvalidProtocolBufferException) {
        return null
      }
    if (
      !params.hasAppParams() ||
        !params.appParams.`is`(DataAvailabilitySyncParams::class.java) ||
        !params.hasDataPathParams()
    ) {
      return null
    }
    val appParams =
      try {
        params.appParams.unpack(DataAvailabilitySyncParams::class.java)
      } catch (_: InvalidProtocolBufferException) {
        return null
      }
    return Identity(queue, serializationKey, appParams, params.dataPathParams)
  }

  private data class Identity(
    val queue: String,
    val serializationKey: String,
    val appParams: DataAvailabilitySyncParams,
    val dataPathParams: WorkItem.WorkItemParams.DataPathParams,
  )
}
