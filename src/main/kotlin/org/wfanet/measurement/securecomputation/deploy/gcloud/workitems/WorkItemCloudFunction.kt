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

package org.wfanet.measurement.securecomputation.deploy.gcloud.workitems

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemPublicationCodec

/**
 * Runs durable WorkItems before HTTP acknowledgment; heartbeats renew control-plane leases only.
 */
abstract class WorkItemCloudFunction : HttpFunction {
  final override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      if (isWorkItemRequest(request)) {
        runBlocking {
          createWorkItemInvocation(WorkItemPublicationCodec.parse(request.inputStream)).run()
        }
      } else {
        serviceLegacyRequest(request, response)
      }
    } catch (e: Exception) {
      handleFailure(e, response)
    } finally {
      flushTelemetry()
    }
  }

  protected abstract fun isWorkItemRequest(request: HttpRequest): Boolean

  protected abstract suspend fun createWorkItemInvocation(workItem: WorkItem): WorkItemInvocation

  protected abstract fun serviceLegacyRequest(request: HttpRequest, response: HttpResponse)

  protected abstract fun handleFailure(error: Exception, response: HttpResponse)

  protected abstract fun flushTelemetry()
}
