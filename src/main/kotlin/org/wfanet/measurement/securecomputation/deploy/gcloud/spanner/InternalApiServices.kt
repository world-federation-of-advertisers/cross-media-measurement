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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import java.time.Duration
import kotlin.coroutines.CoroutineContext
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.Services
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

/**
 * Factory for creating internal API services for the Secure Computation system.
 *
 * This class is responsible for constructing the core gRPC services that handle work item
 * management operations.
 */
class InternalApiServices(
  workItemPublisher: WorkItemPublisher,
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val idGenerator: IdGenerator = IdGenerator.Default,
  workItemPublicationPollInterval: Duration = WorkItemPublicationRunner.DEFAULT_POLL_INTERVAL,
  workItemPublicationLeaseDuration: Duration = WorkItemPublicationRunner.DEFAULT_LEASE_DURATION,
) {
  val workItemPublicationRunner =
    WorkItemPublicationRunner(
      databaseClient,
      queueMapping,
      workItemPublisher,
      pollInterval = workItemPublicationPollInterval,
      leaseDuration = workItemPublicationLeaseDuration,
    )

  /**
   * Builds the core internal API services.
   *
   * @return A [Services] instance containing:
   *     - WorkItemsService: Manages work item lifecycle (create, get, fail, etc.)
   *     - WorkItemAttemptsService: Tracks work item execution attempts
   */
  fun build(coroutineContext: CoroutineContext): Services {
    return Services(
      SpannerWorkItemsService(
        databaseClient,
        queueMapping,
        idGenerator,
        workItemPublicationRunner,
        coroutineContext,
      ),
      SpannerWorkItemAttemptsService(databaseClient, queueMapping, idGenerator, coroutineContext),
    )
  }
}
