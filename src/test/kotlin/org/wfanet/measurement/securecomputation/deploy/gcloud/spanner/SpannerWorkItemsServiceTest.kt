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

import kotlinx.coroutines.Dispatchers
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.testing.WorkItemsServiceTest

class SpannerWorkItemsServiceTest : WorkItemsServiceTest() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)

  override fun initServices(
    queueMapping: QueueMapping,
    idGenerator: IdGenerator,
    workItemPublisher: WorkItemPublisher,
  ): Services {
    val serviceDispatcher = Dispatchers.Default
    return Services(
      SpannerWorkItemsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        workItemPublisher,
      ),
      SpannerWorkItemAttemptsService(
        spannerDatabase.databaseClient,
        queueMapping,
        idGenerator,
        serviceDispatcher,
      ),
    )
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
