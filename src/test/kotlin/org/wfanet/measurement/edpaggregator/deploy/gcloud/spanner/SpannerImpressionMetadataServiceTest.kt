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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.ImpressionMetadataServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.acquireDataAvailabilitySyncLeaseRequest

class SpannerImpressionMetadataServiceTest : ImpressionMetadataServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  override fun newService(
    idGenerator: IdGenerator
  ): ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    runBlocking {
      SpannerDataAvailabilitySyncLeaseService(databaseClient)
        .acquireDataAvailabilitySyncLease(
          acquireDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            synchronizationAttemptId = SYNCHRONIZATION_ATTEMPT_ID
            requestId = LEASE_REQUEST_ID
          }
        )
      SpannerDataAvailabilitySyncLeaseService(databaseClient)
        .acquireDataAvailabilitySyncLease(
          acquireDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = "data-provider-2"
            synchronizationAttemptId = SYNCHRONIZATION_ATTEMPT_ID
            requestId = SECONDARY_LEASE_REQUEST_ID
          }
        )
    }
    return SpannerImpressionMetadataService(databaseClient)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val SYNCHRONIZATION_ATTEMPT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val LEASE_REQUEST_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    private const val SECONDARY_LEASE_REQUEST_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
  }
}
