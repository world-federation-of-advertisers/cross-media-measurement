// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.gcloud

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import kotlinx.coroutines.debug.junit4.CoroutinesTimeout
import org.junit.Rule
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.InProcessLifeOfAMeasurementIntegrationTest
import org.wfanet.measurement.storage.StorageClient

/**
 * Implementation of [InProcessLifeOfAMeasurementIntegrationTest] for GCloud backends (Spanner,
 * GCS).
 */
class GCloudInProcessLifeOfAMeasurementIntegrationTest :
  InProcessLifeOfAMeasurementIntegrationTest() {

  @get:Rule val timeout = CoroutinesTimeout.seconds(90)

  override val kingdomDataServicesRule by lazy { KingdomDataServicesProviderRule() }
  override val duchyDependenciesRule by lazy { DuchyDependencyProviderRule(ALL_DUCHY_NAMES) }
  override val storageClient: StorageClient by lazy {
    GcsStorageClient(LocalStorageHelper.getOptions().service, "bucket-simulator")
  }
}
