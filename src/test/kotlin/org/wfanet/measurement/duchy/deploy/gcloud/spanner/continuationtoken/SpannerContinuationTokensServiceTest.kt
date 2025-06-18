// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.continuationtoken

import kotlinx.coroutines.Dispatchers
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.duchy.service.internal.testing.ContinuationTokensServiceTest
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule

class SpannerContinuationTokensServiceTest :
  ContinuationTokensServiceTest<SpannerContinuationTokensService>() {
  @get:Rule
  val spannerDatabase = SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.DUCHY_CHANGELOG_PATH)

  override fun newService(): SpannerContinuationTokensService {
    return SpannerContinuationTokensService(spannerDatabase.databaseClient, Dispatchers.Default)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
