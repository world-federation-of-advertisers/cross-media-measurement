/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.deploy.gcloud.spanner

import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.access.service.internal.IdGenerator
import org.wfanet.measurement.access.service.internal.testing.PrincipalsServiceTest
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule

class SpannerPrincipalsServiceTest : PrincipalsServiceTest() {
  @get:Rule
  val spannerDatabase = SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.ACCESS_CHANGELOG_PATH)

  override fun initService(tlsClientMapping: TlsClientPrincipalMapping, idGenerator: IdGenerator) =
    SpannerPrincipalsService(spannerDatabase.databaseClient, tlsClientMapping, idGenerator)

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}