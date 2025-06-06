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

package org.wfanet.measurement.integration.deploy.gcloud

import kotlinx.coroutines.Dispatchers
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.access.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.internal.Services as AccessInternalServices
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.common.AccessServicesFactory

class SpannerAccessServicesFactory(emulatorDatabaseAdmin: SpannerDatabaseAdmin) :
  AccessServicesFactory {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(emulatorDatabaseAdmin, Schemata.ACCESS_CHANGELOG_PATH)

  override fun create(
    permissionMapping: PermissionMapping,
    tlsClientMapping: TlsClientPrincipalMapping,
  ): AccessInternalServices =
    InternalApiServices.build(
      spannerDatabase.databaseClient,
      permissionMapping,
      tlsClientMapping,
      Dispatchers.Default,
    )

  override fun apply(base: Statement, description: Description): Statement {
    return spannerDatabase.apply(base, description)
  }
}
