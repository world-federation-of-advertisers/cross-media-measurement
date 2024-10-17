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

package org.wfanet.measurement.integration.deploy.gcloud

import java.time.Clock
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata

class KingdomDataServicesProviderRule(emulatorDatabaseAdmin: SpannerDatabaseAdmin) :
  ProviderRule<DataServices> {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(emulatorDatabaseAdmin, Schemata.KINGDOM_CHANGELOG_PATH)

  private lateinit var dataServices: DataServices

  override val value
    get() = dataServices

  override fun apply(base: Statement, description: Description): Statement {
    val statement =
      object : Statement() {
        override fun evaluate() {
          val clock = Clock.systemUTC()
          dataServices =
            SpannerDataServices(
              clock,
              RandomIdGenerator(clock),
              spannerDatabase.databaseClient,
              KNOWN_EVENT_GROUP_METADATA_TYPES,
            )
          base.evaluate()
        }
      }
    return spannerDatabase.apply(statement, description)
  }

  companion object {
    private val KNOWN_EVENT_GROUP_METADATA_TYPES =
      listOf(TestMetadataMessage.getDescriptor().file, SyntheticEventGroupSpec.getDescriptor().file)
  }
}
