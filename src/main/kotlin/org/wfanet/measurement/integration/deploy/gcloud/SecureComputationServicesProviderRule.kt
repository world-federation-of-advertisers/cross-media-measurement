// Copyright 2025 The Cross-Media Measurement Authors
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

import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

class SecureComputationServicesProviderRule(
  private val workItemPublisher: WorkItemPublisher,
  private val queueMapping: QueueMapping,
  emulatorDatabaseAdmin: SpannerDatabaseAdmin,
) : ProviderRule<InternalApiServices> {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(emulatorDatabaseAdmin, Schemata.SECURECOMPUTATION_CHANGELOG_PATH)

  private lateinit var internalServices: InternalApiServices

  override val value
    get() = internalServices

  override fun apply(base: Statement, description: Description): Statement {
    val statement =
      object : Statement() {
        override fun evaluate() {
          internalServices =
            InternalApiServices(workItemPublisher, spannerDatabase.databaseClient, queueMapping)
          base.evaluate()
        }
      }
    return spannerDatabase.apply(statement, description)
  }
}
