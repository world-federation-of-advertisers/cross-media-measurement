// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import java.time.Clock
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import org.junit.ClassRule
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyDetails
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyState
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.EventGroupAndHelperServices
import org.wfanet.measurement.kingdom.service.internal.testing.EventGroupsServiceTest

@RunWith(JUnit4::class)
class SpannerEventGroupsServiceTest : EventGroupsServiceTest<SpannerEventGroupsService>() {

  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.KINGDOM_CHANGELOG_PATH)
  private val clock = Clock.systemUTC()

  override fun newServices(
    idGenerator: IdGenerator
  ): EventGroupAndHelperServices<SpannerEventGroupsService> {
    val spannerServices =
      SpannerDataServices(clock, idGenerator, spannerDatabase.databaseClient).buildDataServices()

    return EventGroupAndHelperServices(
      spannerServices.eventGroupsService as SpannerEventGroupsService,
      spannerServices.measurementConsumersService,
      spannerServices.dataProvidersService,
      spannerServices.accountsService,
      spannerServices.eventGroupActivitiesService,
    )
  }

  override suspend fun seedActiveHierarchy(
    externalDataProviderId: Long,
    details: EventGroupHierarchyDetails,
  ) {
    val databaseClient = spannerDatabase.databaseClient
    val statement =
      Statement.newBuilder(
          "SELECT DataProviderId FROM DataProviders WHERE ExternalDataProviderId = @externalId"
        )
        .bind("externalId")
        .to(externalDataProviderId)
        .build()
    val dataProviderId =
      databaseClient.singleUse().use { context ->
        context.executeQuery(statement).single().getLong("DataProviderId")
      }

    val hierarchy =
      Mutation.newInsertBuilder("EventGroupHierarchies")
        .set("DataProviderId")
        .to(dataProviderId)
        .set("EventGroupHierarchyId")
        .to(idGenerator.generateInternalId().value)
        .set("ExternalEventGroupHierarchyId")
        .to("test-hierarchy")
        .set("State")
        .toInt64(EventGroupHierarchyState.ACTIVE)
        .set("EventGroupHierarchyDetails")
        .to(Value.protoMessage(details))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    databaseClient.write(hierarchy)
  }

  override suspend fun readStoredEntityPaths(externalDataProviderId: Long): Map<Long, String?> {
    val statement =
      Statement.newBuilder(
          """
          SELECT ExternalEventGroupId, EntityPath
          FROM EventGroups
          JOIN DataProviders USING (DataProviderId)
          WHERE ExternalDataProviderId = @externalId
          """
            .trimIndent()
        )
        .bind("externalId")
        .to(externalDataProviderId)
        .build()
    return spannerDatabase.databaseClient.singleUse().use { context ->
      context.executeQuery(statement).toList().associate { row ->
        val path = if (row.isNull("EntityPath")) null else row.getString("EntityPath")
        row.getLong("ExternalEventGroupId") to path
      }
    }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
