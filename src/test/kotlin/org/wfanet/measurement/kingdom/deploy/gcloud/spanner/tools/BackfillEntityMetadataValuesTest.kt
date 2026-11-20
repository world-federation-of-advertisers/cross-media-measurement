// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.tools

import com.google.cloud.spanner.Struct as SpannerStruct
import com.google.cloud.spanner.Value
import com.google.cloud.spanner.connection.SpannerPool
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Struct
import com.google.protobuf.listValue
import com.google.protobuf.struct
import com.google.protobuf.value
import java.sql.DriverManager
import java.time.Clock
import kotlin.random.Random
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulator
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.Population

@RunWith(JUnit4::class)
class BackfillEntityMetadataValuesTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)
  @get:Rule val temporaryFolder = TemporaryFolder()
  private val emulator = SpannerEmulator()
  private lateinit var connector: SpannerDatabaseConnector
  private lateinit var client: AsyncDatabaseClient
  private val services
    get() =
      SpannerDataServices(clock, idGenerator, client, enableEntityMetadataSearch = true)
        .buildDataServices()

  private val clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock, Random(1))
  private val population = Population(clock, idGenerator)

  @Before
  fun initDatabase() {
    val host = emulator.start()
    DriverManager.getConnection(
        SpannerEmulator.buildJdbcConnectionString(host, "test-project", "test-instance", "test-db")
      )
      .use { connection -> Liquibase.update(connection, Schemata.KINGDOM_CHANGELOG_PATH) }
    connector =
      SpannerDatabaseConnector(
        "test-project",
        "test-instance",
        "test-db",
        java.time.Duration.ofSeconds(10),
        8,
        host,
      )
    client = connector.databaseClient
  }

  @After
  fun closeDatabase() {
    if (::connector.isInitialized) {
      connector.close()
    }
    SpannerPool.closeSpannerPool()
    emulator.close()
  }

  @Test
  fun `main backfills legacy rows in batches without changing EventGroups`(): Unit = runBlocking {
    val nested = struct {
      fields["secretkey"] = value {
        listValue = listValue {
          values += value { stringValue = "Violet" }
          values += value { numberValue = 42.5 }
          values += value { boolValue = true }
          values += value {
            structValue = struct { fields["nestedkey"] = value { stringValue = "Bicycle" } }
          }
        }
      }
    }
    val groups =
      listOf(
          createEventGroup(nested),
          createEventGroup(null),
          createEventGroup(Struct.getDefaultInstance()),
          createEventGroup(nested),
        )
        .toMutableList()
    groups[3] =
      services.eventGroupsService.deleteEventGroup(
        deleteEventGroupRequest {
          externalDataProviderId = groups[3].externalDataProviderId
          externalEventGroupId = groups[3].externalEventGroupId
        }
      )
    clearSearchValues()

    runBackfill("--batch-size=1")

    val rows = readRows()
    assertThat(rows).hasSize(4)
    assertThat(rows.count { it.getStringList("EntityMetadataValues").isEmpty() }).isEqualTo(2)
    for (row in
      rows.filter {
        !it.isNull("EntityMetadata") &&
          it.getProtoMessage("EntityMetadata", Struct.getDefaultInstance()).fieldsCount > 0
      }) {
      assertThat(row.getStringList("EntityMetadataValues"))
        .containsExactly("Violet", "42.5", "true", "Bicycle")
        .inOrder()
    }
    for (group in groups) {
      assertThat(getEventGroup(group)).isEqualTo(group)
    }
    assertThat(search("violet")).containsExactly(groups[0])
    assertThat(search("secretkey")).isEmpty()
  }

  @Test
  fun `main reruns skip populated rows and preserve subsequent updates`(): Unit = runBlocking {
    val group = createEventGroup(struct { fields["name"] = value { stringValue = "Violet" } })
    clearSearchValues()
    runBackfill()
    val updated =
      services.eventGroupsService.updateEventGroup(
        updateEventGroupRequest {
          eventGroup =
            group.copy {
              entityMetadata = struct { fields["name"] = value { stringValue = "Orchid" } }
            }
        }
      )
    val rowsBeforeRerun = readRows()

    runBackfill()

    assertThat(temporaryFolder.root.resolve("backfill.log").readText())
      .contains("Completed entity metadata values backfill: rows=0, dryRun=false")
    assertThat(readRows()).containsExactlyElementsIn(rowsBeforeRerun)
    assertThat(getEventGroup(updated)).isEqualTo(updated)
    assertThat(search("violet")).isEmpty()
    assertThat(search("orchid")).containsExactly(updated)
  }

  @Test
  fun `main dry run counts only missing values without changing rows`(): Unit = runBlocking {
    createEventGroup(struct { fields["name"] = value { stringValue = "Violet" } })
    createEventGroup(null)
    clearSearchValues()
    createEventGroup(Struct.getDefaultInstance())
    val before = readRows()

    runBackfill("--dry-run", "--batch-size=1")

    assertThat(temporaryFolder.root.resolve("backfill.log").readText())
      .contains("Completed entity metadata values backfill: rows=2, dryRun=true")
    assertThat(readRows()).containsExactlyElementsIn(before)
    assertThat(search("violet")).isEmpty()
  }

  @Test
  fun `metadata search excludes entity values until expanded reads are enabled`(): Unit =
    runBlocking {
      val group = createEventGroup(struct { fields["name"] = value { stringValue = "Violet" } })
      val legacyServices = SpannerDataServices(clock, idGenerator, client).buildDataServices()
      val request = streamEventGroupsRequest { filter = filter { metadataSearchQuery = "violet" } }

      assertThat(legacyServices.eventGroupsService.streamEventGroups(request).toList()).isEmpty()
      assertThat(services.eventGroupsService.streamEventGroups(request).toList())
        .containsExactly(group)
    }

  private fun runBackfill(vararg args: String) {
    main(
      arrayOf(
        "--spanner-project=test-project",
        "--spanner-instance=test-instance",
        "--spanner-database=test-db",
        "--spanner-emulator-host=${emulator.emulatorHost}",
        "--log-file=${temporaryFolder.root.resolve("backfill.log")}",
        *args,
      )
    )
  }

  private suspend fun createEventGroup(metadata: Struct?): EventGroup {
    val dataProvider = population.createDataProvider(services.dataProvidersService)
    val measurementConsumer =
      population.createMeasurementConsumer(
        services.measurementConsumersService,
        services.accountsService,
      )
    return services.eventGroupsService.createEventGroup(
      createEventGroupRequest {
        eventGroup = eventGroup {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          if (metadata != null) {
            entityMetadata = metadata
          }
        }
      }
    )
  }

  private suspend fun clearSearchValues() {
    client.readWriteTransaction().run { transaction ->
      val rows =
        transaction
          .executeQuery(statement("SELECT DataProviderId, EventGroupId FROM EventGroups"))
          .toList()
      for (row in rows) {
        transaction.bufferUpdateMutation("EventGroups") {
          set("DataProviderId").to(row.getLong("DataProviderId"))
          set("EventGroupId").to(row.getLong("EventGroupId"))
          set("EntityMetadataValues").to(Value.stringArray(null))
        }
      }
    }
  }

  private suspend fun readRows(): List<SpannerStruct> =
    client
      .singleUse()
      .executeQuery(
        statement(
          "SELECT DataProviderId, EventGroupId, EntityMetadata, EntityMetadataValues, UpdateTime " +
            "FROM EventGroups ORDER BY DataProviderId, EventGroupId"
        )
      )
      .toList()

  private suspend fun getEventGroup(group: EventGroup): EventGroup =
    services.eventGroupsService.getEventGroup(
      getEventGroupRequest {
        externalDataProviderId = group.externalDataProviderId
        externalEventGroupId = group.externalEventGroupId
      }
    )

  private suspend fun search(query: String): List<EventGroup> =
    services.eventGroupsService
      .streamEventGroups(
        streamEventGroupsRequest { filter = filter { metadataSearchQuery = query } }
      )
      .toList()
}
