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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.protobuf.NullValue
import com.google.protobuf.Struct
import com.google.protobuf.copy
import com.google.protobuf.listValue
import com.google.protobuf.struct
import com.google.protobuf.value
import java.time.Clock
import kotlin.random.Random
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyDetailsKt.level
import org.wfanet.measurement.internal.kingdom.EventGroupKt.entityKey
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.FilterKt.after
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupHierarchyDetails
import org.wfanet.measurement.internal.kingdom.eventGroupKey
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.Population

@RunWith(JUnit4::class)
class SpannerInheritedMetadataSearchTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)
  @get:Rule
  val database = SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.KINGDOM_CHANGELOG_PATH)
  private val clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock, Random(1))
  private val population = Population(clock, idGenerator)
  private val services
    get() =
      SpannerDataServices(
          clock,
          idGenerator,
          database.databaseClient,
          enableEntityMetadataSearch = true,
        )
        .buildDataServices()

  private lateinit var dataProvider: DataProvider
  private lateinit var measurementConsumer: MeasurementConsumer

  @Before
  fun initServices(): Unit = runBlocking {
    dataProvider = population.createDataProvider(services.dataProvidersService)
    measurementConsumer =
      population.createMeasurementConsumer(
        services.measurementConsumersService,
        services.accountsService,
      )
    val providerId =
      database.databaseClient
        .singleUse()
        .executeQuery(
          Statement.newBuilder(
              "SELECT DataProviderId FROM DataProviders WHERE ExternalDataProviderId = @id"
            )
            .bind("id")
            .to(dataProvider.externalDataProviderId)
            .build()
        )
        .single()
        .getLong("DataProviderId")
    database.databaseClient.readWriteTransaction().run { transaction ->
      transaction.bufferInsertMutation("EventGroupHierarchies") {
        set("DataProviderId").to(providerId)
        set("EventGroupHierarchyId").to(1L)
        set("ExternalEventGroupHierarchyId").to("test-hierarchy")
        set("State").to(ACTIVE_HIERARCHY_STATE)
        set("EventGroupHierarchyDetails")
          .to(
            eventGroupHierarchyDetails {
              for (type in listOf("advertiser", "campaign", "creative")) {
                levels += level {
                  entityType = type
                  idEntityMetadataKey = "${type}_id"
                  entityMetadataKeys += "${type}_name"
                }
              }
            }
          )
        set("CreateTime").to(Value.COMMIT_TIMESTAMP)
      }
    }
    createGroup("advertiser", "a", "advertiser/a/", "Violet")
  }

  @Test
  fun `streamEventGroups matches terms stored on different hierarchy levels`(): Unit = runBlocking {
    createGroup("campaign", "c", CAMPAIGN_PATH, "Bicycle")
    val leaf = createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Sneaker")

    assertThat(search("violet bicycle sneaker")).containsExactly(leaf)
    assertThat(search("creative bicycle")).containsExactly(leaf)
    assertThat(search("advertiser")).isEmpty()
  }

  @Test
  fun `streamEventGroups preserves search operators across hierarchy levels`(): Unit = runBlocking {
    createGroup("campaign", "c", CAMPAIGN_PATH, "Blue bicycle")
    val leaf = createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Red sneaker")
    val queries =
      mapOf(
        "bicycle sneaker" to true,
        "bicycle missing" to false,
        "missing OR bicycle" to true,
        "bicycle|missing sneaker" to true,
        "missing bicycle OR sneaker" to false,
        "sneaker -bicycle" to false,
        "sneaker -missing" to true,
        "\"blue bicycle\" sneaker" to true,
        "\"bicycle red\"" to false,
        "blue AROUND(2) bicycle" to true,
        "bicycle AROUND(2) sneaker" to false,
        "sneaker -\"blue bicycle\"" to false,
      )
    for ((query, matches) in queries) {
      assertWithMessage(query)
        .that(search(query))
        .containsExactlyElementsIn(listOf(leaf).filter { matches })
    }
  }

  @Test
  fun `streamEventGroups excludes metadata keys and nulls from inherited matches`(): Unit =
    runBlocking {
      val parent = createGroup("campaign", "c", CAMPAIGN_PATH, "Original")
      updateMetadata(
        parent,
        struct {
          fields["campaign_name"] = value {
            structValue = struct {
              fields["hiddenkey"] = value {
                listValue = listValue {
                  values += value { stringValue = "Violet" }
                  values += value { numberValue = 42.5 }
                  values += value { boolValue = true }
                  values += value { nullValue = NullValue.NULL_VALUE }
                }
              }
            }
          }
        },
      )
      val leaf = createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Sneaker")

      for (query in listOf("violet", "42.5", "true")) {
        assertWithMessage(query).that(search(query)).containsExactly(leaf)
      }
      for (query in listOf("hiddenkey", "campaign_name", "null")) {
        assertWithMessage(query).that(search(query)).isEmpty()
      }
    }

  @Test
  fun `updateEventGroup changes inherited search without rewriting descendants`(): Unit =
    runBlocking {
      val parent = createGroup("campaign", "c", CAMPAIGN_PATH, "Original")
      val leaves =
        (1..3).map { id ->
          createGroup("creative", "l$id", "${CAMPAIGN_PATH}creative/l$id/", "Sneaker")
        }
      assertThat(search("original")).containsExactlyElementsIn(leaves)

      updateMetadata(parent, struct { fields["campaign_name"] = value { stringValue = "Renamed" } })

      assertThat(search("original")).isEmpty()
      assertThat(search("renamed sneaker")).containsExactlyElementsIn(leaves)
      for (leaf in leaves) {
        val stored =
          services.eventGroupsService.getEventGroup(
            getEventGroupRequest {
              externalDataProviderId = leaf.externalDataProviderId
              externalEventGroupId = leaf.externalEventGroupId
            }
          )
        assertThat(stored).isEqualTo(leaf)
      }
    }

  @Test
  fun `streamEventGroups does not inherit from siblings or path prefixes without a delimiter`():
    Unit = runBlocking {
    createGroup("campaign", "c", CAMPAIGN_PATH, "Bicycle")
    createGroup("creative", "l1", "${CAMPAIGN_PATH}creative/l1/", "Sneaker")
    createGroup("creative", "l2", "${CAMPAIGN_PATH}creative/l2/", "Boot")
    createGroup("creative", "l3", "advertiser/a/campaign/c2/creative/l3/", "Sandals")

    assertThat(search("sneaker boot")).isEmpty()
    assertThat(search("bicycle sandals")).isEmpty()
  }

  @Test
  fun `streamEventGroups isolates identical ancestor paths by measurement consumer`(): Unit =
    runBlocking {
      val otherConsumer =
        population.createMeasurementConsumer(
          services.measurementConsumersService,
          services.accountsService,
        )
      createGroup("campaign", "c", CAMPAIGN_PATH, "Private", consumer = otherConsumer)
      createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Sneaker")

      assertThat(search("private")).isEmpty()
    }

  @Test
  fun `streamEventGroups isolates identical ancestor paths by data provider`(): Unit = runBlocking {
    val otherProvider = population.createDataProvider(services.dataProvidersService)
    createGroup("campaign", "c", CAMPAIGN_PATH, "Private", provider = otherProvider)
    createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Sneaker")

    assertThat(search("private")).isEmpty()
  }

  @Test
  fun `streamEventGroups stops inheriting when hierarchy is pending`(): Unit = runBlocking {
    createGroup("campaign", "c", CAMPAIGN_PATH, "Bicycle")
    val leaf = createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Sneaker")
    assertThat(search("bicycle")).containsExactly(leaf)
    database.databaseClient.readWriteTransaction().run { transaction ->
      transaction.executeUpdate(
        Statement.of(
          "UPDATE EventGroupHierarchies SET State = $PENDING_HIERARCHY_STATE WHERE EventGroupHierarchyId = 1"
        )
      )
    }

    assertThat(search("bicycle")).isEmpty()
    assertThat(search("sneaker")).containsExactly(leaf)
  }

  @Test
  fun `streamEventGroups excludes deleted ancestor metadata`(): Unit = runBlocking {
    val parent = createGroup("campaign", "c", CAMPAIGN_PATH, "Bicycle")
    val leaf = createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Sneaker")
    setDeleted(parent)

    assertThat(search("bicycle")).isEmpty()
    assertThat(search("sneaker")).containsExactly(leaf)
  }

  @Test
  fun `streamEventGroups does not inherit onto deleted leaves when showing deleted`(): Unit =
    runBlocking {
      createGroup("campaign", "c", CAMPAIGN_PATH, "Bicycle")
      val leaf = createGroup("creative", "l", "${CAMPAIGN_PATH}creative/l/", "Sneaker")
      setDeleted(leaf)
      val request = searchRequest("bicycle").copy { filter = filter.copy { showDeleted = true } }

      assertThat(services.eventGroupsService.streamEventGroups(request).toList()).isEmpty()
    }

  @Test
  fun `streamEventGroups searches local values outside a hierarchy`(): Unit = runBlocking {
    createGroup("campaign", "c", CAMPAIGN_PATH, "Bicycle")
    val leaf = createGroup("creative", "l", null, "Sneaker")

    assertThat(search("bicycle")).isEmpty()
    assertThat(search("sneaker")).containsExactly(leaf)
  }

  @Test
  fun `streamEventGroups applies pagination to inherited matches`(): Unit = runBlocking {
    createGroup("campaign", "c", CAMPAIGN_PATH, "Bicycle")
    val expected =
      (1..5)
        .map { id -> createGroup("creative", "l$id", "${CAMPAIGN_PATH}creative/l$id/", "Sneaker") }
        .sortedBy { it.externalEventGroupId }
    createGroup("creative", "other", "advertiser/a/campaign/other/creative/other/", "Sneaker")
    val results = mutableListOf<EventGroup>()
    var request = searchRequest("bicycle sneaker").copy { limit = 2 }
    do {
      val page = services.eventGroupsService.streamEventGroups(request).toList()
      assertThat(page.size).isAtMost(2)
      results += page
      if (page.isEmpty()) break
      assertThat(results.size).isAtMost(expected.size)
      request =
        request.copy {
          filter =
            filter.copy {
              after = after {
                eventGroupKey = eventGroupKey {
                  externalDataProviderId = page.last().externalDataProviderId
                  externalEventGroupId = page.last().externalEventGroupId
                }
              }
            }
        }
    } while (true)

    assertThat(results).containsExactlyElementsIn(expected).inOrder()
  }

  private suspend fun createGroup(
    type: String,
    id: String,
    path: String?,
    name: String,
    provider: DataProvider = dataProvider,
    consumer: MeasurementConsumer = measurementConsumer,
  ): EventGroup {
    val created =
      services.eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            externalDataProviderId = provider.externalDataProviderId
            externalMeasurementConsumerId = consumer.externalMeasurementConsumerId
            entityKey = entityKey {
              entityType = type
              entityId = id
            }
            entityMetadata = struct {
              fields["${type}_id"] = value { stringValue = id }
              fields["${type}_name"] = value { stringValue = name }
            }
          }
        }
      )
    // Seed normalized paths independently of the hierarchy write-path implementation.
    database.databaseClient.readWriteTransaction().run { transaction ->
      transaction.executeUpdate(
        Statement.newBuilder(
            "UPDATE EventGroups SET EntityPath = @path, IsLeaf = @isLeaf WHERE ExternalEventGroupId = @id AND DataProviderId = " +
              "(SELECT DataProviderId FROM DataProviders WHERE ExternalDataProviderId = @provider)"
          )
          .bind("path")
          .to(path)
          .bind("isLeaf")
          .to(type == "creative")
          .bind("id")
          .to(created.externalEventGroupId)
          .bind("provider")
          .to(provider.externalDataProviderId)
          .build()
      )
    }
    return created
  }

  private suspend fun updateMetadata(group: EventGroup, metadata: Struct) {
    services.eventGroupsService.updateEventGroup(
      updateEventGroupRequest {
        eventGroup =
          group.copy {
            entityMetadata = group.entityMetadata.copy { fields.putAll(metadata.fieldsMap) }
          }
      }
    )
  }

  private suspend fun setDeleted(group: EventGroup) {
    database.databaseClient.readWriteTransaction().run { transaction ->
      transaction.executeUpdate(
        Statement.newBuilder(
            "UPDATE EventGroups SET State = @state WHERE ExternalEventGroupId = @id AND DataProviderId = " +
              "(SELECT DataProviderId FROM DataProviders WHERE ExternalDataProviderId = @provider)"
          )
          .bind("state")
          .to(EventGroup.State.DELETED.number.toLong())
          .bind("id")
          .to(group.externalEventGroupId)
          .bind("provider")
          .to(group.externalDataProviderId)
          .build()
      )
    }
  }

  private suspend fun search(query: String): List<EventGroup> =
    services.eventGroupsService.streamEventGroups(searchRequest(query)).toList()

  private fun searchRequest(query: String): StreamEventGroupsRequest = streamEventGroupsRequest {
    filter = filter {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      entityTypeIn += "creative"
      metadataSearchQuery = query
    }
  }

  companion object {
    private const val CAMPAIGN_PATH = "advertiser/a/campaign/c/"
    private const val PENDING_HIERARCHY_STATE = 1L
    private const val ACTIVE_HIERARCHY_STATE = 2L
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
