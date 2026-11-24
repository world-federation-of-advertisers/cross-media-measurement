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

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TimestampBound
import com.google.cloud.spanner.Value as SpannerValue
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.struct
import com.google.protobuf.value
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
import kotlin.random.Random
import kotlin.test.assertFailsWith
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
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyDetailsKt.level
import org.wfanet.measurement.internal.kingdom.EventGroupHierarchyState
import org.wfanet.measurement.internal.kingdom.EventGroupKt.entityKey
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.FilterKt.after
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.batchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.eventGroupHierarchyDetails
import org.wfanet.measurement.internal.kingdom.eventGroupKey
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.common.service.KingdomDataServices
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging.EventGroupMetadataStore
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging.MetadataWriteGuard
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamEventGroups
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.Population

@RunWith(JUnit4::class)
class SpannerEventGroupMetadataServiceTest {
  @get:Rule
  val database = SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.KINGDOM_CHANGELOG_PATH)
  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)
  private val clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock, Random(1))
  private val population = Population(clock, idGenerator)
  private lateinit var services: KingdomDataServices
  private val service: EventGroupsCoroutineImplBase
    get() = services.eventGroupsService

  private val store: EventGroupMetadataStore
    get() = EventGroupMetadataStore(database.databaseClient)

  private var operationId = 0

  private data class Fixture(val leaf: EventGroup, val provider: Long, val consumer: Long)

  @Before
  fun setUp() {
    services =
      SpannerDataServices(
          clock,
          idGenerator,
          database.databaseClient,
          enableEntityMetadataSearch = true,
        )
        .buildDataServices()
  }

  @Test
  fun `entity metadata search stays disabled by default`() =
    runBlocking<Unit> {
      services =
        SpannerDataServices(clock, idGenerator, database.databaseClient).buildDataServices()
      val fixture = fixture()
      assertThat(list(fixture, "original")).isEmpty()
      publish(fixture)
      assertThat(get(fixture.leaf).entityMetadata.fieldsMap.getValue("account_name").stringValue)
        .isEqualTo("renamed account")
      assertThat(list(fixture, "renamed")).isEmpty()
    }

  @Test
  fun `Get List and search switch to the published version together`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val work = start(fixture)
      store.stageNextBatch(work, 1)
      assertThat(get(fixture.leaf).entityMetadata).isEqualTo(fixture.leaf.entityMetadata)
      assertThat(list(fixture, "renamed")).isEmpty()
      assertThat(list(fixture, "original")).hasSize(3)
      store.stageNextBatch(work, 40)
      store.publish(work)
      assertThat(get(fixture.leaf).entityMetadata.fieldsMap.getValue("account_name").stringValue)
        .isEqualTo("renamed account")
      val consumerRead =
        service.getEventGroup(
          getEventGroupRequest {
            externalMeasurementConsumerId = fixture.leaf.externalMeasurementConsumerId
            externalEventGroupId = fixture.leaf.externalEventGroupId
            view = EventGroup.View.WITH_ACTIVITY_SUMMARY
          }
        )
      assertThat(consumerRead.entityMetadata).isEqualTo(get(fixture.leaf).entityMetadata)
      assertThat(list(fixture, "original")).isEmpty()
      assertThat(list(fixture, "renamed")).hasSize(3)
      assertThat(list(fixture, "account_name")).isEmpty()
      assertThat(
          list(fixture).map { it.entityMetadata.fieldsMap.getValue("account_name").stringValue }
        )
        .containsExactly("renamed account", "renamed account", "renamed account")
      val first =
        service
          .streamEventGroups(
            streamEventGroupsRequest {
              filter = filter {
                externalDataProviderId = fixture.leaf.externalDataProviderId
                metadataSearchQuery = "renamed"
              }
              limit = 1
            }
          )
          .single()
      val rest =
        service
          .streamEventGroups(
            streamEventGroupsRequest {
              filter = filter {
                externalDataProviderId = fixture.leaf.externalDataProviderId
                metadataSearchQuery = "renamed"
                after = after {
                  eventGroupKey = eventGroupKey {
                    externalDataProviderId = first.externalDataProviderId
                    externalEventGroupId = first.externalEventGroupId
                  }
                }
              }
            }
          )
          .toList()
      assertThat(rest).hasSize(2)
      assertThat(rest.map { it.externalEventGroupId }).doesNotContain(first.externalEventGroupId)
    }

  @Test
  fun `list and search continue on their snapshot across a 51 leaf publication`() =
    runBlocking<Unit> {
      val fixture = fixture()
      service.batchCreateEventGroups(
        batchCreateEventGroupsRequest {
          externalDataProviderId = fixture.leaf.externalDataProviderId
          requests +=
            (2..51).map { index ->
              createEventGroupRequest { eventGroup = fixture.leaf.withCreativeId("leaf-$index") }
            }
        }
      )
      val work = start(fixture)
      assertThat(store.stageNextBatch(work, 50))
        .isEqualTo(EventGroupMetadataStore.Progress(50, complete = false))
      assertThat(get(fixture.leaf).entityMetadata).isEqualTo(fixture.leaf.entityMetadata)
      assertThat(list(fixture, "renamed")).isEmpty()
      val originalIds = list(fixture).map { it.externalEventGroupId }
      assertThat(originalIds).hasSize(53)

      val queries = listOf("", "original")
      val (firstPages, snapshotBound) =
        database.databaseClient.readOnlyTransaction().use { snapshot ->
          val pages = queries.associateWith { snapshotPage(snapshot, fixture, it, limit = 25) }
          pages to TimestampBound.ofReadTimestamp(snapshot.readTimestamp)
        }
      assertThat(store.stageNextBatch(work, 50))
        .isEqualTo(EventGroupMetadataStore.Progress(53, complete = true))

      // Discard the publish response and retry using the same operation.
      store.publish(work)
      val operation = store.getOperation(work.operationId)
      assertThat(operation.state).isEqualTo(EventGroupMetadataStore.State.SUCCEEDED)
      assertThat(store.publish(work)).isEqualTo(operation.publicationSequence)

      database.databaseClient.readOnlyTransaction(snapshotBound).use { snapshot ->
        for ((query, firstPage) in firstPages) {
          assertThat(firstPage).hasSize(25)
          val continuation =
            snapshotPage(snapshot, fixture, query, previousPageEnd = firstPage.last())
          val results = firstPage + continuation
          assertThat(results.map { it.externalEventGroupId })
            .containsExactlyElementsIn(originalIds)
            .inOrder()
          assertThat(
              results.map { it.entityMetadata.fieldsMap.getValue("account_name").stringValue }
            )
            .containsExactlyElementsIn(List(53) { "original account" })
        }
      }
      assertThat(get(fixture.leaf).entityMetadata.fieldsMap.getValue("account_name").stringValue)
        .isEqualTo("renamed account")
      assertThat(list(fixture, "original")).isEmpty()
      assertThat(list(fixture, "renamed").map { it.externalEventGroupId })
        .containsExactlyElementsIn(originalIds)
        .inOrder()
    }

  @Test
  fun `ordinary updates supersede published metadata and reject stale shared values`() =
    runBlocking<Unit> {
      val fixture = fixture()
      publish(fixture)
      val changed = get(fixture.leaf).withMetadata("creative_name" to "revised creative")
      val updated = service.updateEventGroup(updateEventGroupRequest { eventGroup = changed })
      assertThat(get(fixture.leaf).entityMetadata).isEqualTo(updated.entityMetadata)
      assertThat(list(fixture, "revised").map { it.externalEventGroupId })
        .containsExactly(fixture.leaf.externalEventGroupId)
      assertStatus(Status.Code.FAILED_PRECONDITION) {
        service.updateEventGroup(updateEventGroupRequest { eventGroup = fixture.leaf })
      }
      publish(fixture, "latest account")
      assertThat(get(fixture.leaf).entityMetadata.fieldsMap.getValue("creative_name").stringValue)
        .isEqualTo("revised creative")
      assertThat(list(fixture, "renamed")).isEmpty()
      assertThat(list(fixture, "latest")).hasSize(3)
    }

  @Test
  fun `create after publication validates against visible ancestor metadata`() =
    runBlocking<Unit> {
      val fixture = fixture()
      publish(fixture)
      assertStatus(Status.Code.FAILED_PRECONDITION) { create(fixture.leaf, "second") }
      val created = create(get(fixture.leaf), "second")
      assertThat(list(fixture, "renamed")).hasSize(4)
      assertThat(get(created).entityMetadata.fieldsMap.getValue("account_name").stringValue)
        .isEqualTo("renamed account")
    }

  @Test
  fun `subtree lock rejects create update batch and delete through the service`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val work = start(fixture)
      val changed = fixture.leaf.withMetadata("creative_name" to "changed")
      assertStatus(Status.Code.ABORTED) { create(fixture.leaf, "second") }
      assertStatus(Status.Code.ABORTED) {
        service.updateEventGroup(updateEventGroupRequest { eventGroup = changed })
      }
      assertStatus(Status.Code.ABORTED) {
        service.batchCreateEventGroups(
          batchCreateEventGroupsRequest {
            externalDataProviderId = fixture.leaf.externalDataProviderId
            requests += createEventGroupRequest {
              eventGroup = fixture.leaf.withCreativeId("second")
            }
          }
        )
      }
      assertStatus(Status.Code.ABORTED) {
        service.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            externalDataProviderId = fixture.leaf.externalDataProviderId
            requests += updateEventGroupRequest { eventGroup = changed }
          }
        )
      }
      assertStatus(Status.Code.ABORTED) { delete(fixture.leaf) }
      assertThat(get(fixture.leaf).entityMetadata).isEqualTo(fixture.leaf.entityMetadata)
      store.cancel(work.operationId)
      service.updateEventGroup(updateEventGroupRequest { eventGroup = changed })
      assertThat(get(fixture.leaf).entityMetadata).isEqualTo(changed.entityMetadata)
    }

  @Test
  fun `move checks both old and destination subtrees and keeps unrelated writes available`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val neighbor =
        create(fixture.leaf.withMetadata("account_id" to "X2", "campaign_id" to "C2"), "neighbor")
      val work = start(fixture)
      assertStatus(Status.Code.ABORTED) {
        service.updateEventGroup(
          updateEventGroupRequest {
            eventGroup = neighbor.withMetadata("account_id" to "X", "campaign_id" to "C")
          }
        )
      }
      assertStatus(Status.Code.ABORTED) {
        service.updateEventGroup(
          updateEventGroupRequest {
            eventGroup = fixture.leaf.withMetadata("account_id" to "X2", "campaign_id" to "C2")
          }
        )
      }
      val changed = neighbor.withMetadata("creative_name" to "unrelated")
      service.updateEventGroup(updateEventGroupRequest { eventGroup = changed })
      assertThat(get(neighbor).entityMetadata).isEqualTo(changed.entityMetadata)
      store.cancel(work.operationId)
    }

  @Test
  fun `hierarchy change guard blocks real creates updates and deletes`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val guard = MetadataWriteGuard(database.databaseClient)
      guard.beginHierarchyChange(fixture.provider, "activation")
      assertStatus(Status.Code.ABORTED) { create(fixture.leaf, "second") }
      assertStatus(Status.Code.ABORTED) {
        service.updateEventGroup(updateEventGroupRequest { eventGroup = fixture.leaf })
      }
      assertStatus(Status.Code.ABORTED) { delete(fixture.leaf) }
      assertStatus(Status.Code.ABORTED) { start(fixture) }
      guard.finishHierarchyChange("activation")
      create(fixture.leaf, "second")
    }

  @Test
  fun `move and delete recompute both ancestor chains without changing resource identity`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val sourceSibling =
        create(
          fixture.leaf.copy {
            mediaTypes.clear()
            mediaTypes += MediaType.DISPLAY
            dataAvailabilityInterval = interval {
              startTime = Instant.ofEpochSecond(20).toProtoTime()
              endTime = Instant.ofEpochSecond(30).toProtoTime()
            }
          },
          "source-sibling",
        )
      val destinationSibling =
        create(
          fixture.leaf.withMetadata("account_id" to "X2", "campaign_id" to "C2").copy {
            mediaTypes.clear()
            mediaTypes += MediaType.DISPLAY
            dataAvailabilityInterval = interval {
              startTime = Instant.ofEpochSecond(25).toProtoTime()
              endTime = Instant.ofEpochSecond(50).toProtoTime()
            }
          },
          "destination-sibling",
        )
      val before = list(fixture)
      val moved =
        service.updateEventGroup(
          updateEventGroupRequest {
            eventGroup = fixture.leaf.withMetadata("account_id" to "X2", "campaign_id" to "C2")
          }
        )
      assertThat(moved.externalEventGroupId).isEqualTo(fixture.leaf.externalEventGroupId)
      val sourceAncestors = before.filter { it.entityKey.entityId in setOf("X", "C") }
      val destinationAncestors = before.filter { it.entityKey.entityId in setOf("X2", "C2") }
      assertThat(sourceAncestors).hasSize(2)
      assertThat(destinationAncestors).hasSize(2)
      for (ancestor in sourceAncestors) {
        val current = get(ancestor)
        assertThat(current.mediaTypesList).containsExactly(MediaType.DISPLAY)
        assertThat(current.dataAvailabilityInterval)
          .isEqualTo(sourceSibling.dataAvailabilityInterval)
      }
      for (ancestor in destinationAncestors) {
        val current = get(ancestor)
        assertThat(current.mediaTypesList).containsExactly(MediaType.VIDEO, MediaType.DISPLAY)
        assertThat(current.dataAvailabilityInterval)
          .isEqualTo(
            interval {
              startTime = fixture.leaf.dataAvailabilityInterval.startTime
              endTime = destinationSibling.dataAvailabilityInterval.endTime
            }
          )
      }
      val newParent = destinationAncestors.single { it.entityKey.entityType == "campaign" }
      assertStatus(Status.Code.FAILED_PRECONDITION) { delete(newParent) }
      delete(moved)
      for (ancestor in destinationAncestors) {
        val current = get(ancestor)
        assertThat(current.mediaTypesList).containsExactly(MediaType.DISPLAY)
        assertThat(current.dataAvailabilityInterval)
          .isEqualTo(destinationSibling.dataAvailabilityInterval)
      }

      delete(destinationSibling)
      // The former parent retains its last derived fields and becomes ordinary.
      service.updateEventGroup(updateEventGroupRequest { eventGroup = get(newParent) })
      assertThat(get(newParent).mediaTypesList).containsExactly(MediaType.DISPLAY)
      assertThat(list(fixture, "leafvalue")).hasSize(1)
    }

  @Test
  fun `move demotes an ordinary destination and replaces its ancestor contribution`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val destination = createOrdinaryCampaign(fixture, accountId = "X2")
      val destinationAccount = list(fixture).single { it.entityKey.entityId == "X2" }
      assertThat(destinationAccount.dataAvailabilityInterval)
        .isEqualTo(destination.dataAvailabilityInterval)

      val moved =
        service.updateEventGroup(
          updateEventGroupRequest {
            eventGroup = fixture.leaf.withMetadata("account_id" to "X2", "campaign_id" to "C2")
          }
        )

      val derivedDestination = get(destination)
      assertThat(derivedDestination.createTime).isEqualTo(destination.createTime)
      assertThat(derivedDestination.entityMetadata.fieldsMap).doesNotContainKey("ordinary_only")
      assertStatus(Status.Code.FAILED_PRECONDITION) { delete(derivedDestination) }
      for (ancestor in listOf(derivedDestination, get(destinationAccount))) {
        assertThat(ancestor.mediaTypesList).containsExactly(MediaType.VIDEO)
        assertThat(ancestor.dataAvailabilityInterval).isEqualTo(moved.dataAvailabilityInterval)
      }
    }

  @Test
  fun `batchCreateEventGroups rolls up demotion and additions below the same ancestor`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val ordinary = createOrdinaryCampaign(fixture, accountId = "X")
      val account = list(fixture).single { it.entityKey.entityType == "account" }
      val newAvailability = interval {
        startTime = Instant.ofEpochSecond(20).toProtoTime()
        endTime = Instant.ofEpochSecond(60).toProtoTime()
      }
      service.batchCreateEventGroups(
        batchCreateEventGroupsRequest {
          externalDataProviderId = fixture.leaf.externalDataProviderId
          requests += createEventGroupRequest {
            eventGroup = fixture.leaf.withMetadata("campaign_id" to "C2").withCreativeId("second")
          }
          requests += createEventGroupRequest {
            eventGroup =
              fixture.leaf.withCreativeId("third").copy {
                mediaTypes.clear()
                mediaTypes += MediaType.DISPLAY
                dataAvailabilityInterval = newAvailability
              }
          }
        }
      )

      assertThat(get(ordinary).dataAvailabilityInterval)
        .isEqualTo(fixture.leaf.dataAvailabilityInterval)
      val rolledUpAccount = get(account)
      assertThat(rolledUpAccount.mediaTypesList).containsExactly(MediaType.VIDEO, MediaType.DISPLAY)
      assertThat(rolledUpAccount.dataAvailabilityInterval)
        .isEqualTo(
          interval {
            startTime = fixture.leaf.dataAvailabilityInterval.startTime
            endTime = newAvailability.endTime
          }
        )
    }

  @Test
  fun `batch updates reject repeated leaf IDs without demoting either destination`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val ordinary = createOrdinaryCampaign(fixture, accountId = "X2")
      val before = list(fixture)

      assertStatus(Status.Code.INVALID_ARGUMENT) {
        service.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            externalDataProviderId = fixture.leaf.externalDataProviderId
            requests += updateEventGroupRequest {
              eventGroup = fixture.leaf.withMetadata("account_id" to "X2", "campaign_id" to "C2")
            }
            requests += updateEventGroupRequest {
              eventGroup = fixture.leaf.withMetadata("account_id" to "X3", "campaign_id" to "C3")
            }
          }
        )
      }

      assertThat(list(fixture)).containsExactlyElementsIn(before).inOrder()
      service.updateEventGroup(updateEventGroupRequest { eventGroup = ordinary })
    }

  @Test
  fun `update rejects moving an ordinary EventGroup below its own path`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val parent = list(fixture).single { it.entityKey.entityType == "campaign" }
      delete(fixture.leaf)
      val ordinary = get(parent)

      assertStatus(Status.Code.FAILED_PRECONDITION) {
        service.updateEventGroup(
          updateEventGroupRequest {
            eventGroup =
              fixture.leaf.withCreativeId("new-child").copy {
                externalEventGroupId = ordinary.externalEventGroupId
              }
          }
        )
      }
      assertThat(get(parent)).isEqualTo(ordinary)
    }

  @Test
  fun `batch updates share a publication version and roll back stale requests`() =
    runBlocking<Unit> {
      val fixture = fixture()
      val second = create(fixture.leaf, "second")
      publish(fixture)
      val firstChanged = get(fixture.leaf).withMetadata("creative_name" to "firstchanged")
      val secondChanged = get(second).withMetadata("creative_name" to "secondchanged")
      assertStatus(Status.Code.FAILED_PRECONDITION) {
        service.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            externalDataProviderId = fixture.leaf.externalDataProviderId
            requests += updateEventGroupRequest { eventGroup = firstChanged }
            requests += updateEventGroupRequest { eventGroup = second }
          }
        )
      }
      assertThat(list(fixture, "firstchanged")).isEmpty()
      service.batchUpdateEventGroup(
        batchUpdateEventGroupsRequest {
          externalDataProviderId = fixture.leaf.externalDataProviderId
          requests += updateEventGroupRequest { eventGroup = firstChanged }
          requests += updateEventGroupRequest { eventGroup = secondChanged }
        }
      )
      assertThat(list(fixture, "firstchanged")).hasSize(1)
      assertThat(list(fixture, "secondchanged")).hasSize(1)
      assertThat(list(fixture, "leafvalue")).isEmpty()
    }

  private suspend fun fixture(): Fixture {
    val consumer =
      population.createMeasurementConsumer(
        services.measurementConsumersService,
        services.accountsService,
      )
    val provider = population.createDataProvider(services.dataProvidersService)
    val ids =
      database.databaseClient.singleUse().use { context ->
        context
          .executeQuery(
            Statement.newBuilder(
                "SELECT DataProviderId, MeasurementConsumerId FROM DataProviders, MeasurementConsumers WHERE ExternalDataProviderId = @dp AND ExternalMeasurementConsumerId = @mc"
              )
              .bind("dp")
              .to(provider.externalDataProviderId)
              .bind("mc")
              .to(consumer.externalMeasurementConsumerId)
              .build()
          )
          .single()
      }
    val hierarchyDetails = eventGroupHierarchyDetails {
      levels += level {
        entityType = "account"
        idEntityMetadataKey = "account_id"
        entityMetadataKeys += "account_name"
      }
      levels += level {
        entityType = "campaign"
        idEntityMetadataKey = "campaign_id"
        entityMetadataKeys += "campaign_name"
      }
      levels += level {
        entityType = "creative"
        idEntityMetadataKey = "creative_id"
        entityMetadataKeys += "creative_name"
      }
    }
    database.databaseClient.write(
      Mutation.newInsertBuilder("EventGroupHierarchies")
        .apply {
          set("DataProviderId").to(ids.getLong("DataProviderId"))
          set("EventGroupHierarchyId").to(idGenerator.generateInternalId().value)
          set("ExternalEventGroupHierarchyId").to("test-hierarchy")
          set("State").to(EventGroupHierarchyState.ACTIVE.number.toLong())
          set("EventGroupHierarchyDetails").to(SpannerValue.protoMessage(hierarchyDetails))
          set("CreateTime").to(SpannerValue.COMMIT_TIMESTAMP)
        }
        .build()
    )
    val created =
      service.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            externalDataProviderId = provider.externalDataProviderId
            externalMeasurementConsumerId = consumer.externalMeasurementConsumerId
            entityKey = entityKey {
              entityType = "creative"
              entityId = "first"
            }
            details = eventGroupDetails { apiVersion = "v2alpha" }
            mediaTypes += MediaType.VIDEO
            dataAvailabilityInterval = interval {
              startTime = Instant.ofEpochSecond(10).toProtoTime()
              endTime = Instant.ofEpochSecond(40).toProtoTime()
            }
            entityMetadata = struct {
              fields.putAll(
                mapOf(
                    "account_id" to "X",
                    "account_name" to "original account",
                    "campaign_id" to "C",
                    "campaign_name" to "shared campaign",
                    "creative_id" to "first",
                    "creative_name" to "leafvalue",
                  )
                  .mapValues { (_, text) -> value { stringValue = text } }
              )
            }
          }
        }
      )
    return Fixture(created, ids.getLong("DataProviderId"), ids.getLong("MeasurementConsumerId"))
  }

  private suspend fun start(fixture: Fixture, name: String = "renamed account") =
    store.start(
      EventGroupMetadataStore.Request(
        operationId = "operation-${++operationId}",
        dataProviderId = fixture.provider,
        measurementConsumerId = fixture.consumer,
        entityPath = "account/X/",
        ownedKeys = setOf("account_name"),
        metadata = struct { fields["account_name"] = value { stringValue = name } },
      )
    )

  private suspend fun publish(fixture: Fixture, name: String = "renamed account") {
    val work = start(fixture, name)
    store.stageNextBatch(work, 40)
    store.publish(work)
  }

  private suspend fun create(source: EventGroup, id: String) =
    service.createEventGroup(createEventGroupRequest { eventGroup = source.withCreativeId(id) })

  private suspend fun createOrdinaryCampaign(fixture: Fixture, accountId: String): EventGroup =
    service.createEventGroup(
      createEventGroupRequest {
        eventGroup =
          fixture.leaf.copy {
            entityKey = entityKey {
              entityType = "campaign"
              entityId = "C2"
            }
            entityMetadata = struct {
              fields["account_id"] = value { stringValue = accountId }
              fields["account_name"] = value { stringValue = "original account" }
              fields["campaign_id"] = value { stringValue = "C2" }
              fields["campaign_name"] = value { stringValue = "shared campaign" }
              fields["ordinary_only"] = value { stringValue = "removed on demotion" }
            }
            mediaTypes.clear()
            mediaTypes += MediaType.DISPLAY
            dataAvailabilityInterval = interval {
              startTime = Instant.ofEpochSecond(5).toProtoTime()
              endTime = Instant.ofEpochSecond(50).toProtoTime()
            }
            clearExternalEventGroupId()
          }
      }
    )

  private suspend fun get(source: EventGroup) =
    service.getEventGroup(
      getEventGroupRequest {
        externalDataProviderId = source.externalDataProviderId
        externalEventGroupId = source.externalEventGroupId
      }
    )

  private suspend fun delete(source: EventGroup) =
    service.deleteEventGroup(
      deleteEventGroupRequest {
        externalDataProviderId = source.externalDataProviderId
        externalEventGroupId = source.externalEventGroupId
      }
    )

  private suspend fun list(fixture: Fixture, query: String = "") =
    service
      .streamEventGroups(
        streamEventGroupsRequest {
          filter = filter {
            externalDataProviderId = fixture.leaf.externalDataProviderId
            metadataSearchQuery = query
          }
        }
      )
      .toList()

  private suspend fun snapshotPage(
    snapshot: AsyncDatabaseClient.ReadContext,
    fixture: Fixture,
    query: String,
    limit: Int = 0,
    previousPageEnd: EventGroup? = null,
  ): List<EventGroup> =
    StreamEventGroups(
        requestFilter =
          filter {
            externalDataProviderId = fixture.leaf.externalDataProviderId
            metadataSearchQuery = query
            if (previousPageEnd != null) {
              after = after {
                eventGroupKey = eventGroupKey {
                  externalDataProviderId = previousPageEnd.externalDataProviderId
                  externalEventGroupId = previousPageEnd.externalEventGroupId
                }
              }
            }
          },
        orderBy = StreamEventGroupsRequest.OrderBy.getDefaultInstance(),
        view = EventGroup.View.BASIC,
        internalDataProviderId = null,
        internalMeasurementConsumerId = null,
        limit = limit,
        enableEntityMetadataSearch = true,
      )
      .execute(snapshot)
      .toList()
      .map { it.eventGroup }

  private fun EventGroup.withCreativeId(id: String) =
    withMetadata("creative_id" to id).copy {
      entityKey = entityKey.copy { entityId = id }
      clearExternalEventGroupId()
      clearCreateTime()
      clearUpdateTime()
    }

  private fun EventGroup.withMetadata(vararg entries: Pair<String, String>) = copy {
    entityMetadata = struct {
      fields.putAll(this@withMetadata.entityMetadata.fieldsMap)
      for ((key, text) in entries) fields[key] = value { stringValue = text }
    }
  }

  private suspend fun assertStatus(code: Status.Code, action: suspend () -> Unit) {
    assertThat(assertFailsWith<StatusRuntimeException> { action() }.status.code).isEqualTo(code)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
