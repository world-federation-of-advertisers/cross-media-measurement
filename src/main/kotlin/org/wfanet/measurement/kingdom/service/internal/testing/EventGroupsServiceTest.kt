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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.copy
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.eventGroupMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt.orderBy
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.eventGroupKey
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private const val EXTERNAL_EVENT_GROUP_ID = 123L
private const val FIXED_EXTERNAL_ID = 6789L
private const val PROVIDED_EVENT_GROUP_ID = "ProvidedEventGroupId"
private val DETAILS = eventGroupDetails {
  apiVersion = Version.V2_ALPHA.string
  encryptedMetadata = ByteString.copyFromUtf8("somedata")
  metadata = eventGroupMetadata {
    adMetadata = adMetadata {
      campaignMetadata = campaignMetadata {
        brandName = "Blammo!"
        campaignName = "Log: Better Than Bad"
      }
    }
  }
}

@RunWith(JUnit4::class)
abstract class EventGroupsServiceTest<T : EventGroupsCoroutineImplBase> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  private val testClock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(testClock, Random(RANDOM_SEED))
  private val population = Population(testClock, idGenerator)

  private lateinit var eventGroupsService: T

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): EventGroupAndHelperServices<T>

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    eventGroupsService = services.eventGroupsService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
    accountsService = services.accountsService
  }

  @Test
  fun `getEventGroup throws INVALID_ARGUMENT when parent ID omitted`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.getEventGroup(
          GetEventGroupRequest.newBuilder().setExternalEventGroupId(EXTERNAL_EVENT_GROUP_ID).build()
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("external_parent_id")
  }

  @Test
  fun `getEventGroup throws NOT_FOUND when EventGroup not found`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.getEventGroup(
          getEventGroupRequest {
            externalDataProviderId = 404L
            externalEventGroupId = EXTERNAL_EVENT_GROUP_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsAtLeast(
        "external_data_provider_id",
        "404",
        "external_event_group_id",
        EXTERNAL_EVENT_GROUP_ID.toString(),
      )
  }

  @Test
  fun `deleteEventGroup fails for missing EventGroup`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.deleteEventGroup(
          deleteEventGroupRequest {
            this.externalDataProviderId = FIXED_EXTERNAL_ID
            this.externalEventGroupId = EXTERNAL_EVENT_GROUP_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup")
  }

  @Test
  fun `createEventGroup fails for missing data provider`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val eventGroup = eventGroup {
      externalDataProviderId = FIXED_EXTERNAL_ID
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.createEventGroup(
          createEventGroupRequest { this.eventGroup = eventGroup }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `createEventGroup fails for missing measurement consumer`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup = eventGroup {
      this.externalDataProviderId = externalDataProviderId
      externalMeasurementConsumerId = FIXED_EXTERNAL_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.createEventGroup(
          createEventGroupRequest { this.eventGroup = eventGroup }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `createEventGroup returns created EventGroup`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val request = createEventGroupRequest {
      this.eventGroup = eventGroup {
        this.externalDataProviderId = externalDataProviderId
        this.externalMeasurementConsumerId = externalMeasurementConsumerId
        providedEventGroupId = PROVIDED_EVENT_GROUP_ID
        mediaTypes += MediaType.VIDEO
        details = DETAILS
      }
    }

    val response: EventGroup = eventGroupsService.createEventGroup(request)

    assertThat(response)
      .ignoringFields(
        EventGroup.EXTERNAL_EVENT_GROUP_ID_FIELD_NUMBER,
        EventGroup.CREATE_TIME_FIELD_NUMBER,
        EventGroup.UPDATE_TIME_FIELD_NUMBER,
      )
      .isEqualTo(request.eventGroup.copy { this.state = EventGroup.State.ACTIVE })
    assertThat(response.externalEventGroupId).isNotEqualTo(0)
    assertThat(response.createTime.seconds).isGreaterThan(0)
    assertThat(response.updateTime).isEqualTo(response.createTime)
    assertThat(response)
      .isEqualTo(
        eventGroupsService.getEventGroup(
          getEventGroupRequest {
            this.externalDataProviderId = externalDataProviderId
            externalEventGroupId = response.externalEventGroupId
          }
        )
      )
  }

  @Test
  fun `createEventGroup returns created EventGroup with data availability interval`() =
    runBlocking {
      val measurementConsumer: MeasurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
      val request = createEventGroupRequest {
        this.eventGroup = eventGroup {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedEventGroupId = PROVIDED_EVENT_GROUP_ID
          mediaTypes += MediaType.VIDEO
          dataAvailabilityInterval = interval { startTime = testClock.instant().toProtoTime() }
          details = DETAILS
        }
      }

      val response: EventGroup = eventGroupsService.createEventGroup(request)

      assertThat(response.dataAvailabilityInterval)
        .isEqualTo(request.eventGroup.dataAvailabilityInterval)
      assertThat(response)
        .isEqualTo(
          eventGroupsService.getEventGroup(
            getEventGroupRequest {
              this.externalDataProviderId = response.externalDataProviderId
              externalEventGroupId = response.externalEventGroupId
            }
          )
        )
    }

  @Test
  fun `createEventGroup returns existing EventGroup for same request ID`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val request = createEventGroupRequest {
      eventGroup = eventGroup {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
      requestId = "foo"
    }
    val existingEventGroup: EventGroup = eventGroupsService.createEventGroup(request)

    val response: EventGroup = eventGroupsService.createEventGroup(request)

    assertThat(response).isEqualTo(existingEventGroup)
  }

  @Test
  fun `createEventGroup creates new EventGroup for different request ID`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider = population.createDataProvider(dataProvidersService)
    val request = createEventGroupRequest {
      eventGroup = eventGroup {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
      requestId = "foo"
    }
    val existingEventGroup: EventGroup = eventGroupsService.createEventGroup(request)

    val response: EventGroup =
      eventGroupsService.createEventGroup(request.copy { requestId = "bar" })

    assertThat(response.externalEventGroupId).isNotEqualTo(existingEventGroup.externalEventGroupId)
  }

  @Test
  fun `updateEventGroup fails for missing EventGroup`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.updateEventGroup(
          updateEventGroupRequest { eventGroup { this.externalEventGroupId = 1L } }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ExternalDataProviderId unspecified")
  }

  @Test
  fun `updateEventGroup fails for missing data provider`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val createdEventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
          }
        }
      )

    val modifyEventGroup = createdEventGroup.copy { this.externalDataProviderId = 1L }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.updateEventGroup(
          updateEventGroupRequest { eventGroup = modifyEventGroup }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup not found")
  }

  @Test
  fun `updateEventGroup fails for missing measurement consumer`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val createdEventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
          }
        }
      )

    val modifyEventGroup = createdEventGroup.copy { this.externalMeasurementConsumerId = 1L }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.updateEventGroup(
          updateEventGroupRequest { eventGroup = modifyEventGroup }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("EventGroup modification param is invalid")
  }

  @Test
  fun `updateEventGroup succeeds`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val eventGroup: EventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            providedEventGroupId = PROVIDED_EVENT_GROUP_ID
            mediaTypes += MediaType.VIDEO
            mediaTypes += MediaType.OTHER
          }
        }
      )
    val request = updateEventGroupRequest {
      this.eventGroup =
        eventGroup.copy {
          details = eventGroupDetails { encryptedMetadata = ByteString.copyFromUtf8("metadata") }
          dataAvailabilityInterval = interval {
            startTime = now.minus(90L, ChronoUnit.DAYS).toProtoTime()
            endTime = now.minus(3L, ChronoUnit.DAYS).toProtoTime()
          }
          mediaTypes.clear()
          mediaTypes += MediaType.DISPLAY
          mediaTypes += MediaType.OTHER
        }
    }

    val response: EventGroup = eventGroupsService.updateEventGroup(request)

    assertThat(response)
      .ignoringFields(EventGroup.UPDATE_TIME_FIELD_NUMBER)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .isEqualTo(request.eventGroup)
    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .isEqualTo(
        eventGroupsService.getEventGroup(
          getEventGroupRequest {
            this.externalDataProviderId = eventGroup.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
          }
        )
      )
  }

  @Test
  fun `updateEventGroup succeeds when clearing non-required fields`(): Unit = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val createdEventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            providedEventGroupId = PROVIDED_EVENT_GROUP_ID
          }
        }
      )

    val modifyEventGroup = createdEventGroup.copy { providedEventGroupId = "" }

    val updatedEventGroup =
      eventGroupsService.updateEventGroup(updateEventGroupRequest { eventGroup = modifyEventGroup })

    assertThat(updatedEventGroup)
      .isEqualTo(
        createdEventGroup
          .toBuilder()
          .also {
            it.updateTime = updatedEventGroup.updateTime
            it.providedEventGroupId = ""
          }
          .build()
      )
  }

  @Test
  fun `getEventGroup returns EventGroup by MeasurementConsumer`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val eventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
          }
        }
      )

    val response: EventGroup =
      eventGroupsService.getEventGroup(
        getEventGroupRequest {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          externalEventGroupId = eventGroup.externalEventGroupId
        }
      )

    assertThat(response).isEqualTo(eventGroup)
  }

  @Test
  fun `streamEventGroups returns all eventGroups in order`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val eventGroup1 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId =
              population
                .createMeasurementConsumer(measurementConsumersService, accountsService)
                .externalMeasurementConsumerId
            providedEventGroupId = "eventGroup1"
          }
        }
      )
    val eventGroup2 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId =
              population
                .createMeasurementConsumer(measurementConsumersService, accountsService)
                .externalMeasurementConsumerId
            providedEventGroupId = "eventGroup2"
          }
        }
      )
    val eventGroups =
      listOf(eventGroup1, eventGroup2)
        .sortedWith(
          compareBy<EventGroup> { it.externalDataProviderId }.thenBy { it.externalEventGroupId }
        )

    val response: List<EventGroup> =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter { externalDataProviderIdIn += externalDataProviderId }
          }
        )
        .toList()

    assertThat(response).containsExactlyElementsIn(eventGroups).inOrder()
  }

  @Test
  fun `streamEventGroups can get one page at a time`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup1 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId =
              population
                .createMeasurementConsumer(measurementConsumersService, accountsService)
                .externalMeasurementConsumerId
            providedEventGroupId = "eventGroup1"
          }
        }
      )

    val eventGroup2 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId =
              population
                .createMeasurementConsumer(measurementConsumersService, accountsService)
                .externalMeasurementConsumerId
            providedEventGroupId = "eventGroup2"
          }
        }
      )

    val eventGroups: List<EventGroup> =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter { externalDataProviderIdIn += externalDataProviderId }
            limit = 1
          }
        )
        .toList()

    assertThat(eventGroups).containsAnyOf(eventGroup1, eventGroup2)
    assertThat(eventGroups).hasSize(1)

    val eventGroups2: List<EventGroup> =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              this.externalDataProviderId = externalDataProviderId
              after =
                StreamEventGroupsRequestKt.FilterKt.after {
                  eventGroupKey = eventGroupKey {
                    this.externalDataProviderId = eventGroups[0].externalDataProviderId
                    externalEventGroupId = eventGroups[0].externalEventGroupId
                  }
                }
            }
            limit = 1
          }
        )
        .toList()

    assertThat(eventGroups2).hasSize(1)
    assertThat(eventGroups2).containsAnyOf(eventGroup1, eventGroup2)
    assertThat(eventGroups2[0].externalEventGroupId)
      .isGreaterThan(eventGroups[0].externalEventGroupId)
  }

  @Test
  fun `streamEventGroups respects limit`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    eventGroupsService.createEventGroup(
      createEventGroupRequest {
        eventGroup = eventGroup {
          this.externalDataProviderId = externalDataProviderId
          this.externalMeasurementConsumerId =
            population
              .createMeasurementConsumer(measurementConsumersService, accountsService)
              .externalMeasurementConsumerId
          providedEventGroupId = "eventGroup1"
        }
      }
    )

    eventGroupsService.createEventGroup(
      createEventGroupRequest {
        eventGroup = eventGroup {
          this.externalDataProviderId = externalDataProviderId
          this.externalMeasurementConsumerId =
            population
              .createMeasurementConsumer(measurementConsumersService, accountsService)
              .externalMeasurementConsumerId
          providedEventGroupId = "eventGroup2"
        }
      }
    )

    val eventGroups: List<EventGroup> =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter { this.externalDataProviderId = externalDataProviderId }
            limit = 1
          }
        )
        .toList()

    assertThat(eventGroups).hasSize(1)
  }

  @Test
  fun `streamEventGroups respects externalMeasurementConsumerIdIn`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    eventGroupsService.createEventGroup(
      createEventGroupRequest {
        eventGroup = eventGroup {
          this.externalDataProviderId = externalDataProviderId
          this.externalMeasurementConsumerId =
            population
              .createMeasurementConsumer(measurementConsumersService, accountsService)
              .externalMeasurementConsumerId
          providedEventGroupId = "eventGroup1"
        }
      }
    )

    val eventGroup2 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId =
              population
                .createMeasurementConsumer(measurementConsumersService, accountsService)
                .externalMeasurementConsumerId
            providedEventGroupId = "eventGroup2"
          }
        }
      )

    val eventGroups: List<EventGroup> =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              this.externalDataProviderId = externalDataProviderId
              this.externalMeasurementConsumerIdIn += eventGroup2.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(eventGroups).comparingExpectedFieldsOnly().containsExactly(eventGroup2)
  }

  @Test
  fun `streamEventGroups respects metadata search query on campaign name`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider1: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2: DataProvider = population.createDataProvider(dataProvidersService)
    val eventGroups: List<EventGroup> =
      populateTestEventGroups(now, measurementConsumer, dataProvider1, dataProvider2)

    val response =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              metadataSearchQuery = "better"
            }
          }
        )
        .toList()

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .containsExactlyElementsIn(
        eventGroups.filter {
          it.details.metadata.adMetadata.campaignMetadata.campaignName.contains(
            "better",
            ignoreCase = true,
          )
        }
      )
  }

  @Test
  fun `streamEventGroups respects metadata search query on brand name`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider1: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2: DataProvider = population.createDataProvider(dataProvidersService)
    val eventGroups: List<EventGroup> =
      populateTestEventGroups(now, measurementConsumer, dataProvider1, dataProvider2)

    val response =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              metadataSearchQuery = "log"
            }
          }
        )
        .toList()

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .containsExactlyElementsIn(
        eventGroups.filter {
          it.details.metadata.adMetadata.campaignMetadata.brandName.contains(
            "log",
            ignoreCase = true,
          )
        }
      )
  }

  @Test
  fun `streamEventGroups respects media types filter`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider1: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2: DataProvider = population.createDataProvider(dataProvidersService)
    val eventGroups: List<EventGroup> =
      populateTestEventGroups(now, measurementConsumer, dataProvider1, dataProvider2)

    val response =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              mediaTypesIntersect += MediaType.VIDEO
              mediaTypesIntersect += MediaType.DISPLAY
            }
          }
        )
        .toList()

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .containsExactlyElementsIn(
        eventGroups.filter {
          it.mediaTypesList.contains(MediaType.VIDEO) ||
            it.mediaTypesList.contains(MediaType.DISPLAY)
        }
      )
  }

  @Test
  fun `streamEventGroups respects data availability interval filter`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider1: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2: DataProvider = population.createDataProvider(dataProvidersService)
    val eventGroups: List<EventGroup> =
      populateTestEventGroups(now, measurementConsumer, dataProvider1, dataProvider2)
    val filterRange: ClosedRange<Instant> =
      now.minus(92L, ChronoUnit.DAYS)..now.minus(59L, ChronoUnit.DAYS)

    val response =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              dataAvailabilityStartTimeOnOrAfter = filterRange.start.toProtoTime()
              dataAvailabilityEndTimeOnOrBefore = filterRange.endInclusive.toProtoTime()
            }
          }
        )
        .toList()

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .containsExactlyElementsIn(
        eventGroups.filter {
          it.dataAvailabilityInterval.startTime.toInstant() in filterRange &&
            (it.dataAvailabilityInterval.hasEndTime() &&
              it.dataAvailabilityInterval.endTime.toInstant() in filterRange)
        }
      )
  }

  @Test
  fun `streamEventGroups respects orderBy`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider1: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2: DataProvider = population.createDataProvider(dataProvidersService)
    val eventGroups: List<EventGroup> =
      populateTestEventGroups(now, measurementConsumer, dataProvider1, dataProvider2)

    val response =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
            orderBy = orderBy {
              field = StreamEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
              descending = true
            }
          }
        )
        .toList()

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .containsExactlyElementsIn(
        eventGroups.sortedWith(
          compareByDescending<EventGroup> { it.dataAvailabilityInterval.startTime.toInstant() }
            .thenBy { it.externalDataProviderId }
            .thenBy { it.externalEventGroupId }
        )
      )
      .inOrder()
  }

  /** Populates a set of test [EventGroup]s for the same [measurementConsumer]. */
  private suspend fun populateTestEventGroups(
    now: Instant,
    measurementConsumer: MeasurementConsumer,
    dataProvider1: DataProvider,
    dataProvider2: DataProvider,
  ): List<EventGroup> {
    val eventGroup1 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            externalDataProviderId = dataProvider1.externalDataProviderId
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            dataAvailabilityInterval = interval {
              startTime = now.minus(90L, ChronoUnit.DAYS).toProtoTime()
              endTime = now.minus(60L, ChronoUnit.DAYS).toProtoTime()
            }
            mediaTypes += MediaType.VIDEO
            details = eventGroupDetails {
              metadata = eventGroupMetadata {
                adMetadata = adMetadata {
                  campaignMetadata = campaignMetadata {
                    brandName = "Log, from Blammo!"
                    campaignName = "Better Than Bad"
                  }
                }
              }
            }
          }
        }
      )
    val eventGroup2 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup =
            eventGroup1.copy {
              externalDataProviderId = dataProvider2.externalDataProviderId
              dataAvailabilityInterval =
                dataAvailabilityInterval.copy {
                  startTime = now.minus(91L, ChronoUnit.DAYS).toProtoTime()
                  clearEndTime()
                }
              mediaTypes.clear()
              mediaTypes += MediaType.OTHER
            }
        }
      )
    val eventGroup3 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup =
            eventGroup1.copy {
              mediaTypes += MediaType.DISPLAY
              details =
                details.copy {
                  metadata =
                    metadata.copy {
                      adMetadata =
                        adMetadata.copy {
                          campaignMetadata = campaignMetadata.copy { campaignName = "It's Good" }
                        }
                    }
                }
            }
        }
      )
    val eventGroup4 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup =
            eventGroup1.copy {
              dataAvailabilityInterval =
                dataAvailabilityInterval.copy {
                  startTime = now.minus(93L, ChronoUnit.DAYS).toProtoTime()
                }
              details =
                details.copy {
                  metadata =
                    metadata.copy {
                      adMetadata =
                        adMetadata.copy {
                          campaignMetadata =
                            campaignMetadata.copy {
                              brandName = "Flod"
                              campaignName = "The most perfect cube of fat"
                            }
                        }
                    }
                }
            }
        }
      )

    return listOf(eventGroup1, eventGroup2, eventGroup3, eventGroup4)
  }

  @Test
  fun `deleteEventGroup throws NOT_FOUND for missing DataProvider`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val createdEventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
          }
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.deleteEventGroup(
          deleteEventGroupRequest {
            this.externalDataProviderId = 1L
            this.externalEventGroupId = createdEventGroup.externalEventGroupId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup")
  }

  @Test
  fun `deleteEventGroup transitions EventGroup to DELETED state`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val eventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          }
        }
      )

    val response: EventGroup =
      eventGroupsService.deleteEventGroup(
        deleteEventGroupRequest {
          this.externalDataProviderId = externalDataProviderId
          this.externalEventGroupId = eventGroup.externalEventGroupId
        }
      )

    assertThat(response)
      .isEqualTo(
        eventGroup.copy {
          this.updateTime = response.updateTime
          this.state = EventGroup.State.DELETED
          clearDetails()
        }
      )
    assertThat(response)
      .isEqualTo(
        eventGroupsService.getEventGroup(
          getEventGroupRequest {
            this.externalDataProviderId = externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
          }
        )
      )
  }

  @Test
  fun `deleteEventGroup fails for deleted EventGroup`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val createdEventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
          }
        }
      )

    val deletedEventGroup =
      eventGroupsService.deleteEventGroup(
        deleteEventGroupRequest {
          this.externalDataProviderId = externalDataProviderId
          this.externalEventGroupId = createdEventGroup.externalEventGroupId
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.deleteEventGroup(
          deleteEventGroupRequest {
            this.externalDataProviderId = deletedEventGroup.externalDataProviderId
            this.externalEventGroupId = deletedEventGroup.externalEventGroupId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup state is DELETED")
  }

  @Test
  fun `updateEventGroup fails for deleted EventGroup`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId

    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val createdEventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
          }
        }
      )

    val deletedEventGroup =
      eventGroupsService.deleteEventGroup(
        deleteEventGroupRequest {
          this.externalDataProviderId = externalDataProviderId
          this.externalEventGroupId = createdEventGroup.externalEventGroupId
        }
      )

    val modifyEventGroup =
      deletedEventGroup.copy {
        details = eventGroupDetails { encryptedMetadata = ByteString.copyFromUtf8("metadata") }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.updateEventGroup(
          updateEventGroupRequest { eventGroup = modifyEventGroup }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup state is DELETED")
  }

  @Test
  fun `getEventGroup succeeds for deleted EventGroup`() = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val eventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
          }
        }
      )
    val deletedEventGroup: EventGroup =
      eventGroupsService.deleteEventGroup(
        deleteEventGroupRequest {
          this.externalDataProviderId = externalDataProviderId
          this.externalEventGroupId = eventGroup.externalEventGroupId
        }
      )

    val response: EventGroup =
      eventGroupsService.getEventGroup(
        getEventGroupRequest {
          this.externalDataProviderId = externalDataProviderId
          this.externalEventGroupId = eventGroup.externalEventGroupId
        }
      )

    assertThat(response).isEqualTo(deletedEventGroup)
  }

  @Test
  fun `streamEventGroups succeeds for deleted EventGroups when is_deleted is true`(): Unit =
    runBlocking {
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId

      val eventGroup1 =
        eventGroupsService.createEventGroup(
          createEventGroupRequest {
            eventGroup = eventGroup {
              this.externalDataProviderId = externalDataProviderId
              this.externalMeasurementConsumerId =
                population
                  .createMeasurementConsumer(measurementConsumersService, accountsService)
                  .externalMeasurementConsumerId
              providedEventGroupId = "eventGroup1"
            }
          }
        )

      val eventGroup2 =
        eventGroupsService.createEventGroup(
          createEventGroupRequest {
            eventGroup = eventGroup {
              this.externalDataProviderId = externalDataProviderId
              this.externalMeasurementConsumerId =
                population
                  .createMeasurementConsumer(measurementConsumersService, accountsService)
                  .externalMeasurementConsumerId
              providedEventGroupId = "eventGroup2"
            }
          }
        )

      val deletedEventGroup1 =
        eventGroupsService.deleteEventGroup(
          deleteEventGroupRequest {
            this.externalDataProviderId = externalDataProviderId
            this.externalEventGroupId = eventGroup1.externalEventGroupId
          }
        )

      val deletedEventGroup2 =
        eventGroupsService.deleteEventGroup(
          deleteEventGroupRequest {
            this.externalDataProviderId = externalDataProviderId
            this.externalEventGroupId = eventGroup2.externalEventGroupId
          }
        )

      val eventGroups: List<EventGroup> =
        eventGroupsService
          .streamEventGroups(
            streamEventGroupsRequest {
              filter = filter {
                this.externalDataProviderId = externalDataProviderId
                this.showDeleted = true
              }
            }
          )
          .toList()

      assertThat(eventGroups).containsExactly(deletedEventGroup1, deletedEventGroup2)
    }

  @Test
  fun `streamEventGroups respects show_deleted is false`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup1 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId =
              population
                .createMeasurementConsumer(measurementConsumersService, accountsService)
                .externalMeasurementConsumerId
            providedEventGroupId = "eventGroup1"
          }
        }
      )

    val eventGroup2 =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId =
              population
                .createMeasurementConsumer(measurementConsumersService, accountsService)
                .externalMeasurementConsumerId
            providedEventGroupId = "eventGroup2"
          }
        }
      )

    eventGroupsService.deleteEventGroup(
      deleteEventGroupRequest {
        this.externalDataProviderId = externalDataProviderId
        this.externalEventGroupId = eventGroup1.externalEventGroupId
      }
    )

    eventGroupsService.deleteEventGroup(
      deleteEventGroupRequest {
        this.externalDataProviderId = externalDataProviderId
        this.externalEventGroupId = eventGroup2.externalEventGroupId
      }
    )

    val eventGroups: List<EventGroup> =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              this.externalDataProviderId = externalDataProviderId
              this.showDeleted = false
            }
          }
        )
        .toList()

    assertThat(eventGroups).isEmpty()
  }
}

data class EventGroupAndHelperServices<T : EventGroupsCoroutineImplBase>(
  val eventGroupsService: T,
  val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
  val dataProvidersService: DataProvidersCoroutineImplBase,
  val accountsService: AccountsCoroutineImplBase,
)
