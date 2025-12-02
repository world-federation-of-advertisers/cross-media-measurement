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
import com.google.protobuf.util.Timestamps
import com.google.type.copy
import com.google.type.endTimeOrNull
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.single
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
import org.wfanet.measurement.internal.kingdom.BatchCreateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.BatchUpdateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.CreateEventGroupRequest
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
import org.wfanet.measurement.internal.kingdom.batchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupsResponse
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
private const val PROVIDED_EVENT_GROUP_ID_2 = "ProvidedEventGroupId2"
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
  fun `batchCreateEventGroups returns created EventGroups`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    val measurementConsumer2 =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val externalMeasurementConsumerId2 = measurementConsumer2.externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val request = batchCreateEventGroupsRequest {
      this.externalDataProviderId = externalDataProviderId
      requests += createEventGroupRequest {
        eventGroup = eventGroup {
          this.externalDataProviderId = externalDataProviderId
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          providedEventGroupId = PROVIDED_EVENT_GROUP_ID
          mediaTypes += MediaType.VIDEO
          details = DETAILS
        }
      }
      requests += createEventGroupRequest {
        eventGroup = eventGroup {
          this.externalDataProviderId = externalDataProviderId
          this.externalMeasurementConsumerId = externalMeasurementConsumerId2
          providedEventGroupId = PROVIDED_EVENT_GROUP_ID_2
          mediaTypes += MediaType.DISPLAY
          details = DETAILS
        }
      }
    }

    val response: BatchCreateEventGroupsResponse =
      eventGroupsService.batchCreateEventGroups(request)

    assertThat(response.eventGroupsList)
      .ignoringFields(
        EventGroup.EXTERNAL_EVENT_GROUP_ID_FIELD_NUMBER,
        EventGroup.CREATE_TIME_FIELD_NUMBER,
        EventGroup.UPDATE_TIME_FIELD_NUMBER,
      )
      .containsExactly(
        request.requestsList[0].eventGroup.copy { this.state = EventGroup.State.ACTIVE },
        request.requestsList[1].eventGroup.copy { this.state = EventGroup.State.ACTIVE },
      )

    for (eventGroup in response.eventGroupsList) {
      assertThat(eventGroup.externalEventGroupId).isNotEqualTo(0)
      assertThat(eventGroup.createTime.seconds).isGreaterThan(0)
      assertThat(eventGroup.updateTime).isEqualTo(eventGroup.createTime)
      assertThat(eventGroup)
        .isEqualTo(
          eventGroupsService.getEventGroup(
            getEventGroupRequest {
              this.externalDataProviderId = externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
            }
          )
        )
    }
  }

  @Test
  fun `batchCreateEventGroups throws INVALID_ARGUMENT when parent external data provider id is not set`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId

      val request = batchCreateEventGroupsRequest {
        requests += createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            providedEventGroupId = PROVIDED_EVENT_GROUP_ID
            mediaTypes += MediaType.VIDEO
            details = DETAILS
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupsService.batchCreateEventGroups(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_data_provider_id")
    }

  @Test
  fun `batchCreateEventGroups throws INVALID_ARGUMENT when parent and child has different external data provider id`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId
      val externalDataProviderId2 =
        population.createDataProvider(dataProvidersService).externalDataProviderId

      val request = batchCreateEventGroupsRequest {
        this.externalDataProviderId = externalDataProviderId2
        requests += createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            providedEventGroupId = PROVIDED_EVENT_GROUP_ID
            mediaTypes += MediaType.VIDEO
            details = DETAILS
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupsService.batchCreateEventGroups(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("differs from that of the parent request")
    }

  @Test
  fun `batchCreateEventGroups throws INVALID_ARGUMENT when child event group is not set`() =
    runBlocking {
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId

      val request = batchCreateEventGroupsRequest {
        this.externalDataProviderId = externalDataProviderId
        requests += CreateEventGroupRequest.getDefaultInstance()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupsService.batchCreateEventGroups(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("not set")
    }

  @Test
  fun `batchCreateEventGroups throws INVALID_ARGUMENT when child request request id is duplicate in the batch`() =
    runBlocking {
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId

      val request = batchCreateEventGroupsRequest {
        this.externalDataProviderId = externalDataProviderId
        requests += createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            providedEventGroupId = PROVIDED_EVENT_GROUP_ID
            mediaTypes += MediaType.VIDEO
            details = DETAILS
          }
          requestId = "duplicate-id"
        }
        requests += createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            providedEventGroupId = PROVIDED_EVENT_GROUP_ID_2
            mediaTypes += MediaType.DISPLAY
            details = DETAILS
          }
          requestId = "duplicate-id"
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupsService.batchCreateEventGroups(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("duplicate")
    }

  @Test
  fun `batchCreateEventGroups throws FAILED_PRECONDITION when measurement consumer is not found`() =
    runBlocking {
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId

      val request = batchCreateEventGroupsRequest {
        this.externalDataProviderId = externalDataProviderId
        requests += createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = 123L
            providedEventGroupId = PROVIDED_EVENT_GROUP_ID
            mediaTypes += MediaType.VIDEO
            details = DETAILS
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupsService.batchCreateEventGroups(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
    }

  @Test
  fun `batchCreateEventGroups throws NOT_FOUND when data provider is not found`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId

    val request = batchCreateEventGroupsRequest {
      this.externalDataProviderId = 123L
      requests += createEventGroupRequest {
        eventGroup = eventGroup {
          this.externalDataProviderId = 123L
          this.externalMeasurementConsumerId = externalMeasurementConsumerId
          providedEventGroupId = PROVIDED_EVENT_GROUP_ID
          mediaTypes += MediaType.VIDEO
          details = DETAILS
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { eventGroupsService.batchCreateEventGroups(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
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
  fun `batchUpdateEventGroup succeeds`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup1: EventGroup =
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

    val eventGroup2: EventGroup =
      eventGroupsService.createEventGroup(
        createEventGroupRequest {
          eventGroup = eventGroup {
            this.externalDataProviderId = externalDataProviderId
            this.externalMeasurementConsumerId = externalMeasurementConsumerId
            providedEventGroupId = "ProvidedEventGroupId2"
            mediaTypes += MediaType.VIDEO
            mediaTypes += MediaType.OTHER
          }
        }
      )

    val request = batchUpdateEventGroupsRequest {
      this.externalDataProviderId = externalDataProviderId
      requests += updateEventGroupRequest {
        eventGroup =
          eventGroup1.copy {
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
      requests += updateEventGroupRequest {
        eventGroup =
          eventGroup2.copy {
            details = eventGroupDetails { encryptedMetadata = ByteString.copyFromUtf8("metadata2") }
            dataAvailabilityInterval = interval {
              startTime = now.minus(100L, ChronoUnit.DAYS).toProtoTime()
              endTime = now.minus(4L, ChronoUnit.DAYS).toProtoTime()
            }
            mediaTypes.clear()
            mediaTypes += MediaType.VIDEO
            mediaTypes += MediaType.OTHER
          }
      }
    }

    val response: BatchUpdateEventGroupsResponse = eventGroupsService.batchUpdateEventGroup(request)

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        batchUpdateEventGroupsResponse {
          eventGroups +=
            request.requestsList.first().eventGroup.copy {
              clearUpdateTime()
              mediaTypes.clear()
              mediaTypes += MediaType.DISPLAY
              mediaTypes += MediaType.OTHER
            }
          eventGroups +=
            request.requestsList.last().eventGroup.copy {
              clearUpdateTime()
              mediaTypes.clear()
              mediaTypes += MediaType.VIDEO
              mediaTypes += MediaType.OTHER
            }
        }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        batchUpdateEventGroupsResponse {
          eventGroups +=
            eventGroupsService
              .getEventGroup(
                getEventGroupRequest {
                  this.externalDataProviderId = eventGroup1.externalDataProviderId
                  externalEventGroupId = eventGroup1.externalEventGroupId
                }
              )
              .copy {
                mediaTypes.clear()
                mediaTypes += MediaType.DISPLAY
                mediaTypes += MediaType.OTHER
              }

          eventGroups +=
            eventGroupsService
              .getEventGroup(
                getEventGroupRequest {
                  this.externalDataProviderId = eventGroup2.externalDataProviderId
                  externalEventGroupId = eventGroup2.externalEventGroupId
                }
              )
              .copy {
                mediaTypes.clear()
                mediaTypes += MediaType.VIDEO
                mediaTypes += MediaType.OTHER
              }
        }
      )
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for missing parent external data provider id`():
    Unit = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup1: EventGroup =
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            requests += updateEventGroupRequest {
              eventGroup = eventGroup1.copy { mediaTypes += MediaType.DISPLAY }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("external_data_provider_id")
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for missing child external data provider id`():
    Unit = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup1: EventGroup =
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            this.externalDataProviderId = externalDataProviderId
            requests += updateEventGroupRequest {
              eventGroup = eventGroup1.copy { clearExternalDataProviderId() }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .hasMessageThat()
      .contains("requests.0.event_group.external_data_provider_id")
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for different parent and child external data provider id`():
    Unit = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup1: EventGroup =
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            this.externalDataProviderId = 1
            requests += updateEventGroupRequest { eventGroup = eventGroup1 }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("different")
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for missing external event group id`(): Unit =
    runBlocking {
      val externalMeasurementConsumerId =
        population
          .createMeasurementConsumer(measurementConsumersService, accountsService)
          .externalMeasurementConsumerId
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId

      val eventGroup1: EventGroup =
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

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupsService.batchUpdateEventGroup(
            batchUpdateEventGroupsRequest {
              this.externalDataProviderId = externalDataProviderId
              requests += updateEventGroupRequest {
                eventGroup = eventGroup1.copy { clearExternalEventGroupId() }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("requests.0.event_group.external_event_group_id")
    }

  @Test
  fun `batchUpdateEventGroup throws NOT_FOUND for non-existent event group`(): Unit = runBlocking {
    val externalMeasurementConsumerId =
      population
        .createMeasurementConsumer(measurementConsumersService, accountsService)
        .externalMeasurementConsumerId
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroup = eventGroup {
      this.externalDataProviderId = externalDataProviderId
      this.externalMeasurementConsumerId = externalMeasurementConsumerId
      externalEventGroupId = 123L
      mediaTypes += MediaType.VIDEO
      mediaTypes += MediaType.OTHER
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            this.externalDataProviderId = externalDataProviderId
            requests += updateEventGroupRequest {
              this.eventGroup = eventGroup.copy { mediaTypes += MediaType.DISPLAY }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup not found")
  }

  @Test
  fun `batchUpdateEventGroup throws NOT_FOUND for deleted event group`(): Unit = runBlocking {
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

    eventGroupsService.deleteEventGroup(
      deleteEventGroupRequest {
        this.externalDataProviderId = eventGroup.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupsService.batchUpdateEventGroup(
          batchUpdateEventGroupsRequest {
            this.externalDataProviderId = externalDataProviderId
            requests += updateEventGroupRequest {
              this.eventGroup = eventGroup.copy { mediaTypes += MediaType.DISPLAY }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup state is DELETED")
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for invalid modification params`(): Unit =
    runBlocking {
      val externalMeasurementConsumerId =
        population
          .createMeasurementConsumer(measurementConsumersService, accountsService)
          .externalMeasurementConsumerId
      val externalMeasurementConsumerId2 =
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

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupsService.batchUpdateEventGroup(
            batchUpdateEventGroupsRequest {
              this.externalDataProviderId = externalDataProviderId
              requests += updateEventGroupRequest {
                this.eventGroup =
                  eventGroup.copy {
                    mediaTypes += MediaType.DISPLAY
                    this.externalMeasurementConsumerId = externalMeasurementConsumerId2
                  }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("invalid arguments")
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
  fun `streamEventGroups respects data availability interval contains filter`(): Unit =
    runBlocking {
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
                dataAvailabilityStartTimeOnOrBefore = filterRange.start.toProtoTime()
                dataAvailabilityEndTimeOnOrAfter = filterRange.endInclusive.toProtoTime()
              }
            }
          )
          .toList()

      assertThat(response)
        .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
        .containsExactlyElementsIn(
          eventGroups.filter {
            val availabilityRange =
              it.dataAvailabilityInterval.startTime.toInstant()..<(it.dataAvailabilityInterval
                    .endTimeOrNull ?: Timestamps.MAX_VALUE)
                  .toInstant()

            filterRange in availabilityRange
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

  @Test
  fun `streamEventGroups returns subsequent page with orderBy`(): Unit = runBlocking {
    val now: Instant = testClock.instant()
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider1: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2: DataProvider = population.createDataProvider(dataProvidersService)
    val eventGroups: List<EventGroup> =
      populateTestEventGroups(now, measurementConsumer, dataProvider1, dataProvider2)
    val orderBy = orderBy {
      field = StreamEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
      descending = true
    }
    val endOfPage: EventGroup =
      eventGroupsService
        .streamEventGroups(
          streamEventGroupsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
            this.orderBy = orderBy
            limit = 1
          }
        )
        .single()
    val request = streamEventGroupsRequest {
      filter = filter {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        after =
          StreamEventGroupsRequestKt.FilterKt.after {
            dataAvailabilityStartTime = endOfPage.dataAvailabilityInterval.startTime
            eventGroupKey = eventGroupKey {
              externalDataProviderId = endOfPage.externalDataProviderId
              externalEventGroupId = endOfPage.externalEventGroupId
            }
          }
      }
      this.orderBy = orderBy
    }

    val response: List<EventGroup> = eventGroupsService.streamEventGroups(request).toList()

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFields(EventGroup.MEDIA_TYPES_FIELD_NUMBER)
      .containsExactlyElementsIn(
        eventGroups
          .filter {
            !(it.externalDataProviderId == endOfPage.externalDataProviderId &&
              it.externalEventGroupId == endOfPage.externalEventGroupId)
          }
          .sortedWith(
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

private operator fun OpenEndRange<Instant>.contains(other: ClosedRange<Instant>): Boolean {
  return start <= other.start && endExclusive > other.endInclusive
}
