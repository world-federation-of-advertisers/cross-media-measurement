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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.date
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupActivity
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.eventGroupMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupActivity
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.updateEventGroupActivityRequest

private const val RANDOM_SEED = 1
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
abstract class EventGroupActivitiesServiceTest<T : EventGroupActivitiesCoroutineImplBase> {

  private val testClock: Clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(testClock, Random(RANDOM_SEED))
  private val population = Population(testClock, idGenerator)

  data class Services<T>(
    val accountsService: AccountsCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val eventGroupActivitiesService: T,
    val eventGroupsService: EventGroupsCoroutineImplBase,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
  )

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  private lateinit var accountsService: AccountsCoroutineImplBase

  private lateinit var dataProvidersService: DataProvidersCoroutineImplBase

  private lateinit var eventGroupActivitiesService: T

  private lateinit var eventGroupsService: EventGroupsCoroutineImplBase

  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase

  private lateinit var dataProvider: DataProvider

  private lateinit var eventGroup: EventGroup

  @Before
  fun init() = runBlocking {
    val services = newServices(idGenerator)
    accountsService = services.accountsService
    dataProvidersService = services.dataProvidersService
    eventGroupActivitiesService = services.eventGroupActivitiesService
    eventGroupsService = services.eventGroupsService
    measurementConsumersService = services.measurementConsumersService

    dataProvider = population.createDataProvider(dataProvidersService)
    eventGroup = createEventGroup(dataProvider)
  }

  @Test
  fun `batchUpdateEventGroupActivities returns created EventGroupActivities when allow_missing is set to true`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }

        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 10
            }
          }
        }
      }

      val startTime = Instant.now()

      val activities =
        eventGroupActivitiesService
          .batchUpdateEventGroupActivities(request)
          .eventGroupActivitiesList

      assertThat(activities[0])
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(
          eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        )

      assertThat(activities[0].createTime.toInstant()).isGreaterThan(startTime)

      assertThat(activities[1])
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(
          eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 10
            }
          }
        )

      assertThat(activities[1].createTime.toInstant()).isGreaterThan(startTime)
    }

  @Test
  fun `batchUpdateEventGroupActivities returns updated existing EventGroupActivities`() =
    runBlocking {
      val createRequest = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }

        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 10
            }
          }
        }
      }

      val created =
        eventGroupActivitiesService
          .batchUpdateEventGroupActivities(createRequest)
          .eventGroupActivitiesList

      // TODO(@roaminggypsy): Update more fields once fields are added to EventGroupActivity
      val updateRequest = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }

        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 10
            }
          }
        }
      }

      val updated =
        eventGroupActivitiesService
          .batchUpdateEventGroupActivities(updateRequest)
          .eventGroupActivitiesList

      assertThat(updated[0])
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(created[0])

      assertThat(updated[0].createTime).isEqualTo(created[0].createTime)

      assertThat(updated[1])
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(created[1])

      assertThat(updated[1].createTime).isEqualTo(created[1].createTime)
    }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when parent external data provider id is not set`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        // missing external data provider id
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchUpdateEventGroupActivities(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_data_provider_id")
    }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when parent external event group id is not set`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        // missing parent external event group id
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchUpdateEventGroupActivities(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_event_group_id")
    }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when parent and child has different external event group id`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            // different external event group id
            externalEventGroupId = 1L
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchUpdateEventGroupActivities(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("different")
    }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when activity date is not set`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            // date not set
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchUpdateEventGroupActivities(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("date")
    }

  @Test
  fun `batchUpdateEventGroupActivities throws NOT_FOUND for non existent DataProvider`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        // non-existent DataProvider
        externalDataProviderId = 1L
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchUpdateEventGroupActivities(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("DataProvider not found")
    }

  @Test
  fun `batchUpdateEventGroupActivities throws NOT_FOUND for non existent EventGroup`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = 1L
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = 1L
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchUpdateEventGroupActivities(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("EventGroup not found")
    }

  private suspend fun createEventGroup(dataProvider: DataProvider): EventGroup {
    val measurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    return eventGroupsService.createEventGroup(
      createEventGroupRequest {
        this.eventGroup = eventGroup {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          providedEventGroupId = PROVIDED_EVENT_GROUP_ID
          mediaTypes += MediaType.VIDEO
          dataAvailabilityInterval = interval { startTime = testClock.instant().toProtoTime() }
          details = DETAILS
        }
      }
    )
  }
}
