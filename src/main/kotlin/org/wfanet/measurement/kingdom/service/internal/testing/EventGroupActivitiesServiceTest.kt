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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import com.google.rpc.errorInfo
import com.google.type.Date
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
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupActivity
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt.eventGroupMetadata
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ListEventGroupActivitiesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MediaType
import org.wfanet.measurement.internal.kingdom.batchDeleteEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest
import org.wfanet.measurement.internal.kingdom.dateInterval
import org.wfanet.measurement.internal.kingdom.deleteEventGroupActivityRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupActivity
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.listEventGroupActivitiesRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupActivityRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException

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
            externalDataProviderId = dataProvider.externalDataProviderId
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
            externalDataProviderId = dataProvider.externalDataProviderId
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
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when parent and child has different external data provider id`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            // different data provider id
            externalDataProviderId = 1L
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
      assertThat(exception)
        .hasMessageThat()
        .contains("requests.0.event_group_activity.external_data_provider_id")
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
      assertThat(exception)
        .hasMessageThat()
        .contains("requests.0.event_group_activity.external_event_group_id")
    }

  @Test
  fun `batchUpdateEventGroupActivities throws INVALID_ARGUMENT when activity is not set`() =
    runBlocking {
      val request = batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest { allowMissing = true }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchUpdateEventGroupActivities(request)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("requests.0.event_group_activity")
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
      assertThat(exception).hasMessageThat().contains("requests.0.event_group_activity.date")
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

  @Test
  fun `deleteEventGroupActivity succeeded`() = runBlocking {
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
    }

    val activity =
      eventGroupActivitiesService
        .batchUpdateEventGroupActivities(request)
        .eventGroupActivitiesList
        .single()

    val response =
      eventGroupActivitiesService.deleteEventGroupActivity(
        deleteEventGroupActivityRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          externalEventGroupActivityId = activity.date
        }
      )

    assertThat(response).isEqualTo(Empty.getDefaultInstance())
  }

  @Test
  fun `deleteEventGroupActivity throws NOT_FOUND for non existent DataProvider`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupActivitiesService.deleteEventGroupActivity(
          deleteEventGroupActivityRequest {
            externalDataProviderId = 1L
            externalEventGroupId = eventGroup.externalEventGroupId
            externalEventGroupActivityId = date {
              year = 2025
              month = 1
              day = 1
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `deleteEventGroupActivity throws NOT_FOUND for non existent EventGroup`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupActivitiesService.deleteEventGroupActivity(
          deleteEventGroupActivityRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = 1L
            externalEventGroupActivityId = date {
              year = 2025
              month = 1
              day = 1
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroup not found")
  }

  @Test
  fun `deleteEventGroupActivity throws NOT_FOUND when activity does not exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupActivitiesService.deleteEventGroupActivity(
          deleteEventGroupActivityRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
            externalEventGroupActivityId = date {
              year = 2025
              month = 1
              day = 1
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("EventGroupActivity not found")
  }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when external data provider id is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.deleteEventGroupActivity(
            deleteEventGroupActivityRequest {
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityId = date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_data_provider_id")
    }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when external event group id is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.deleteEventGroupActivity(
            deleteEventGroupActivityRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupActivityId = date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_event_group_id")
    }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when external event group activity id is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.deleteEventGroupActivity(
            deleteEventGroupActivityRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityId = Date.getDefaultInstance()
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_event_group_activity_id")
    }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when year is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupActivitiesService.deleteEventGroupActivity(
          deleteEventGroupActivityRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
            externalEventGroupActivityId = date {
              // year not set
              month = 1
              day = 1
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when month is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupActivitiesService.deleteEventGroupActivity(
          deleteEventGroupActivityRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
            externalEventGroupActivityId = date {
              year = 2025
              // month not set
              day = 1
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteEventGroupActivity throws INVALID_ARGUMENT when day is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupActivitiesService.deleteEventGroupActivity(
          deleteEventGroupActivityRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
            externalEventGroupActivityId = date {
              year = 2025
              month = 1
              // day not set
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchDeleteEventGroupActivities succeeded`() = runBlocking {
    val request = batchUpdateEventGroupActivitiesRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalEventGroupId = eventGroup.externalEventGroupId
      requests += updateEventGroupActivityRequest {
        allowMissing = true
        eventGroupActivity = eventGroupActivity {
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 1
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
            month = 1
            day = 2
          }
        }
      }
    }

    eventGroupActivitiesService.batchUpdateEventGroupActivities(request)

    val deleteRequest = batchDeleteEventGroupActivitiesRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalEventGroupId = eventGroup.externalEventGroupId
      externalEventGroupActivityIds += date {
        year = 2025
        month = 1
        day = 1
      }
      externalEventGroupActivityIds += date {
        year = 2025
        month = 1
        day = 2
      }
    }

    val response = eventGroupActivitiesService.batchDeleteEventGroupActivities(deleteRequest)

    assertThat(response).isEqualTo(Empty.getDefaultInstance())

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupActivitiesService.batchDeleteEventGroupActivities(deleteRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `batchDeleteEventGroupActivities throws NOT_FOUND for non existent DataProvider`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = 1L
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception).hasMessageThat().contains("DataProvider not found")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws NOT_FOUND for non existent EventGroup`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = 1L
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception).hasMessageThat().contains("EventGroup not found")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws NOT_FOUND when activity does not exist`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception).hasMessageThat().contains("EventGroupActivity not found")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when external data provider id is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_data_provider_id")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when external event group id is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_event_group_id")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when external event group activity ids is empty`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_event_group_activity_ids")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when an activity id is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
              externalEventGroupActivityIds += Date.getDefaultInstance()
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("external_event_group_activity_ids.1")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when duplicate dates are provided`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("duplicate")
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when year is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                // year not set
                month = 1
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when month is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                year = 2025
                // month not set
                day = 1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchDeleteEventGroupActivities throws INVALID_ARGUMENT when day is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.batchDeleteEventGroupActivities(
            batchDeleteEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eventGroup.externalEventGroupId
              externalEventGroupActivityIds += date {
                year = 2025
                month = 1
                // day not set
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listEventGroupActivities returns activities for event group`() = runBlocking {
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

    eventGroupActivitiesService.batchUpdateEventGroupActivities(createRequest)

    val response =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
        }
      )

    assertThat(response.eventGroupActivitiesList)
      .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
      .containsExactly(
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 1
          }
        },
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 10
          }
        },
      )
      .inOrder()
  }

  @Test
  fun `listEventGroupActivities paginates with page_token`() = runBlocking {
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
            day = 5
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

    eventGroupActivitiesService.batchUpdateEventGroupActivities(createRequest)

    val firstResponse =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          pageSize = 1
        }
      )

    assertThat(firstResponse.eventGroupActivitiesList).hasSize(1)
    assertThat(firstResponse.hasNextPageToken()).isTrue()

    val secondResponse =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse.eventGroupActivitiesList)
      .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
      .containsExactly(
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 5
          }
        },
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 10
          }
        },
      )
      .inOrder()
    assertThat(secondResponse.eventGroupActivitiesList.map { it.date })
      .containsNoneIn(firstResponse.eventGroupActivitiesList.map { it.date })
  }

  @Test
  fun `listEventGroupActivities returns empty when no activities exist`() = runBlocking {
    val response =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
        }
      )

    assertThat(response.eventGroupActivitiesList).isEmpty()
    assertThat(response.hasNextPageToken()).isFalse()
  }

  @Test
  fun `listEventGroupActivities returns activities for MeasurementConsumer-rooted event group`() =
    runBlocking {
      eventGroupActivitiesService.batchUpdateEventGroupActivities(
        batchUpdateEventGroupActivitiesRequest {
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
        }
      )

      val response =
        eventGroupActivitiesService.listEventGroupActivities(
          listEventGroupActivitiesRequest {
            externalMeasurementConsumerId = eventGroup.externalMeasurementConsumerId
            externalEventGroupId = eventGroup.externalEventGroupId
          }
        )

      assertThat(response.eventGroupActivitiesList)
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .containsExactly(
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        )
        .inOrder()
    }

  @Test
  fun `listEventGroupActivities returns activities across event groups for MeasurementConsumer`() {
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(measurementConsumersService, accountsService)
      val eventGroupA =
        createEventGroup(dataProvider, measurementConsumer, "provided-event-group-a")
      val eventGroupB =
        createEventGroup(dataProvider, measurementConsumer, "provided-event-group-b")
      // EventGroup under a different MeasurementConsumer; its activity must be excluded.
      val otherEventGroup = createEventGroup(dataProvider)

      val activityDate = date {
        year = 2025
        month = 12
        day = 5
      }
      for (eventGroupUnderTest in listOf(eventGroupA, eventGroupB, otherEventGroup)) {
        eventGroupActivitiesService.batchUpdateEventGroupActivities(
          batchUpdateEventGroupActivitiesRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroupUnderTest.externalEventGroupId
            requests += updateEventGroupActivityRequest {
              allowMissing = true
              eventGroupActivity = eventGroupActivity {
                externalEventGroupId = eventGroupUnderTest.externalEventGroupId
                date = activityDate
              }
            }
          }
        )
      }

      val response =
        eventGroupActivitiesService.listEventGroupActivities(
          listEventGroupActivitiesRequest {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          }
        )

      assertThat(response.eventGroupActivitiesList)
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .containsExactly(
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroupA.externalEventGroupId
            date = activityDate
          },
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroupB.externalEventGroupId
            date = activityDate
          },
        )
    }
  }

  @Test
  fun `listEventGroupActivities throws NOT_FOUND for non existent EventGroup under MeasurementConsumer`() {
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.listEventGroupActivities(
            listEventGroupActivitiesRequest {
              externalMeasurementConsumerId = eventGroup.externalMeasurementConsumerId
              externalEventGroupId = 999999L
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.EVENT_GROUP_NOT_FOUND.name
            metadata["external_measurement_consumer_id"] =
              eventGroup.externalMeasurementConsumerId.toString()
            metadata["external_event_group_id"] = "999999"
          }
        )
    }
  }

  @Test
  fun `listEventGroupActivities throws NOT_FOUND for non existent MeasurementConsumer`() {
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.listEventGroupActivities(
            listEventGroupActivitiesRequest { externalMeasurementConsumerId = 999999L }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND.name
            metadata["external_measurement_consumer_id"] = "999999"
          }
        )
    }
  }

  @Test
  fun `listEventGroupActivities throws INVALID_ARGUMENT when scope is not set`() {
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.listEventGroupActivities(listEventGroupActivitiesRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.REQUIRED_FIELD_NOT_SET.name
            metadata["field_name"] = "scope"
          }
        )
    }
  }

  @Test
  fun `listEventGroupActivities respects page_size`() = runBlocking {
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
            day = 5
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

    eventGroupActivitiesService.batchUpdateEventGroupActivities(createRequest)

    val response =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          pageSize = 1
        }
      )

    assertThat(response.eventGroupActivitiesList)
      .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
      .containsExactly(
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 1
          }
        }
      )
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `listEventGroupActivities paginates with page_token and page_size`() = runBlocking {
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
            day = 5
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

    eventGroupActivitiesService.batchUpdateEventGroupActivities(createRequest)

    val firstResponse =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          pageSize = 1
        }
      )

    assertThat(firstResponse.eventGroupActivitiesList).hasSize(1)
    assertThat(firstResponse.hasNextPageToken()).isTrue()

    val secondResponse =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse.eventGroupActivitiesList)
      .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
      .containsExactly(
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 5
          }
        }
      )
    assertThat(secondResponse.hasNextPageToken()).isTrue()
    assertThat(secondResponse.eventGroupActivitiesList.map { it.date })
      .containsNoneIn(firstResponse.eventGroupActivitiesList.map { it.date })
  }

  @Test
  fun `listEventGroupActivities coerces page_size above MAX_PAGE_SIZE`() = runBlocking {
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
    }

    eventGroupActivitiesService.batchUpdateEventGroupActivities(createRequest)

    val response =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          pageSize = 5000
        }
      )

    assertThat(response.eventGroupActivitiesList).hasSize(1)
  }

  @Test
  fun `listEventGroupActivities throws NOT_FOUND for non existent DataProvider`() {
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.listEventGroupActivities(
            listEventGroupActivitiesRequest { externalDataProviderId = 999999L }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.DATA_PROVIDER_NOT_FOUND.name
            metadata["external_data_provider_id"] = "999999"
          }
        )
    }
  }

  @Test
  fun `listEventGroupActivities returns activities across multiple event groups with empty filter`() =
    runBlocking {
      val eventGroup2 = createEventGroup(dataProvider)

      // Create activities on event group 1
      eventGroupActivitiesService.batchUpdateEventGroupActivities(
        batchUpdateEventGroupActivitiesRequest {
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
        }
      )

      // Create activities on event group 2
      eventGroupActivitiesService.batchUpdateEventGroupActivities(
        batchUpdateEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup2.externalEventGroupId
          requests += updateEventGroupActivityRequest {
            allowMissing = true
            eventGroupActivity = eventGroupActivity {
              externalEventGroupId = eventGroup2.externalEventGroupId
              date = date {
                year = 2025
                month = 12
                day = 1
              }
            }
          }
        }
      )

      // List with no event group filter — should return all
      val response =
        eventGroupActivitiesService.listEventGroupActivities(
          listEventGroupActivitiesRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
          }
        )

      val expectedActivities =
        listOf(eventGroup, eventGroup2)
          .sortedBy { it.externalEventGroupId }
          .map { eg ->
            eventGroupActivity {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = eg.externalEventGroupId
              date = date {
                year = 2025
                month = 12
                day = 1
              }
            }
          }

      assertThat(response.eventGroupActivitiesList)
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .containsExactlyElementsIn(expectedActivities)
        .inOrder()
    }

  @Test
  fun `listEventGroupActivities filters to subset of event groups`() {
    runBlocking {
      val eventGroup2 = createEventGroup(dataProvider)

      // Create activities on event group 1
      eventGroupActivitiesService.batchUpdateEventGroupActivities(
        batchUpdateEventGroupActivitiesRequest {
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
        }
      )

      // Create activities on event group 2
      eventGroupActivitiesService.batchUpdateEventGroupActivities(
        batchUpdateEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup2.externalEventGroupId
          requests += updateEventGroupActivityRequest {
            allowMissing = true
            eventGroupActivity = eventGroupActivity {
              externalEventGroupId = eventGroup2.externalEventGroupId
              date = date {
                year = 2025
                month = 12
                day = 5
              }
            }
          }
        }
      )

      // Filter to only event group 1
      val response =
        eventGroupActivitiesService.listEventGroupActivities(
          listEventGroupActivitiesRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
          }
        )

      assertThat(response.eventGroupActivitiesList)
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .containsExactly(
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          }
        )
    }
  }

  @Test
  fun `listEventGroupActivities filters by date_interval`() = runBlocking {
    eventGroupActivitiesService.batchUpdateEventGroupActivities(
      batchUpdateEventGroupActivitiesRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        externalEventGroupId = eventGroup.externalEventGroupId
        requests += updateEventGroupActivityRequest {
          allowMissing = true
          eventGroupActivity = eventGroupActivity {
            externalEventGroupId = eventGroup.externalEventGroupId
            date = date {
              year = 2025
              month = 11
              day = 15
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
              day = 15
            }
          }
        }
      }
    )

    val response =
      eventGroupActivitiesService.listEventGroupActivities(
        listEventGroupActivitiesRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          filter = filter {
            dateInterval = dateInterval {
              startDate = date {
                year = 2025
                month = 12
                day = 1
              }
              endDate = date {
                year = 2026
                month = 1
                day = 1
              }
            }
          }
        }
      )

    assertThat(response.eventGroupActivitiesList)
      .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
      .containsExactly(
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 1
          }
        },
        eventGroupActivity {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalEventGroupId = eventGroup.externalEventGroupId
          date = date {
            year = 2025
            month = 12
            day = 15
          }
        },
      )
      .inOrder()
  }

  @Test
  fun `listEventGroupActivities throws NOT_FOUND for non existent EventGroup`() {
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupActivitiesService.listEventGroupActivities(
            listEventGroupActivitiesRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              externalEventGroupId = 999999L
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = KingdomInternalException.DOMAIN
            reason = ErrorCode.EVENT_GROUP_NOT_FOUND.name
            metadata["external_data_provider_id"] = dataProvider.externalDataProviderId.toString()
            metadata["external_event_group_id"] = "999999"
          }
        )
    }
  }

  @Test
  fun `listEventGroupActivities paginates across multiple event groups`() {
    runBlocking {
      val eventGroup2 = createEventGroup(dataProvider)

      // Determine sorted order by externalEventGroupId.
      val sortedEgs = listOf(eventGroup, eventGroup2).sortedBy { it.externalEventGroupId }

      // Create activities on both EGs on 2025-12-01 and 2025-12-05.
      for (eg in listOf(eventGroup, eventGroup2)) {
        eventGroupActivitiesService.batchUpdateEventGroupActivities(
          batchUpdateEventGroupActivitiesRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = eg.externalEventGroupId
            requests += updateEventGroupActivityRequest {
              allowMissing = true
              eventGroupActivity = eventGroupActivity {
                externalEventGroupId = eg.externalEventGroupId
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
                externalEventGroupId = eg.externalEventGroupId
                date = date {
                  year = 2025
                  month = 12
                  day = 5
                }
              }
            }
          }
        )
      }

      // List with no externalEventGroupId filter, pageSize = 2.
      val firstResponse =
        eventGroupActivitiesService.listEventGroupActivities(
          listEventGroupActivitiesRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            pageSize = 2
          }
        )

      assertThat(firstResponse.eventGroupActivitiesList).hasSize(2)
      assertThat(firstResponse.hasNextPageToken()).isTrue()
      // Page 1: both activities on 2025-12-01, ordered by externalEventGroupId ASC.
      assertThat(firstResponse.eventGroupActivitiesList)
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .containsExactly(
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = sortedEgs[0].externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          },
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = sortedEgs[1].externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 1
            }
          },
        )
        .inOrder()

      // Page 2.
      val secondResponse =
        eventGroupActivitiesService.listEventGroupActivities(
          listEventGroupActivitiesRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            pageSize = 2
            pageToken = firstResponse.nextPageToken
          }
        )

      assertThat(secondResponse.eventGroupActivitiesList).hasSize(2)
      // Page 2: both activities on 2025-12-05.
      assertThat(secondResponse.eventGroupActivitiesList)
        .ignoringFields(EventGroupActivity.CREATE_TIME_FIELD_NUMBER)
        .containsExactly(
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = sortedEgs[0].externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 5
            }
          },
          eventGroupActivity {
            externalDataProviderId = dataProvider.externalDataProviderId
            externalEventGroupId = sortedEgs[1].externalEventGroupId
            date = date {
              year = 2025
              month = 12
              day = 5
            }
          },
        )
        .inOrder()

      // No row repeated across pages.
      assertThat(secondResponse.eventGroupActivitiesList)
        .containsNoneIn(firstResponse.eventGroupActivitiesList)
    }
  }

  private suspend fun createEventGroup(
    dataProvider: DataProvider,
    measurementConsumer: MeasurementConsumer? = null,
    providedEventGroupId: String = PROVIDED_EVENT_GROUP_ID,
  ): EventGroup {
    val resolvedMeasurementConsumer =
      measurementConsumer
        ?: population.createMeasurementConsumer(measurementConsumersService, accountsService)
    return eventGroupsService.createEventGroup(
      createEventGroupRequest {
        this.eventGroup = eventGroup {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementConsumerId = resolvedMeasurementConsumer.externalMeasurementConsumerId
          this.providedEventGroupId = providedEventGroupId
          mediaTypes += MediaType.VIDEO
          dataAvailabilityInterval = interval { startTime = testClock.instant().toProtoTime() }
          details = DETAILS
        }
      }
    )
  }
}
