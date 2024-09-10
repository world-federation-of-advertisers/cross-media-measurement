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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
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
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage2
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamEventGroupMetadataDescriptorsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptorDetails
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptorKey
import org.wfanet.measurement.internal.kingdom.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private val DETAILS = eventGroupMetadataDescriptorDetails {
  apiVersion = Version.V2_ALPHA.string
  descriptorSet = ProtoReflection.buildFileDescriptorSet(TestMetadataMessage.getDescriptor())
}
private val DETAILS_2 = eventGroupMetadataDescriptorDetails {
  apiVersion = Version.V2_ALPHA.string
  descriptorSet = ProtoReflection.buildFileDescriptorSet(TestMetadataMessage2.getDescriptor())
}

@RunWith(JUnit4::class)
abstract class EventGroupMetadataDescriptorsServiceTest<
  T : EventGroupMetadataDescriptorsCoroutineImplBase
> {

  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  private val testClock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(testClock, Random(RANDOM_SEED))
  private val population = Population(testClock, idGenerator)

  private lateinit var eventGroupMetadataDescriptorService: T

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(
    idGenerator: IdGenerator
  ): EventGroupMetadataDescriptorsAndHelperServices<T>

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    eventGroupMetadataDescriptorService = services.eventGroupMetadataDescriptorService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `getEventGroupMetadataDescriptor succeeds`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      this.externalDataProviderId = externalDataProviderId
      details = DETAILS
    }

    val createdDescriptor =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor
      )

    val eventGroupMetadataDescriptorRead =
      eventGroupMetadataDescriptorService.getEventGroupMetadataDescriptor(
        getEventGroupMetadataDescriptorRequest {
          this.externalDataProviderId = externalDataProviderId
          externalEventGroupMetadataDescriptorId =
            createdDescriptor.externalEventGroupMetadataDescriptorId
        }
      )

    assertThat(eventGroupMetadataDescriptorRead).isEqualTo(createdDescriptor)
  }

  @Test
  fun `getEventGroupMetadataDescriptor fails for missing EventGroupMetadataDescriptor`() =
    runBlocking {
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupMetadataDescriptorService.getEventGroupMetadataDescriptor(
            getEventGroupMetadataDescriptorRequest {
              this.externalDataProviderId = externalDataProviderId
              externalEventGroupMetadataDescriptorId = 1L
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception)
        .hasMessageThat()
        .contains("NOT_FOUND: EventGroupMetadataDescriptor not found")
    }

  @Test
  fun `createEventGroupMetadataDescriptor succeeds`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      this.externalDataProviderId = externalDataProviderId
      details = DETAILS
    }

    val createdEventGroupMetadataDescriptor =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor
      )

    assertThat(createdEventGroupMetadataDescriptor)
      .ignoringFields(
        EventGroupMetadataDescriptor.EXTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR_ID_FIELD_NUMBER
      )
      .isEqualTo(eventGroupMetadataDescriptor)
    assertThat(createdEventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId)
      .isGreaterThan(0)
  }

  @Test
  fun `createEventGroupMetadataDescriptor fails for missing data provider`() = runBlocking {
    val eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      this.externalDataProviderId = 1L
      details = DETAILS
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
          eventGroupMetadataDescriptor
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: DataProvider not found")
  }

  @Test
  fun `createEventGroupMetadataDescriptor fails for duplicate message type`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val externalDataProviderId2 =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
      eventGroupMetadataDescriptor {
        this.externalDataProviderId = externalDataProviderId
        details = DETAILS
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
          eventGroupMetadataDescriptor {
            this.externalDataProviderId = externalDataProviderId2
            details = DETAILS
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception).hasMessageThat().contains("EventGroupMetadataDescriptor")
  }

  @Test
  fun `createEventGroupMetadataDescriptor returns existing Descriptor by idempotency key`() =
    runBlocking {
      val externalDataProviderId =
        population.createDataProvider(dataProvidersService).externalDataProviderId
      val idempotencyKey = "type.googleapis.com/example.TestMetadataMessage"
      val request = eventGroupMetadataDescriptor {
        this.externalDataProviderId = externalDataProviderId
        this.idempotencyKey = idempotencyKey
        this.details = DETAILS
      }
      val existingDescriptor: EventGroupMetadataDescriptor =
        eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(request)

      val createdDescriptor =
        eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(request)

      assertThat(createdDescriptor).isEqualTo(existingDescriptor)
    }

  @Test
  fun `updateEventGroupMetadataDescriptor fails for missing EventGroupMetadataDescriptor`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          eventGroupMetadataDescriptorService.updateEventGroupMetadataDescriptor(
            updateEventGroupMetadataDescriptorRequest {
              this.eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
                this.externalDataProviderId = 1L
                details = DETAILS
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .hasMessageThat()
        .contains("ExternalEventGroupMetadataDescriptorId unspecified")
    }

  @Test
  fun `updateEventGroupMetadataDescriptor succeeds`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      this.externalDataProviderId = externalDataProviderId
      details = DETAILS
    }

    val createdEventGroupMetadataDescriptor =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor
      )

    val modifyEventGroupMetadataDescriptor =
      createdEventGroupMetadataDescriptor.copy {
        details = eventGroupMetadataDescriptorDetails { apiVersion = "alternate version" }
      }

    val updatedEventGroupMetadataDescriptor =
      eventGroupMetadataDescriptorService.updateEventGroupMetadataDescriptor(
        updateEventGroupMetadataDescriptorRequest {
          this.eventGroupMetadataDescriptor = modifyEventGroupMetadataDescriptor
        }
      )

    assertThat(updatedEventGroupMetadataDescriptor)
      .isEqualTo(
        createdEventGroupMetadataDescriptor
          .toBuilder()
          .also { it.details = updatedEventGroupMetadataDescriptor.details }
          .build()
      )
  }

  @Test
  fun `updateEventGroupMetadataDescriptor fails for duplicate message type`() = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    val externalDataProviderId2 =
      population.createDataProvider(dataProvidersService).externalDataProviderId
    eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
      eventGroupMetadataDescriptor {
        this.externalDataProviderId = externalDataProviderId
        details = DETAILS
      }
    )
    val eventGroupMetadataDescriptor =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId2
          details = DETAILS_2
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupMetadataDescriptorService.updateEventGroupMetadataDescriptor(
          updateEventGroupMetadataDescriptorRequest {
            this.eventGroupMetadataDescriptor =
              eventGroupMetadataDescriptor.copy {
                // This should error as the message type is already in use by the other EDP.
                details = DETAILS
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception).hasMessageThat().contains("EventGroupMetadataDescriptor")
  }

  @Test
  fun `streamEventGroupMetadataDescriptors returns all descriptors in order`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroupMetadataDescriptor1 =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId
          details = DETAILS
        }
      )

    val eventGroupMetadataDescriptor2 =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId
          details = DETAILS_2
        }
      )

    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      eventGroupMetadataDescriptorService
        .streamEventGroupMetadataDescriptors(
          streamEventGroupMetadataDescriptorsRequest {
            filter = filter { this.externalDataProviderId = externalDataProviderId }
          }
        )
        .toList()

    if (
      eventGroupMetadataDescriptor1.externalEventGroupMetadataDescriptorId <
        eventGroupMetadataDescriptor2.externalEventGroupMetadataDescriptorId
    ) {
      assertThat(eventGroupMetadataDescriptors)
        .comparingExpectedFieldsOnly()
        .containsExactly(eventGroupMetadataDescriptor1, eventGroupMetadataDescriptor2)
        .inOrder()
    } else {
      assertThat(eventGroupMetadataDescriptors)
        .comparingExpectedFieldsOnly()
        .containsExactly(eventGroupMetadataDescriptor2, eventGroupMetadataDescriptor1)
        .inOrder()
    }
  }

  @Test
  fun `streamEventGroupMetadataDescriptors respects externalEventGroupMetadataDescriptorIds`():
    Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
      eventGroupMetadataDescriptor {
        this.externalDataProviderId = externalDataProviderId
        details = DETAILS
      }
    )

    val eventGroupMetadataDescriptor2 =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId
          details = DETAILS_2
        }
      )

    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      eventGroupMetadataDescriptorService
        .streamEventGroupMetadataDescriptors(
          streamEventGroupMetadataDescriptorsRequest {
            filter = filter {
              this.externalDataProviderId = externalDataProviderId
              this.externalEventGroupMetadataDescriptorIds +=
                eventGroupMetadataDescriptor2.externalEventGroupMetadataDescriptorId
            }
          }
        )
        .toList()

    assertThat(eventGroupMetadataDescriptors)
      .comparingExpectedFieldsOnly()
      .containsExactly(eventGroupMetadataDescriptor2)
  }

  @Test
  fun `streamEventGroupMetadataDescriptors respects limit`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroupMetadataDescriptor =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId
          details = DETAILS
        }
      )

    val eventGroupMetadataDescriptor2 =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId
          details = DETAILS_2
        }
      )

    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      eventGroupMetadataDescriptorService
        .streamEventGroupMetadataDescriptors(
          streamEventGroupMetadataDescriptorsRequest {
            filter = filter { this.externalDataProviderId = externalDataProviderId }
            limit = 1
          }
        )
        .toList()

    if (
      eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId <
        eventGroupMetadataDescriptor2.externalEventGroupMetadataDescriptorId
    ) {
      assertThat(eventGroupMetadataDescriptors).containsExactly(eventGroupMetadataDescriptor)
    } else {
      assertThat(eventGroupMetadataDescriptors).containsExactly(eventGroupMetadataDescriptor2)
    }
  }

  @Test
  fun `streamEventGroupMetadataDescriptors skips results based on key after`(): Unit = runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val eventGroupMetadataDescriptor =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId
          details = DETAILS
        }
      )

    val eventGroupMetadataDescriptor2 =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        eventGroupMetadataDescriptor {
          this.externalDataProviderId = externalDataProviderId
          details = DETAILS_2
        }
      )

    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      eventGroupMetadataDescriptorService
        .streamEventGroupMetadataDescriptors(
          streamEventGroupMetadataDescriptorsRequest {
            filter = filter {
              this.externalDataProviderId = externalDataProviderId
              keyAfter = eventGroupMetadataDescriptorKey {
                this.externalDataProviderId = externalDataProviderId
                this.externalEventGroupMetadataDescriptorId =
                  if (
                    eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId <
                      eventGroupMetadataDescriptor2.externalEventGroupMetadataDescriptorId
                  ) {
                    eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId
                  } else {
                    eventGroupMetadataDescriptor2.externalEventGroupMetadataDescriptorId
                  }
              }
            }
            limit = 1
          }
        )
        .toList()

    if (
      eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId <
        eventGroupMetadataDescriptor2.externalEventGroupMetadataDescriptorId
    ) {
      assertThat(eventGroupMetadataDescriptors).containsExactly(eventGroupMetadataDescriptor2)
    } else {
      assertThat(eventGroupMetadataDescriptors).containsExactly(eventGroupMetadataDescriptor)
    }
  }

  @Test
  fun `streamEventGroupMetadataDescriptors fails for negative limit`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupMetadataDescriptorService.streamEventGroupMetadataDescriptors(
          streamEventGroupMetadataDescriptorsRequest { limit = -1 }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Limit")
  }
}

data class EventGroupMetadataDescriptorsAndHelperServices<
  T : EventGroupMetadataDescriptorsCoroutineImplBase
>(
  val eventGroupMetadataDescriptorService: T,
  val dataProvidersService: DataProvidersCoroutineImplBase,
)
