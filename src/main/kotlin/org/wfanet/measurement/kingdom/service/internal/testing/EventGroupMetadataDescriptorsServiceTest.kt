package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
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
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorKt.details
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.createEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.getEventGroupMetadataDescriptorRequest

private const val RANDOM_SEED = 1
private val DETAILS = details {
  apiVersion = Version.V2_ALPHA.string
  descriptorSet = FileDescriptorSet.getDefaultInstance()
}

@RunWith(JUnit4::class)
abstract class EventGroupMetadataDescriptorsServiceTest<
  T : EventGroupMetadataDescriptorsCoroutineImplBase> {

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
        createEventGroupMetadataDescriptorRequest {
          this.eventGroupMetadataDescriptor = eventGroupMetadataDescriptor
        }
      )

    val eventGroupMetadataDescriptorRead =
      eventGroupMetadataDescriptorService.getEventGroupMetadataDescriptor(
        getEventGroupMetadataDescriptorRequest {
          this.eventGroupMetadataDescriptor =
            eventGroupMetadataDescriptor {
              externalEventGroupMetadataDescriptorId =
                createdDescriptor.externalEventGroupMetadataDescriptorId
              this.externalDataProviderId = externalDataProviderId
            }
        }
      )

    assertThat(eventGroupMetadataDescriptorRead).isEqualTo(createdDescriptor)
  }

  @Test
  fun `getEventGroupMetadataDescriptor fails for missing EventGroupMetadataDescriptor`() =
      runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        eventGroupMetadataDescriptorService.getEventGroupMetadataDescriptor(
          getEventGroupMetadataDescriptorRequest {
            this.eventGroupMetadataDescriptor =
              eventGroupMetadataDescriptor { externalEventGroupMetadataDescriptorId = 1L }
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
        createEventGroupMetadataDescriptorRequest {
          this.eventGroupMetadataDescriptor = eventGroupMetadataDescriptor
        }
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
          createEventGroupMetadataDescriptorRequest {
            this.eventGroupMetadataDescriptor = eventGroupMetadataDescriptor
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: DataProvider not found")
  }

  @Test
  fun `createEventGroupMetadataDescriptor returns already created Descriptor for the same externalEventGroupMetadataDescriptorId`() =
      runBlocking {
    val externalDataProviderId =
      population.createDataProvider(dataProvidersService).externalDataProviderId

    val createdEventGroupMetadataDescriptor =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        createEventGroupMetadataDescriptorRequest {
          this.eventGroupMetadataDescriptor =
            eventGroupMetadataDescriptor {
              this.externalDataProviderId = externalDataProviderId
              details = DETAILS
            }
        }
      )
    val secondCreatedEventGroupMetadataDescriptorAttempt =
      eventGroupMetadataDescriptorService.createEventGroupMetadataDescriptor(
        createEventGroupMetadataDescriptorRequest {
          this.eventGroupMetadataDescriptor =
            eventGroupMetadataDescriptor {
              this.externalDataProviderId = externalDataProviderId
              externalEventGroupMetadataDescriptorId =
                createdEventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId
              details = DETAILS
            }
        }
      )

    assertThat(secondCreatedEventGroupMetadataDescriptorAttempt)
      .isEqualTo(createdEventGroupMetadataDescriptor)
  }
}

data class EventGroupMetadataDescriptorsAndHelperServices<
  T : EventGroupMetadataDescriptorsCoroutineImplBase>(
  val eventGroupMetadataDescriptorService: T,
  val dataProvidersService: DataProvidersCoroutineImplBase,
)
