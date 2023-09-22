package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.eventTemplate
import org.wfanet.measurement.internal.kingdom.getPopulationRequest
import org.wfanet.measurement.internal.kingdom.population
import org.wfanet.measurement.internal.kingdom.streamPopulationsRequest


private const val RANDOM_SEED = 1

abstract class PopulationsServiceTest<T: PopulationsCoroutineImplBase> {

  protected data class Services<T>(
    val populationsService: T,
    val dataProvidersService: DataProvidersCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var populationsService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  val desc = "Population description"
  val blobUri = "modelBlobUri"
  val type = "Type 1"
  val dp = dataProvider {
    certificate {
      notValidBefore = timestamp { seconds = 12345 }
      notValidAfter = timestamp { seconds = 23456 }
      details = CertificateKt.details { x509Der = ByteString.copyFromUtf8("This is a certificate der.") }
    }
    details =
      DataProviderKt.details {
        apiVersion = "v2alpha"
        publicKey =  ByteString.copyFromUtf8("This is a  public key.")
        publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
      }
  }

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    populationsService = services.populationsService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `createPopulation succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(dp)
    val population = population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = desc
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
      eventTemplate = eventTemplate { fullyQualifiedType = type }
    }

    val createdPopulation = populationsService.createPopulation(population)

    assertThat(createdPopulation)
      .ignoringFields(
        Population.CREATE_TIME_FIELD_NUMBER,
        Population.EXTERNAL_POPULATION_ID_FIELD_NUMBER
      )
      .isEqualTo(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = desc
          populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
          eventTemplate = eventTemplate { fullyQualifiedType = type }
        }
      )
  }

  fun `createPopulation fails with invalid DataProvider Id`() = runBlocking {
    val population = population {
      externalDataProviderId = 0
      description = desc
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
      eventTemplate = eventTemplate { fullyQualifiedType = type }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        populationsService.createPopulation(population)
      }

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Truth.assertThat(exception)
      .hasMessageThat()
      .contains("DataProvider not found")
  }

  fun `getPopulation succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(dp)
    val population = population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = desc
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
      eventTemplate = eventTemplate { fullyQualifiedType = type }
    }

    val createdPopulation = populationsService.createPopulation(population)

    val returnedPopulation = populationsService.getPopulation(getPopulationRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalPopulationId = createdPopulation.externalPopulationId
    })

    assertThat(createdPopulation).isEqualTo(returnedPopulation)
  }

  fun `getPopulation fails with invalid PopulationId`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(dp)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        populationsService.getPopulation(getPopulationRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalPopulationId = 0
        })
      }

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    Truth.assertThat(exception)
      .hasMessageThat()
      .contains("Population not found")
  }

  fun `getPopulation fails with invalid DataProviderId`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(dp)
    val population = population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = desc
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
      eventTemplate = eventTemplate { fullyQualifiedType = type }
    }

    val createdPopulation = populationsService.createPopulation(population)


    val exception =
      assertFailsWith<StatusRuntimeException> {
        populationsService.getPopulation(getPopulationRequest {
          externalDataProviderId = 0
          externalPopulationId = createdPopulation.externalPopulationId
        })
      }

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    Truth.assertThat(exception)
      .hasMessageThat()
      .contains("Population not found")
  }

  fun `streamPopulations succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(dp)

    val population1 = populationsService.createPopulation(population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = desc + "1"
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
      eventTemplate = eventTemplate { fullyQualifiedType = type }
    })

    val population2 = populationsService.createPopulation(population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = desc + "2"
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
      eventTemplate = eventTemplate { fullyQualifiedType = type }
    })

    val population3 = populationsService.createPopulation(population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = desc + "3"
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = blobUri }.build()
      eventTemplate = eventTemplate { fullyQualifiedType = type }
    })

    val populations: List<Population> = populationsService.streamPopulations(streamPopulationsRequest { StreamPopulationsRequestKt.filter { externalDataProviderId = dataProvider.externalDataProviderId } }).toList()

    assertThat(populations)
      .comparingExpectedFieldsOnly()
      .containsExactly(population1, population2, population3)
      .inOrder()
  }
}
