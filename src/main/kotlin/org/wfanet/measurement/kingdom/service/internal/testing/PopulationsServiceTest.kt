package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationKt.populationBlob
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.eventTemplate
import org.wfanet.measurement.internal.kingdom.getPopulationRequest
import org.wfanet.measurement.internal.kingdom.population
import org.wfanet.measurement.internal.kingdom.streamPopulationsRequest

private const val RANDOM_SEED = 1

abstract class PopulationsServiceTest<T : PopulationsCoroutineImplBase> {

  protected data class Services<T>(
    val populationsService: T,
    val dataProvidersService: DataProvidersCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = SequentialIdGenerator()

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var populationsService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    populationsService = services.populationsService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `createPopulation succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)
    val request = population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = DESCRIPTION
      populationBlob = populationBlob { modelBlobUri = BLOB_URI }
      eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
    }

    val response = populationsService.createPopulation(request)

    assertThat(response)
      .ignoringFields(
        Population.CREATE_TIME_FIELD_NUMBER,
        Population.EXTERNAL_POPULATION_ID_FIELD_NUMBER
      )
      .isEqualTo(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = DESCRIPTION
          populationBlob = populationBlob { modelBlobUri = BLOB_URI }
          eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
        }
      )
    assertThat(response.createTime.seconds).isGreaterThan(0L)
    assertThat(response.externalPopulationId).isNotEqualTo(request.externalPopulationId)
  }

  @Test
  fun `createPopulation fails with invalid DataProvider Id`() = runBlocking {
    val request = population {
      externalDataProviderId = 1234
      description = DESCRIPTION
      populationBlob = populationBlob { modelBlobUri = BLOB_URI }
      eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { populationsService.createPopulation(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  fun `getPopulation succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)

    val population = population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = DESCRIPTION
      populationBlob = populationBlob { modelBlobUri = BLOB_URI }
      eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
    }
    val createdPopulation = populationsService.createPopulation(population)

    val request = getPopulationRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalPopulationId = createdPopulation.externalPopulationId
    }

    val response = populationsService.getPopulation(request)

    assertThat(createdPopulation).isEqualTo(response)
  }

  @Test
  fun `getPopulation fails with invalid PopulationId`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)

    val request = getPopulationRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalPopulationId = 1234
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { populationsService.getPopulation(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Population not found")
  }

  @Test
  fun `getPopulation fails with invalid DataProviderId`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)

    val population = population {
      externalDataProviderId = dataProvider.externalDataProviderId
      description = DESCRIPTION
      populationBlob = populationBlob { modelBlobUri = BLOB_URI }
      eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
    }
    val createdPopulation = populationsService.createPopulation(population)

    val request = getPopulationRequest {
      externalDataProviderId = 1234
      externalPopulationId = createdPopulation.externalPopulationId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { populationsService.getPopulation(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Population not found")
  }

  @Test
  fun `streamPopulations succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)

    val population1 =
      populationsService.createPopulation(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = DESCRIPTION + "1"
          populationBlob = populationBlob { modelBlobUri = BLOB_URI }
          eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
        }
      )

    val population2 =
      populationsService.createPopulation(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = DESCRIPTION + "2"
          populationBlob = populationBlob { modelBlobUri = BLOB_URI }
          eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
        }
      )

    val population3 =
      populationsService.createPopulation(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = DESCRIPTION + "3"
          populationBlob = populationBlob { modelBlobUri = BLOB_URI }
          eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
        }
      )

    val request = streamPopulationsRequest {
      filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
    }

    val response: List<Population> = populationsService.streamPopulations(request).toList()

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .containsExactly(population3, population2, population1)
      .inOrder()
  }

  @Test
  fun `streamPopulations with After filter succeeds`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)

    val population1 =
      populationsService.createPopulation(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = DESCRIPTION + "1"
          populationBlob = populationBlob { modelBlobUri = BLOB_URI }
          eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
        }
      )

    val population2 =
      populationsService.createPopulation(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalDataProviderId = dataProvider.externalDataProviderId
          description = DESCRIPTION + "2"
          populationBlob = populationBlob { modelBlobUri = BLOB_URI }
          eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
        }
      )

    val population3 =
      populationsService.createPopulation(
        population {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = DESCRIPTION + "3"
          populationBlob = populationBlob { modelBlobUri = BLOB_URI }
          eventTemplate = eventTemplate { fullyQualifiedType = TYPE }
        }
      )

    val request = streamPopulationsRequest {
      filter = filter {
        externalDataProviderId = dataProvider.externalDataProviderId
        after = afterFilter {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalPopulationId = population1.externalPopulationId
          createTime = timestamp { seconds = population1.createTime.seconds }
        }
      }
    }

    val response: List<Population> = populationsService.streamPopulations(request).toList()

    assertThat(response).containsExactly(population3, population2).inOrder()
  }

  companion object {
    private const val DESCRIPTION = "Population description"
    private const val BLOB_URI = "modelBlobUri"
    private const val TYPE = "Type 1"
    private val DATA_PROVIDER = dataProvider {
      certificate {
        notValidBefore = timestamp { seconds = 12345 }
        notValidAfter = timestamp { seconds = 23456 }
        details =
          CertificateKt.details { x509Der = ByteString.copyFromUtf8("This is a certificate der.") }
      }
      details =
        DataProviderKt.details {
          apiVersion = "v2alpha"
          publicKey = ByteString.copyFromUtf8("This is a  public key.")
          publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
        }
    }
  }
}
