/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.kingdom.CreatePopulationRequest
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationDetailsKt
import org.wfanet.measurement.internal.kingdom.PopulationKt.populationBlob
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.certificateDetails
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createPopulationRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderDetails
import org.wfanet.measurement.internal.kingdom.getPopulationRequest
import org.wfanet.measurement.internal.kingdom.population
import org.wfanet.measurement.internal.kingdom.populationDetails
import org.wfanet.measurement.internal.kingdom.streamPopulationsRequest

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
  fun `createPopulation returns Population`() = runBlocking {
    val dataProvider: DataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)
    val request: CreatePopulationRequest = createPopulationRequest {
      population = POPULATION.copy { externalDataProviderId = dataProvider.externalDataProviderId }
    }

    val response: Population = populationsService.createPopulation(request)

    assertThat(response)
      .ignoringFields(
        Population.CREATE_TIME_FIELD_NUMBER,
        Population.EXTERNAL_POPULATION_ID_FIELD_NUMBER,
      )
      .isEqualTo(request.population)
    assertThat(response.createTime.seconds).isGreaterThan(0L)
    assertThat(response.externalPopulationId).isNotEqualTo(request.population.externalPopulationId)
  }

  @Test
  fun `createPopulation returns existing Population when request ID matches`() = runBlocking {
    val dataProvider: DataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)
    val requestId = UUID.randomUUID().toString()
    val existingPopulation =
      populationsService.createPopulation(
        createPopulationRequest {
          population =
            POPULATION.copy { externalDataProviderId = dataProvider.externalDataProviderId }
          this.requestId = requestId
        }
      )
    val request: CreatePopulationRequest = createPopulationRequest {
      population =
        POPULATION.copy {
          externalDataProviderId = dataProvider.externalDataProviderId
          description = "different description"
        }
      this.requestId = requestId
    }

    val response: Population = populationsService.createPopulation(request)

    assertThat(response).isEqualTo(existingPopulation)
  }

  @Test
  fun `createPopulation fails with invalid DataProvider Id`() = runBlocking {
    val request = createPopulationRequest {
      population = POPULATION.copy { externalDataProviderId = 404L }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { populationsService.createPopulation(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `getPopulation returns created Population`() = runBlocking {
    val dataProvider: DataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)
    val population: Population =
      populationsService.createPopulation(
        createPopulationRequest {
          population =
            POPULATION.copy { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    val request = getPopulationRequest {
      externalDataProviderId = population.externalDataProviderId
      externalPopulationId = population.externalPopulationId
    }

    val response: Population = populationsService.getPopulation(request)

    assertThat(response).isEqualTo(population)
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
  fun `getPopulation fails when Population not found`() = runBlocking {
    val dataProvider = dataProvidersService.createDataProvider(DATA_PROVIDER)

    val request = getPopulationRequest {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalPopulationId = 404L
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
        createPopulationRequest {
          population =
            POPULATION.copy {
              externalDataProviderId = dataProvider.externalDataProviderId
              description = DESCRIPTION + "1"
            }
        }
      )

    val population2 =
      populationsService.createPopulation(
        createPopulationRequest {
          population =
            POPULATION.copy {
              externalDataProviderId = dataProvider.externalDataProviderId
              description = DESCRIPTION + "2"
            }
        }
      )

    val population3 =
      populationsService.createPopulation(
        createPopulationRequest {
          population =
            POPULATION.copy {
              externalDataProviderId = dataProvider.externalDataProviderId
              description = DESCRIPTION + "3"
            }
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
        createPopulationRequest {
          population =
            POPULATION.copy {
              externalDataProviderId = dataProvider.externalDataProviderId
              description = DESCRIPTION + "1"
            }
        }
      )

    val population2 =
      populationsService.createPopulation(
        createPopulationRequest {
          population =
            POPULATION.copy {
              externalDataProviderId = dataProvider.externalDataProviderId
              description = DESCRIPTION + "2"
            }
        }
      )

    val population3 =
      populationsService.createPopulation(
        createPopulationRequest {
          population =
            POPULATION.copy {
              externalDataProviderId = dataProvider.externalDataProviderId
              description = DESCRIPTION + "3"
            }
        }
      )

    val request = streamPopulationsRequest {
      filter = filter {
        externalDataProviderId = dataProvider.externalDataProviderId
        after = afterFilter {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalPopulationId = population3.externalPopulationId
          createTime = population3.createTime
        }
      }
    }

    val response: List<Population> = populationsService.streamPopulations(request).toList()

    assertThat(response).containsExactly(population2, population1).inOrder()
  }

  companion object {
    private const val DESCRIPTION = "Population description"
    private const val BLOB_URI = "blobUri"
    private val DATA_PROVIDER = dataProvider {
      certificate {
        notValidBefore = timestamp { seconds = 12345 }
        notValidAfter = timestamp { seconds = 23456 }
        details = certificateDetails {
          x509Der = ByteString.copyFromUtf8("This is a certificate der.")
        }
      }
      details = dataProviderDetails {
        apiVersion = "v2alpha"
        publicKey = ByteString.copyFromUtf8("This is a  public key.")
        publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
      }
    }
    private val POPULATION = population {
      description = DESCRIPTION
      // TODO(@SanjayVas): Stop setting this field once it is no longer used by operators.
      populationBlob = populationBlob { blobUri = BLOB_URI }
      details = populationDetails {
        populationSpec =
          PopulationDetailsKt.populationSpec {
            subpopulations +=
              PopulationDetailsKt.PopulationSpecKt.subPopulation {
                vidRanges +=
                  PopulationDetailsKt.PopulationSpecKt.vidRange {
                    startVid = 1
                    endVidInclusive = 10_000
                  }
              }
          }
      }
    }
  }
}
