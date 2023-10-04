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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.Population as InternalPopulation
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.population as internalPopulation
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.internal.kingdom.PopulationKt.populationBlob as internalPopulationBlob
import org.wfanet.measurement.internal.kingdom.eventTemplate as internalEventTemplate
import org.wfanet.measurement.api.v2alpha.PopulationKt.populationBlob
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.eventTemplate
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt

private const val DEFAULT_LIMIT = 50

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val POPULATION_NAME = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAHs"
private const val POPULATION_NAME_2 = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAJs"
private const val POPULATION_NAME_3 = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAKs"
private val EXTERNAL_DATA_PROVIDER_ID =
  apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
private val EXTERNAL_POPULATION_ID =
  apiIdToExternalId(PopulationKey.fromName(POPULATION_NAME)!!.populationId)
private val EXTERNAL_POPULATION_ID_2 =
  apiIdToExternalId(PopulationKey.fromName(POPULATION_NAME_2)!!.populationId)
private val EXTERNAL_POPULATION_ID_3 =
  apiIdToExternalId(PopulationKey.fromName(POPULATION_NAME_3)!!.populationId)
private const val DISPLAY_NAME = "Display name"
private const val DESCRIPTION = "Description"
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val MODEL_BLOB_URI = "Blob URI"
private val EVENT_TEMPLATE_TYPE = "Type 1"

private val INTERNAL_POPULATION: InternalPopulation = internalPopulation {
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalPopulationId = EXTERNAL_POPULATION_ID
  description = DESCRIPTION
  createTime = CREATE_TIME
  populationBlob = internalPopulationBlob {
    modelBlobUri = MODEL_BLOB_URI
  }
  eventTemplate = internalEventTemplate {
    fullyQualifiedType = EVENT_TEMPLATE_TYPE
  }
}

private val POPULATION: Population = population {
  name = POPULATION_NAME
  description = DESCRIPTION
  createTime = CREATE_TIME
  populationBlob = populationBlob {
    modelBlobUri = MODEL_BLOB_URI
  }
  eventTemplate = eventTemplate {
    type = EVENT_TEMPLATE_TYPE
  }
}

@RunWith(JUnit4::class)
class PopulationsServiceTest {

  private val internalPopulationsMock: PopulationsCoroutineImplBase =
    mockService() {
      onBlocking { createPopulation(any()) }
        .thenAnswer {
          val request = it.getArgument<InternalPopulation>(0)
          if (request.externalDataProviderId != 123L) {
            failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
          } else {
            INTERNAL_POPULATION
          }
        }
      onBlocking { getPopulation(any()) }.thenReturn(INTERNAL_POPULATION)
      onBlocking { streamPopulations(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_POPULATION,
            INTERNAL_POPULATION.copy { externalPopulationId = EXTERNAL_POPULATION_ID_2 },
            INTERNAL_POPULATION.copy { externalPopulationId = EXTERNAL_POPULATION_ID_3 }
          )
        )
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalPopulationsMock) }

  private lateinit var service: PopulationsService

  @Before
  fun initService() {
    service =
      PopulationsService(
        PopulationsGrpcKt.PopulationsCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `createPopulation returns model suite`() {
    val request = createPopulationRequest {
      parent = DATA_PROVIDER_NAME
      population = POPULATION
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createPopulation(request) }
      }

    val expected = POPULATION

    verifyProtoArgument(internalPopulationsMock, PopulationsCoroutineImplBase::createPopulation)
      .isEqualTo(
        INTERNAL_POPULATION.copy {
          clearCreateTime()
          clearExternalPopulationId()
        }
      )

    assertThat(result).isEqualTo(expected)
  }
}
