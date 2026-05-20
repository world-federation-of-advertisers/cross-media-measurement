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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ListPopulationsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.getPopulationRequest
import org.wfanet.measurement.api.v2alpha.listPopulationsPageToken
import org.wfanet.measurement.api.v2alpha.listPopulationsRequest
import org.wfanet.measurement.api.v2alpha.listPopulationsResponse
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.CreatePopulationRequest as InternalCreatePopulationRequest
import org.wfanet.measurement.internal.kingdom.Population as InternalPopulation
import org.wfanet.measurement.internal.kingdom.PopulationDetailsKt
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequest
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createPopulationRequest as internalCreatePopulationRequest
import org.wfanet.measurement.internal.kingdom.getPopulationRequest as internalGetPopulationRequest
import org.wfanet.measurement.internal.kingdom.population as internalPopulation
import org.wfanet.measurement.internal.kingdom.populationDetails
import org.wfanet.measurement.internal.kingdom.streamPopulationsRequest as internalStreamPopulationsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PopulationNotFoundException

private const val DEFAULT_LIMIT = 50

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME_2 = "dataProviders/AAAAAAAAAJs"
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
private const val DESCRIPTION = "Description"
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val PERSON_ATTRIBUTES =
  ProtoAny.pack(
    person {
      gender = Person.Gender.MALE
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }
  )

private val INTERNAL_POPULATION: InternalPopulation = internalPopulation {
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalPopulationId = EXTERNAL_POPULATION_ID
  description = DESCRIPTION
  createTime = CREATE_TIME
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
            attributes += PERSON_ATTRIBUTES
          }
      }
  }
}

private val POPULATION: Population = population {
  name = POPULATION_NAME
  description = DESCRIPTION
  createTime = CREATE_TIME
  populationSpec = populationSpec {
    subpopulations +=
      PopulationSpecKt.subPopulation {
        vidRanges +=
          PopulationSpecKt.vidRange {
            startVid = 1
            endVidInclusive = 10_000
          }
        attributes += PERSON_ATTRIBUTES
      }
  }
}

@RunWith(JUnit4::class)
class PopulationsServiceTest {

  private val internalPopulationsMock: PopulationsCoroutineImplBase = mockService {
    onBlocking { createPopulation(any()) }
      .thenAnswer {
        val request = it.getArgument<InternalCreatePopulationRequest>(0)
        if (request.population.externalDataProviderId != 123L) {
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
          INTERNAL_POPULATION.copy { externalPopulationId = EXTERNAL_POPULATION_ID_3 },
        )
      )
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalPopulationsMock) }

  private lateinit var service: PopulationsService

  @Before
  fun initService() {
    service =
      PopulationsService(PopulationsGrpcKt.PopulationsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createPopulation returns population`() {
    val request = createPopulationRequest {
      parent = DATA_PROVIDER_NAME
      population = POPULATION
      requestId = UUID.randomUUID().toString()
    }
    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createPopulation(request) }
      }

    val expected = POPULATION

    verifyProtoArgument(internalPopulationsMock, PopulationsCoroutineImplBase::createPopulation)
      .isEqualTo(
        internalCreatePopulationRequest {
          population =
            INTERNAL_POPULATION.copy {
              clearCreateTime()
              clearExternalPopulationId()
            }
          requestId = request.requestId
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createPopulation throws PERMISSION_DENIED with measurement consumer principal`() {
    val request = createPopulationRequest {
      parent = DATA_PROVIDER_NAME
      population = POPULATION
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createPopulation(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createPopulation throws PERMISSION_DENIED when principal is model provider`() {
    val request = createPopulationRequest {
      parent = DATA_PROVIDER_NAME
      population = POPULATION
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.createPopulation(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createPopulation throws UNAUTHENTICATED when no principal is found`() {
    val request = createPopulationRequest {
      parent = DATA_PROVIDER_NAME
      population = POPULATION
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createPopulation(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getPopulation returns population when data provider caller who created population is found`() {
    val request = getPopulationRequest { name = POPULATION_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.getPopulation(request) }
      }

    val expected = POPULATION

    verifyProtoArgument(internalPopulationsMock, PopulationsCoroutineImplBase::getPopulation)
      .isEqualTo(
        internalGetPopulationRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalPopulationId = EXTERNAL_POPULATION_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getPopulation returns population when model provider caller is found`() {
    val request = getPopulationRequest { name = POPULATION_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.getPopulation(request) }
      }

    val expected = POPULATION

    verifyProtoArgument(internalPopulationsMock, PopulationsCoroutineImplBase::getPopulation)
      .isEqualTo(
        internalGetPopulationRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalPopulationId = EXTERNAL_POPULATION_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getPopulation throws PERMISSION_DENIED when principal is data provider that did not create population`() {
    val request = getPopulationRequest { name = POPULATION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.getPopulation(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getPopulation throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = getPopulationRequest { name = POPULATION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getPopulation(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getPopulation throws UNAUTHENTICATED when no principal is found`() {
    val request = getPopulationRequest { name = POPULATION_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getPopulation(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listPopulations with parent succeeds for data provider caller who created population`() {
    val request = listPopulationsRequest { parent = DATA_PROVIDER_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listPopulations(request) }
      }

    val expected = listPopulationsResponse {
      populations += POPULATION
      populations += POPULATION.copy { name = POPULATION_NAME_2 }
      populations += POPULATION.copy { name = POPULATION_NAME_3 }
    }

    val streamPopulationsRequest: StreamPopulationsRequest = captureFirst {
      verify(internalPopulationsMock).streamPopulations(capture())
    }

    assertThat(streamPopulationsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamPopulationsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamPopulationsRequestKt.filter { externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listPopulations with parent succeeds for model provider caller`() {
    val request = listPopulationsRequest { parent = DATA_PROVIDER_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listPopulations(request) }
      }

    val expected = listPopulationsResponse {
      populations += POPULATION
      populations += POPULATION.copy { name = POPULATION_NAME_2 }
      populations += POPULATION.copy { name = POPULATION_NAME_3 }
    }

    val streamPopulationsRequest: StreamPopulationsRequest = captureFirst {
      verify(internalPopulationsMock).streamPopulations(capture())
    }

    assertThat(streamPopulationsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamPopulationsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamPopulationsRequestKt.filter { externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listPopulations with pagination succeeds`() {
    val request = listPopulationsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listPopulations(request) }
      }

    val expected = listPopulationsResponse {
      populations += POPULATION
      populations += POPULATION.copy { name = POPULATION_NAME_2 }
      val listPopulationsPageToken = listPopulationsPageToken {
        pageSize = request.pageSize
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastPopulation = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalPopulationId = EXTERNAL_POPULATION_ID_2
        }
      }
      nextPageToken = listPopulationsPageToken.toByteArray().base64UrlEncode()
    }

    val streamPopulationsRequest: StreamPopulationsRequest = captureFirst {
      verify(internalPopulationsMock).streamPopulations(capture())
    }

    assertThat(streamPopulationsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamPopulationsRequest {
          limit = request.pageSize + 1
          filter =
            StreamPopulationsRequestKt.filter { externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `listPopulations with page token succeeds`() {
    val pageSize = 2
    val request = listPopulationsRequest {
      parent = DATA_PROVIDER_NAME
      val listPopulationsPageToken = listPopulationsPageToken {
        this.pageSize = pageSize
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastPopulation = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalPopulationId = EXTERNAL_POPULATION_ID
        }
      }
      pageToken = listPopulationsPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listPopulations(request) }
      }

    val expected = listPopulationsResponse {
      populations += POPULATION.copy { name = POPULATION_NAME }
      populations += POPULATION.copy { name = POPULATION_NAME_2 }
      val listPopulationsPageToken = listPopulationsPageToken {
        this.pageSize = pageSize
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        lastPopulation = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalPopulationId = EXTERNAL_POPULATION_ID_2
        }
      }
      nextPageToken = listPopulationsPageToken.toByteArray().base64UrlEncode()
    }

    val streamPopulationsRequest: StreamPopulationsRequest = captureFirst {
      verify(internalPopulationsMock).streamPopulations(capture())
    }

    assertThat(streamPopulationsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamPopulationsRequest {
          limit = pageSize + 1
          filter =
            StreamPopulationsRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              after = afterFilter {
                createTime = CREATE_TIME
                externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
                externalPopulationId = EXTERNAL_POPULATION_ID
              }
            }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `listPopulations throws INVALID_ARGUMENT when parent doesn't match parent in page token`() {
    val request = listPopulationsRequest {
      parent = DATA_PROVIDER_NAME
      val listPopulationsPageToken = listPopulationsPageToken {
        externalDataProviderId = 789
        lastPopulation = previousPageEnd {
          createTime = CREATE_TIME
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalPopulationId = EXTERNAL_POPULATION_ID
        }
      }
      pageToken = listPopulationsPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listPopulations(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listPopulations throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = listPopulationsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listPopulations(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listPopulations throws PERMISSION_DENIED when principal is data provider that did not create population`() {
    val request = listPopulationsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.listPopulations(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listPopulations throws UNAUTHENTICATED when no principal is found`() {
    val request = listPopulationsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listPopulations(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createPopulation throws NOT_FOUND with data provider name when data provider not fonud`() {
    internalPopulationsMock.stub {
      onBlocking { createPopulation(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(EXTERNAL_DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createPopulation(
              createPopulationRequest {
                parent = DATA_PROVIDER_NAME
                population = POPULATION
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  @Test
  fun `getPopulation throws NOT_FOUND with population name when population not found`() {
    internalPopulationsMock.stub {
      onBlocking { getPopulation(any()) }
        .thenThrow(
          PopulationNotFoundException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              ExternalId(EXTERNAL_POPULATION_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "Population not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getPopulation(getPopulationRequest { name = POPULATION_NAME }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("population", POPULATION_NAME)
  }

  @Test
  fun `listPopulations throws INVALID_ARGUMENT when fields missing`() {
    internalPopulationsMock.stub {
      onBlocking { streamPopulations(any()) }
        .thenThrow(Status.INVALID_ARGUMENT.withDescription("Missing fields").asRuntimeException())
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.listPopulations(listPopulationsRequest { parent = DATA_PROVIDER_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
