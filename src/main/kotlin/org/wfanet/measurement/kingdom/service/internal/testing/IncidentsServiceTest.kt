/*
 * Copyright 2024 The Cross-Media Measurement Authors
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
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Incident
import org.wfanet.measurement.internal.kingdom.IncidentsGrpcKt.IncidentsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.createIncidentRequest
import org.wfanet.measurement.internal.kingdom.incident
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

@RunWith(JUnit4::class)
abstract class IncidentsServiceTest<T : IncidentsCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  protected data class Services<T>(
    val incidentsService: T,
    val dataProvidersService: DataProvidersCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  private lateinit var incidentsService: T

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    incidentsService = services.incidentsService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `createIncident succeeds`() = runBlocking {
    val dataProvider = population.createDataProvider(dataProvidersService)

    val createIncidentRequest = createIncidentRequest {
      incident = incident {
        externalDataProviderId = dataProvider.externalDataProviderId
        description = "description"
        solution = "solution"
        canRerun = true
      }
    }

    val createdIncident = incidentsService.createIncident(createIncidentRequest)

    assertThat(createdIncident.externalIncidentId).isNotEqualTo(0L)
    assertThat(createdIncident.createTime.seconds).isGreaterThan(0L)
    assertThat(createdIncident.latestAssociationTime).isEqualTo(Timestamp.getDefaultInstance())
    assertThat(createdIncident)
      .ignoringFields(
        Incident.EXTERNAL_INCIDENT_ID_FIELD_NUMBER,
        Incident.CREATE_TIME_FIELD_NUMBER,
        Incident.LATEST_ASSOCIATION_TIME_FIELD_NUMBER,
      )
      .isEqualTo(createIncidentRequest.incident)
  }

  @Test
  fun `createIncident fails for missing data provider`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        incidentsService.createIncident(
          createIncidentRequest {
            incident = incident {
              externalDataProviderId = 100L
              description = "description"
              solution = "solution"
              canRerun = true
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  companion object {
    private const val RANDOM_SEED = 1
  }
}
