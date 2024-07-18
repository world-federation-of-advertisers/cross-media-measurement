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

package org.wfanet.measurement.kingdom.job

import com.google.protobuf.ByteString
import java.time.Clock
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.batchCreateMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementRequest
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerAccountsService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataProvidersService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerMeasurementConsumersService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerMeasurementsService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.kingdom.service.internal.testing.Population
import org.wfanet.measurement.kingdom.service.internal.testing.toDataProviderValue

@RunWith(JUnit4::class)
class OperationalMetricsJobTest {
  private val clock: Clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock, Random(1))
  private val population = Population(clock, idGenerator)

  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)
  private lateinit var spannerAccountsService: SpannerAccountsService
  private lateinit var spannerDataProvidersService: SpannerDataProvidersService
  private lateinit var spannerMeasurementConsumersService: SpannerMeasurementConsumersService
  private lateinit var spannerMeasurementsService: SpannerMeasurementsService

  @Before
  fun init() {
    spannerAccountsService = SpannerAccountsService(idGenerator, spannerDatabase.databaseClient)
    spannerDataProvidersService = SpannerDataProvidersService(idGenerator, spannerDatabase.databaseClient)
    spannerMeasurementConsumersService = SpannerMeasurementConsumersService(idGenerator, spannerDatabase.databaseClient)
    spannerMeasurementsService = SpannerMeasurementsService(idGenerator, spannerDatabase.databaseClient)
  }

  @Ignore
  @Test
  fun `job successfully creates json for appending from measurements`() = runBlocking {
    val dataProvider = population.createDataProvider(spannerDataProvidersService)
    val measurementConsumer =
      population.createMeasurementConsumer(spannerMeasurementConsumersService, spannerAccountsService)

    val computationParticipantMeasurementRequest =
      createMeasurementRequest {
        measurement = MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          providedMeasurementId = "computation-participant"
          dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
          details =
            details.copy {
              duchyProtocolConfig = duchyProtocolConfig {
                liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
              }
              protocolConfig = protocolConfig {
                liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
              }
            }
        }
      }

    val directMeasurementRequest =
      createMeasurementRequest {
        measurement = MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          providedMeasurementId = "direct"
          dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
          details =
            details.copy {
              clearDuchyProtocolConfig()
              protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
            }
        }
      }

    val createdMeasurements = spannerMeasurementsService.batchCreateMeasurements(batchCreateMeasurementsRequest {
      externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      requests += computationParticipantMeasurementRequest
      requests += directMeasurementRequest
    })

    val computationParticipantMeasurement: Measurement =
      if (createdMeasurements.measurementsList[0].details.hasDuchyProtocolConfig()) {
        createdMeasurements.measurementsList[0]
      } else {
        createdMeasurements.measurementsList[1]
      }

    val directMeasurement: Measurement =
      if (createdMeasurements.measurementsList[0].details.hasDuchyProtocolConfig()) {
        createdMeasurements.measurementsList[1]
      } else {
        createdMeasurements.measurementsList[0]
      }


  }

  companion object {
    private const val API_VERSION = "v2alpha"
    private val MEASUREMENT = measurement {
      details =
        MeasurementKt.details {
          apiVersion = API_VERSION
          measurementSpec = ByteString.copyFromUtf8("MeasurementSpec")
          measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
          measurementSpecSignatureAlgorithmOid = "2.9999"
        }
    }
  }
}
