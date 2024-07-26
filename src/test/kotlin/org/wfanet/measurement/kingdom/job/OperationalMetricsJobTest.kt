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

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.TableResult
import com.google.protobuf.ByteString
import java.time.Clock
import kotlin.random.Random
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.batchCancelMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.batchCreateMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementRequest
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
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

  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)
  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.KINGDOM_CHANGELOG_PATH)
  private lateinit var spannerAccountsService: SpannerAccountsService
  private lateinit var spannerDataProvidersService: SpannerDataProvidersService
  private lateinit var spannerMeasurementConsumersService: SpannerMeasurementConsumersService
  private lateinit var spannerMeasurementsService: SpannerMeasurementsService

  @Before
  fun init() {
    spannerAccountsService = SpannerAccountsService(idGenerator, spannerDatabase.databaseClient)
    spannerDataProvidersService =
      SpannerDataProvidersService(idGenerator, spannerDatabase.databaseClient)
    spannerMeasurementConsumersService =
      SpannerMeasurementConsumersService(idGenerator, spannerDatabase.databaseClient)
    spannerMeasurementsService =
      SpannerMeasurementsService(idGenerator, spannerDatabase.databaseClient)
  }

  @Test
  fun `job successfully creates protos for appending to streams`() = runBlocking {
    val dataProvider = population.createDataProvider(spannerDataProvidersService)
    val measurementConsumer =
      population.createMeasurementConsumer(
        spannerMeasurementConsumersService,
        spannerAccountsService,
      )

    val computationParticipantMeasurementRequest = createMeasurementRequest {
      measurement =
        MEASUREMENT.copy {
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

    val directMeasurementRequest = createMeasurementRequest {
      measurement =
        MEASUREMENT.copy {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          providedMeasurementId = "direct"
          dataProviders[dataProvider.externalDataProviderId] = dataProvider.toDataProviderValue()
          details =
            details.copy {
              clearDuchyProtocolConfig()
              protocolConfig = protocolConfig {
                direct = ProtocolConfig.Direct.getDefaultInstance()
              }
            }
        }
    }

    val createdMeasurements =
      spannerMeasurementsService.batchCreateMeasurements(
        batchCreateMeasurementsRequest {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          requests += computationParticipantMeasurementRequest
          requests += directMeasurementRequest
        }
      )

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

    spannerMeasurementsService.batchCancelMeasurements(
      batchCancelMeasurementsRequest {
        requests += cancelMeasurementRequest {
          externalMeasurementConsumerId = computationParticipantMeasurement.externalMeasurementConsumerId
          externalMeasurementId = computationParticipantMeasurement.externalMeasurementId
        }
        requests += cancelMeasurementRequest {
          externalMeasurementConsumerId = directMeasurement.externalMeasurementConsumerId
          externalMeasurementId = directMeasurement.externalMeasurementId
        }
      }
    )

    val primitiveFieldValue: FieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0")
    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll())
        .thenReturn(
          listOf(
            FieldValueList.of(
              mutableListOf(primitiveFieldValue, primitiveFieldValue, primitiveFieldValue),
              LATEST_MEASUREMENT_FIELD_LIST
            )
          )
        )
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any()))
        .thenReturn(tableResultMock)
    }

    val measurementsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any()))
        .thenReturn(async {  })
    }

    val requisitionsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any()))
        .thenReturn(async {  })
    }

    val computationParticipantsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any()))
        .thenReturn(async {  })
    }

    val latestMeasurementReadDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any()))
        .thenReturn(async {  })
    }

    val operationalMetricsJob =
      OperationalMetricsJob(
        spannerClient = spannerDatabase.databaseClient,
        bigQuery = bigQueryMock,
        datasetId = "dataset",
        latestMeasurementReadTableId = "table",
        measurementsDataWriter = measurementsDataWriterMock,
        requisitionsDataWriter = requisitionsDataWriterMock,
        computationParticipantsDataWriter = computationParticipantsDataWriterMock,
        latestMeasurementReadDataWriter = latestMeasurementReadDataWriterMock,
      )

    operationalMetricsJob.execute()
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      Llv2ProtocolConfig.setForTest(
        ProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance(),
        setOf(
          Population.AGGREGATOR_DUCHY.externalDuchyId,
          Population.WORKER1_DUCHY.externalDuchyId,
        ),
        2,
      )
    }

    private const val API_VERSION = "v2alpha"

    private val PUBLIC_API_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = ByteString.copyFromUtf8("key")
    }

    private val PUBLIC_API_MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.pack()
      reachAndFrequency = MeasurementSpecKt.reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams {
          epsilon = 1.1
          delta = 1.2
        }
        frequencyPrivacyParams = differentialPrivacyParams {
          epsilon = 2.1
          delta = 2.2
        }
        maximumFrequency = 10
      }
    }

    private val MEASUREMENT = measurement {
      details =
        MeasurementKt.details {
          apiVersion = API_VERSION
          measurementSpec = PUBLIC_API_MEASUREMENT_SPEC.toByteString()
          measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
          measurementSpecSignatureAlgorithmOid = "2.9999"
        }
    }
    private val LATEST_MEASUREMENT_FIELD_LIST: FieldList =
      FieldList.of(
        listOf(
          Field.of("update_time", LegacySQLTypeName.INTEGER),
          Field.of("external_measurement_consumer_id", LegacySQLTypeName.INTEGER),
          Field.of("external_measurement_id", LegacySQLTypeName.INTEGER)
        )
      )
  }
}
