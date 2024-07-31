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
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
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
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.internal.kingdom.ComputationParticipantData
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt
import org.wfanet.measurement.internal.kingdom.LatestMeasurementRead
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementData
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.RequisitionData
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementRequest
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.setMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerAccountsService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerCertificatesService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataProvidersService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerMeasurementConsumersService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerMeasurementsService
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerRequisitionsService
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
  private lateinit var spannerCertificatesService: SpannerCertificatesService
  private lateinit var spannerRequisitionsService: SpannerRequisitionsService

  @Before
  fun init() {
    spannerAccountsService = SpannerAccountsService(idGenerator, spannerDatabase.databaseClient)
    spannerDataProvidersService =
      SpannerDataProvidersService(idGenerator, spannerDatabase.databaseClient)
    spannerMeasurementConsumersService =
      SpannerMeasurementConsumersService(idGenerator, spannerDatabase.databaseClient)
    spannerMeasurementsService =
      SpannerMeasurementsService(idGenerator, spannerDatabase.databaseClient)
    spannerCertificatesService =
      SpannerCertificatesService(idGenerator, spannerDatabase.databaseClient)
    spannerRequisitionsService =
      SpannerRequisitionsService(idGenerator, spannerDatabase.databaseClient)
  }

  @Test
  fun `job successfully creates protos for appending to streams`() = runBlocking {
    val createdMeasurements =
      createSucceededMeasurements(
        population,
        spannerDataProvidersService,
        spannerMeasurementsService,
        spannerAccountsService,
        spannerMeasurementConsumersService,
        spannerRequisitionsService,
        spannerCertificatesService,
      )

    val computationParticipantMeasurement = createdMeasurements.computationParticipantMeasurement
    val directMeasurement = createdMeasurements.directMeasurement

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val measurementsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any())).thenAnswer {
        val protoRows: ProtoRows = it.getArgument(0)
        assertThat(protoRows.serializedRowsList).hasSize(2)

        val computationParticipantMeasurementData =
          MeasurementData.parseFrom(protoRows.serializedRowsList[1])
        assertThat(computationParticipantMeasurementData.measurementConsumerId)
          .isEqualTo(
            externalIdToApiId(computationParticipantMeasurement.externalMeasurementConsumerId)
          )
        assertThat(computationParticipantMeasurementData.measurementId)
          .isEqualTo(externalIdToApiId(computationParticipantMeasurement.externalMeasurementId))
        assertThat(computationParticipantMeasurementData.isDirect).isFalse()
        assertThat(computationParticipantMeasurementData.measurementType)
          .isEqualTo("REACH_AND_FREQUENCY")
        assertThat(computationParticipantMeasurementData.state).isEqualTo("SUCCEEDED")
        assertThat(computationParticipantMeasurementData.createTime)
          .isEqualTo(Timestamps.toMicros(computationParticipantMeasurement.createTime))
        assertThat(computationParticipantMeasurementData.updateTime)
          .isEqualTo(Timestamps.toMicros(computationParticipantMeasurement.updateTime))
        assertThat(computationParticipantMeasurementData.updateTimeMinusCreateTime)
          .isEqualTo(
            Durations.toMillis(
              Timestamps.between(
                computationParticipantMeasurement.createTime,
                computationParticipantMeasurement.updateTime,
              )
            )
          )
        assertThat(computationParticipantMeasurementData.updateTimeMinusCreateTimeSquared)
          .isEqualTo(
            computationParticipantMeasurementData.updateTimeMinusCreateTime *
              computationParticipantMeasurementData.updateTimeMinusCreateTime
          )

        val directMeasurementData = MeasurementData.parseFrom(protoRows.serializedRowsList[0])
        assertThat(directMeasurementData.measurementConsumerId)
          .isEqualTo(externalIdToApiId(directMeasurement.externalMeasurementConsumerId))
        assertThat(directMeasurementData.measurementId)
          .isEqualTo(externalIdToApiId(directMeasurement.externalMeasurementId))
        assertThat(directMeasurementData.isDirect).isTrue()
        assertThat(directMeasurementData.measurementType).isEqualTo("REACH_AND_FREQUENCY")
        assertThat(directMeasurementData.state).isEqualTo("SUCCEEDED")
        assertThat(directMeasurementData.createTime)
          .isEqualTo(Timestamps.toMicros(directMeasurement.createTime))
        assertThat(directMeasurementData.updateTime)
          .isEqualTo(Timestamps.toMicros(directMeasurement.updateTime))
        assertThat(directMeasurementData.updateTimeMinusCreateTime)
          .isEqualTo(
            Durations.toMillis(
              Timestamps.between(directMeasurement.createTime, directMeasurement.updateTime)
            )
          )
        assertThat(directMeasurementData.updateTimeMinusCreateTimeSquared)
          .isEqualTo(
            directMeasurementData.updateTimeMinusCreateTime *
              directMeasurementData.updateTimeMinusCreateTime
          )
        async {}
      }
    }

    val requisitionsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any())).thenAnswer {
        val protoRows: ProtoRows = it.getArgument(0)
        assertThat(protoRows.serializedRowsList).hasSize(2)

        val computationParticipantRequisitionData =
          RequisitionData.parseFrom(protoRows.serializedRowsList[1])
        assertThat(computationParticipantRequisitionData.measurementConsumerId)
          .isEqualTo(
            externalIdToApiId(computationParticipantMeasurement.externalMeasurementConsumerId)
          )
        assertThat(computationParticipantRequisitionData.measurementId)
          .isEqualTo(externalIdToApiId(computationParticipantMeasurement.externalMeasurementId))
        assertThat(computationParticipantRequisitionData.requisitionId)
          .isEqualTo(
            externalIdToApiId(
              computationParticipantMeasurement.requisitionsList[0].externalRequisitionId
            )
          )
        assertThat(computationParticipantRequisitionData.dataProviderId)
          .isEqualTo(
            externalIdToApiId(
              computationParticipantMeasurement.requisitionsList[0].externalDataProviderId
            )
          )
        assertThat(computationParticipantRequisitionData.isDirect).isFalse()
        assertThat(computationParticipantRequisitionData.measurementType)
          .isEqualTo("REACH_AND_FREQUENCY")
        assertThat(computationParticipantRequisitionData.state).isEqualTo("UNFULFILLED")
        assertThat(computationParticipantRequisitionData.createTime)
          .isEqualTo(Timestamps.toMicros(computationParticipantMeasurement.createTime))
        assertThat(computationParticipantRequisitionData.updateTime)
          .isEqualTo(
            Timestamps.toMicros(computationParticipantMeasurement.requisitionsList[0].updateTime)
          )
        assertThat(computationParticipantRequisitionData.updateTimeMinusCreateTime)
          .isEqualTo(
            Durations.toMillis(
              Timestamps.between(
                computationParticipantMeasurement.createTime,
                computationParticipantMeasurement.requisitionsList[0].updateTime,
              )
            )
          )
        assertThat(computationParticipantRequisitionData.updateTimeMinusCreateTimeSquared)
          .isEqualTo(
            computationParticipantRequisitionData.updateTimeMinusCreateTime *
              computationParticipantRequisitionData.updateTimeMinusCreateTime
          )

        val directRequisitionData = RequisitionData.parseFrom(protoRows.serializedRowsList[0])
        assertThat(directRequisitionData.measurementConsumerId)
          .isEqualTo(externalIdToApiId(directMeasurement.externalMeasurementConsumerId))
        assertThat(directRequisitionData.measurementId)
          .isEqualTo(externalIdToApiId(directMeasurement.externalMeasurementId))
        assertThat(directRequisitionData.requisitionId)
          .isEqualTo(externalIdToApiId(directMeasurement.requisitionsList[0].externalRequisitionId))
        assertThat(directRequisitionData.dataProviderId)
          .isEqualTo(
            externalIdToApiId(directMeasurement.requisitionsList[0].externalDataProviderId)
          )
        assertThat(directRequisitionData.isDirect).isTrue()
        assertThat(directRequisitionData.measurementType).isEqualTo("REACH_AND_FREQUENCY")
        assertThat(directRequisitionData.state).isEqualTo("FULFILLED")
        assertThat(directRequisitionData.createTime)
          .isEqualTo(Timestamps.toMicros(directMeasurement.createTime))
        assertThat(directRequisitionData.updateTime)
          .isEqualTo(Timestamps.toMicros(directMeasurement.requisitionsList[0].updateTime))
        assertThat(directRequisitionData.updateTimeMinusCreateTime)
          .isEqualTo(
            Durations.toMillis(
              Timestamps.between(
                directMeasurement.createTime,
                directMeasurement.requisitionsList[0].updateTime,
              )
            )
          )
        assertThat(directRequisitionData.updateTimeMinusCreateTimeSquared)
          .isEqualTo(
            directRequisitionData.updateTimeMinusCreateTime *
              directRequisitionData.updateTimeMinusCreateTime
          )
        async {}
      }
    }

    val computationParticipantsDataWriterMock: OperationalMetricsJob.DataWriter =
      mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenAnswer {
          val protoRows: ProtoRows = it.getArgument(0)
          assertThat(protoRows.serializedRowsList).hasSize(3)

          for (serializedProtoRow in protoRows.serializedRowsList) {
            val computationParticipantData =
              ComputationParticipantData.parseFrom(serializedProtoRow)
            for (computationParticipant in
              computationParticipantMeasurement.computationParticipantsList) {
              if (computationParticipant.externalDuchyId == computationParticipantData.duchyId) {
                assertThat(computationParticipantData.measurementConsumerId)
                  .isEqualTo(
                    externalIdToApiId(
                      computationParticipantMeasurement.externalMeasurementConsumerId
                    )
                  )
                assertThat(computationParticipantData.measurementId)
                  .isEqualTo(
                    externalIdToApiId(computationParticipantMeasurement.externalMeasurementId)
                  )
                assertThat(computationParticipantData.computationId)
                  .isEqualTo(externalIdToApiId(computationParticipant.externalComputationId))
                assertThat(computationParticipantData.protocol).isEqualTo("PROTOCOL_NOT_SET")
                assertThat(computationParticipantData.measurementType)
                  .isEqualTo("REACH_AND_FREQUENCY")
                assertThat(computationParticipantData.state).isEqualTo("CREATED")
                assertThat(computationParticipantData.createTime)
                  .isEqualTo(Timestamps.toMicros(computationParticipantMeasurement.createTime))
                assertThat(computationParticipantData.updateTime)
                  .isEqualTo(Timestamps.toMicros(computationParticipant.updateTime))
                assertThat(computationParticipantData.updateTimeMinusCreateTime)
                  .isEqualTo(
                    Durations.toMillis(
                      Timestamps.between(
                        computationParticipantMeasurement.createTime,
                        computationParticipant.updateTime,
                      )
                    )
                  )
                assertThat(computationParticipantData.updateTimeMinusCreateTimeSquared)
                  .isEqualTo(
                    computationParticipantData.updateTimeMinusCreateTime *
                      computationParticipantData.updateTimeMinusCreateTime
                  )
              }
            }
          }
          async {}
        }
      }

    val latestMeasurementReadDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any())).thenAnswer {
        val protoRows: ProtoRows = it.getArgument(0)
        assertThat(protoRows.serializedRowsList).hasSize(1)

        val latestMeasurementRead = LatestMeasurementRead.parseFrom(protoRows.serializedRowsList[0])
        assertThat(latestMeasurementRead.updateTime)
          .isEqualTo(Timestamps.toNanos(computationParticipantMeasurement.updateTime))
        assertThat(latestMeasurementRead.externalMeasurementConsumerId)
          .isEqualTo(computationParticipantMeasurement.externalMeasurementConsumerId)
        assertThat(latestMeasurementRead.externalMeasurementId)
          .isEqualTo(computationParticipantMeasurement.externalMeasurementId)
        async {}
      }
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

  @Test
  fun `job can process the next batch of measurements without starting at the beginning`() =
    runBlocking {
      val createdMeasurements =
        createSucceededMeasurements(
          population,
          spannerDataProvidersService,
          spannerMeasurementsService,
          spannerAccountsService,
          spannerMeasurementConsumersService,
          spannerRequisitionsService,
          spannerCertificatesService,
        )

      val directMeasurement = createdMeasurements.directMeasurement

      val updateTimeFieldValue: FieldValue =
        FieldValue.of(
          FieldValue.Attribute.PRIMITIVE,
          "${Timestamps.toNanos(directMeasurement.updateTime)}",
        )
      val externalMeasurementConsumerIdFieldValue: FieldValue =
        FieldValue.of(
          FieldValue.Attribute.PRIMITIVE,
          "${directMeasurement.externalMeasurementConsumerId}",
        )
      val externalMeasurementIdFieldValue: FieldValue =
        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${directMeasurement.externalMeasurementId}")
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll())
          .thenReturn(
            listOf(
              FieldValueList.of(
                mutableListOf(
                  updateTimeFieldValue,
                  externalMeasurementConsumerIdFieldValue,
                  externalMeasurementIdFieldValue,
                ),
                LATEST_MEASUREMENT_FIELD_LIST,
              )
            )
          )
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val measurementsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenAnswer {
          val protoRows: ProtoRows = it.getArgument(0)
          assertThat(protoRows.serializedRowsList).hasSize(1)
          async {}
        }
      }

      val requisitionsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenAnswer {
          val protoRows: ProtoRows = it.getArgument(0)
          assertThat(protoRows.serializedRowsList).hasSize(1)
          async {}
        }
      }

      val computationParticipantsDataWriterMock: OperationalMetricsJob.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenAnswer {
            val protoRows: ProtoRows = it.getArgument(0)
            assertThat(protoRows.serializedRowsList).hasSize(3)
            async {}
          }
        }

      val latestMeasurementReadDataWriterMock: OperationalMetricsJob.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenAnswer {
            val protoRows: ProtoRows = it.getArgument(0)
            assertThat(protoRows.serializedRowsList).hasSize(1)
            async {}
          }
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

  @Test
  fun `job fails when bigquery append fails`() {
    runBlocking {
      createSucceededMeasurements(
        population,
        spannerDataProvidersService,
        spannerMeasurementsService,
        spannerAccountsService,
        spannerMeasurementConsumersService,
        spannerRequisitionsService,
        spannerCertificatesService,
      )

      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val measurementsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenThrow(IllegalStateException(""))
      }

      val requisitionsDataWriterMock: OperationalMetricsJob.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenReturn(async {})
      }

      val computationParticipantsDataWriterMock: OperationalMetricsJob.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenReturn(async {})
        }

      val latestMeasurementReadDataWriterMock: OperationalMetricsJob.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenReturn(async {})
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

      assertFailsWith<IllegalStateException> { operationalMetricsJob.execute() }
    }
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

    private data class CreatedMeasurements(
      val computationParticipantMeasurement: Measurement,
      val directMeasurement: Measurement,
    )

    private suspend fun createSucceededMeasurements(
      population: Population,
      spannerDataProvidersService: SpannerDataProvidersService,
      spannerMeasurementsService: SpannerMeasurementsService,
      spannerAccountsService: SpannerAccountsService,
      spannerMeasurementConsumersService: SpannerMeasurementConsumersService,
      spannerRequisitionsService: SpannerRequisitionsService,
      spannerCertificatesService: SpannerCertificatesService,
    ): CreatedMeasurements {
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

      val computationParticipantMeasurement =
        spannerMeasurementsService.createMeasurement(computationParticipantMeasurementRequest)
      var directMeasurement = spannerMeasurementsService.createMeasurement(directMeasurementRequest)

      directMeasurement =
        spannerMeasurementsService
          .streamMeasurements(
            streamMeasurementsRequest { measurementView = Measurement.View.COMPUTATION }
          )
          .filter { it.providedMeasurementId == directMeasurement.providedMeasurementId }
          .first()

      spannerRequisitionsService.fulfillRequisition(
        fulfillRequisitionRequest {
          externalRequisitionId = directMeasurement.requisitionsList[0].externalRequisitionId
          nonce = 123L
          directParams =
            FulfillRequisitionRequestKt.directRequisitionParams {
              externalDataProviderId = directMeasurement.requisitionsList[0].externalDataProviderId
              encryptedData = ByteString.copyFromUtf8("encryptedData")
              externalCertificateId =
                directMeasurement.requisitionsList[0].details.externalCertificateId
              apiVersion = PUBLIC_API_VERSION
            }
        }
      )

      val aggregatorDuchyId = Population.AGGREGATOR_DUCHY.externalDuchyId
      val aggregatorCertificate =
        population.createDuchyCertificate(spannerCertificatesService, aggregatorDuchyId)
      spannerMeasurementsService.setMeasurementResult(
        setMeasurementResultRequest {
          externalComputationId = computationParticipantMeasurement.externalComputationId
          externalAggregatorDuchyId = aggregatorDuchyId
          externalAggregatorCertificateId = aggregatorCertificate.externalCertificateId
          resultPublicKey = ByteString.copyFromUtf8("resultPublicKey")
          encryptedResult = ByteString.copyFromUtf8("encryptedResult")
          publicApiVersion = Version.V2_ALPHA.string
        }
      )

      val measurements =
        spannerMeasurementsService
          .streamMeasurements(
            streamMeasurementsRequest { measurementView = Measurement.View.COMPUTATION }
          )
          .toList()

      return CreatedMeasurements(
        computationParticipantMeasurement = measurements[1],
        directMeasurement = measurements[0],
      )
    }

    private const val API_VERSION = "v2alpha"
    private val PUBLIC_API_VERSION = Version.V2_ALPHA.string

    private val PUBLIC_API_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = ByteString.copyFromUtf8("key")
    }

    private val PUBLIC_API_MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.pack()
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
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
          Field.of("external_measurement_id", LegacySQLTypeName.INTEGER),
        )
      )
  }
}
