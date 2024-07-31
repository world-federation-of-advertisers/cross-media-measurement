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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantData
import org.wfanet.measurement.internal.kingdom.LatestMeasurementRead
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementData
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionData
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

@RunWith(JUnit4::class)
class OperationalMetricsExportTest {
  private val measurementsMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase = mockService {
    onBlocking { streamMeasurements(any()) }
      .thenReturn(flowOf(DIRECT_MEASUREMENT, COMPUTATION_MEASUREMENT))
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(measurementsMock) }

  private lateinit var measurementsClient: MeasurementsGrpcKt.MeasurementsCoroutineStub

  @Before
  fun init() {
    measurementsClient = MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `job successfully creates protos for appending to streams`() = runBlocking {
    val computationMeasurement = COMPUTATION_MEASUREMENT
    val directMeasurement = DIRECT_MEASUREMENT

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val measurementsDataWriterMock: OperationalMetricsExport.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any())).thenAnswer {
        val protoRows: ProtoRows = it.getArgument(0)
        assertThat(protoRows.serializedRowsList).hasSize(2)

        val computationMeasurementData = MeasurementData.parseFrom(protoRows.serializedRowsList[1])
        assertThat(computationMeasurementData.measurementConsumerId)
          .isEqualTo(externalIdToApiId(computationMeasurement.externalMeasurementConsumerId))
        assertThat(computationMeasurementData.measurementId)
          .isEqualTo(externalIdToApiId(computationMeasurement.externalMeasurementId))
        assertThat(computationMeasurementData.isDirect).isFalse()
        assertThat(computationMeasurementData.measurementType).isEqualTo("REACH_AND_FREQUENCY")
        assertThat(computationMeasurementData.state).isEqualTo("SUCCEEDED")
        assertThat(computationMeasurementData.createTime)
          .isEqualTo(Timestamps.toMicros(computationMeasurement.createTime))
        assertThat(computationMeasurementData.updateTime)
          .isEqualTo(Timestamps.toMicros(computationMeasurement.updateTime))
        assertThat(computationMeasurementData.updateTimeMinusCreateTime)
          .isEqualTo(
            Durations.toMillis(
              Timestamps.between(
                computationMeasurement.createTime,
                computationMeasurement.updateTime,
              )
            )
          )
        assertThat(computationMeasurementData.updateTimeMinusCreateTimeSquared)
          .isEqualTo(
            computationMeasurementData.updateTimeMinusCreateTime *
              computationMeasurementData.updateTimeMinusCreateTime
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

    val requisitionsDataWriterMock: OperationalMetricsExport.DataWriter = mock { dataWriter ->
      whenever(dataWriter.appendRows(any())).thenAnswer {
        val protoRows: ProtoRows = it.getArgument(0)
        assertThat(protoRows.serializedRowsList).hasSize(2)

        val computationParticipantRequisitionData =
          RequisitionData.parseFrom(protoRows.serializedRowsList[1])
        assertThat(computationParticipantRequisitionData.measurementConsumerId)
          .isEqualTo(externalIdToApiId(computationMeasurement.externalMeasurementConsumerId))
        assertThat(computationParticipantRequisitionData.measurementId)
          .isEqualTo(externalIdToApiId(computationMeasurement.externalMeasurementId))
        assertThat(computationParticipantRequisitionData.requisitionId)
          .isEqualTo(
            externalIdToApiId(computationMeasurement.requisitionsList[0].externalRequisitionId)
          )
        assertThat(computationParticipantRequisitionData.dataProviderId)
          .isEqualTo(
            externalIdToApiId(computationMeasurement.requisitionsList[0].externalDataProviderId)
          )
        assertThat(computationParticipantRequisitionData.isDirect).isFalse()
        assertThat(computationParticipantRequisitionData.measurementType)
          .isEqualTo("REACH_AND_FREQUENCY")
        assertThat(computationParticipantRequisitionData.state).isEqualTo("UNFULFILLED")
        assertThat(computationParticipantRequisitionData.createTime)
          .isEqualTo(Timestamps.toMicros(computationMeasurement.createTime))
        assertThat(computationParticipantRequisitionData.updateTime)
          .isEqualTo(Timestamps.toMicros(computationMeasurement.requisitionsList[0].updateTime))
        assertThat(computationParticipantRequisitionData.updateTimeMinusCreateTime)
          .isEqualTo(
            Durations.toMillis(
              Timestamps.between(
                computationMeasurement.createTime,
                computationMeasurement.requisitionsList[0].updateTime,
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
        assertThat(directRequisitionData.state).isEqualTo("UNFULFILLED")
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

    val computationParticipantsDataWriterMock: OperationalMetricsExport.DataWriter =
      mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenAnswer {
          val protoRows: ProtoRows = it.getArgument(0)
          assertThat(protoRows.serializedRowsList).hasSize(3)

          for (serializedProtoRow in protoRows.serializedRowsList) {
            val computationParticipantData =
              ComputationParticipantData.parseFrom(serializedProtoRow)
            for (computationParticipant in computationMeasurement.computationParticipantsList) {
              if (computationParticipant.externalDuchyId == computationParticipantData.duchyId) {
                assertThat(computationParticipantData.measurementConsumerId)
                  .isEqualTo(
                    externalIdToApiId(computationMeasurement.externalMeasurementConsumerId)
                  )
                assertThat(computationParticipantData.measurementId)
                  .isEqualTo(externalIdToApiId(computationMeasurement.externalMeasurementId))
                assertThat(computationParticipantData.computationId)
                  .isEqualTo(externalIdToApiId(computationMeasurement.externalComputationId))
                assertThat(computationParticipantData.protocol).isEqualTo("PROTOCOL_NOT_SET")
                assertThat(computationParticipantData.measurementType)
                  .isEqualTo("REACH_AND_FREQUENCY")
                assertThat(computationParticipantData.state).isEqualTo("CREATED")
                assertThat(computationParticipantData.createTime)
                  .isEqualTo(Timestamps.toMicros(computationMeasurement.createTime))
                assertThat(computationParticipantData.updateTime)
                  .isEqualTo(Timestamps.toMicros(computationParticipant.updateTime))
                assertThat(computationParticipantData.updateTimeMinusCreateTime)
                  .isEqualTo(
                    Durations.toMillis(
                      Timestamps.between(
                        computationMeasurement.createTime,
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

    val latestMeasurementReadDataWriterMock: OperationalMetricsExport.DataWriter =
      mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenAnswer {
          val protoRows: ProtoRows = it.getArgument(0)
          assertThat(protoRows.serializedRowsList).hasSize(1)

          val latestMeasurementRead =
            LatestMeasurementRead.parseFrom(protoRows.serializedRowsList[0])
          assertThat(latestMeasurementRead.updateTime)
            .isEqualTo(Timestamps.toNanos(computationMeasurement.updateTime))
          assertThat(latestMeasurementRead.externalMeasurementConsumerId)
            .isEqualTo(computationMeasurement.externalMeasurementConsumerId)
          assertThat(latestMeasurementRead.externalMeasurementId)
            .isEqualTo(computationMeasurement.externalMeasurementId)
          async {}
        }
      }

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        bigQuery = bigQueryMock,
        datasetId = "dataset",
        latestMeasurementReadTableId = "table",
        measurementsDataWriter = measurementsDataWriterMock,
        requisitionsDataWriter = requisitionsDataWriterMock,
        computationParticipantsDataWriter = computationParticipantsDataWriterMock,
        latestMeasurementReadDataWriter = latestMeasurementReadDataWriterMock,
      )

    operationalMetricsExport.execute()
  }

  @Test
  fun `job can process the next batch of measurements without starting at the beginning`() =
    runBlocking {
      val directMeasurement = DIRECT_MEASUREMENT

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

      whenever(measurementsMock.streamMeasurements(any())).thenAnswer {
        val streamMeasurementsRequest: StreamMeasurementsRequest = it.getArgument(0)
        assertThat(streamMeasurementsRequest)
          .ignoringRepeatedFieldOrder()
          .isEqualTo(
            streamMeasurementsRequest {
              measurementView = Measurement.View.COMPUTATION
              filter =
                StreamMeasurementsRequestKt.filter {
                  states += Measurement.State.SUCCEEDED
                  states += Measurement.State.FAILED
                  after =
                    StreamMeasurementsRequestKt.FilterKt.after {
                      updateTime = directMeasurement.updateTime
                      measurement = measurementKey {
                        externalMeasurementConsumerId =
                          directMeasurement.externalMeasurementConsumerId
                        externalMeasurementId = directMeasurement.externalMeasurementId
                      }
                    }
                }
              limit = 1000
            }
          )
        flowOf(COMPUTATION_MEASUREMENT)
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val measurementsDataWriterMock: OperationalMetricsExport.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenReturn(async {})
      }

      val requisitionsDataWriterMock: OperationalMetricsExport.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenReturn(async {})
      }

      val computationParticipantsDataWriterMock: OperationalMetricsExport.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenReturn(async {})
        }

      val latestMeasurementReadDataWriterMock: OperationalMetricsExport.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenReturn(async {})
        }

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQueryMock,
          datasetId = "dataset",
          latestMeasurementReadTableId = "table",
          measurementsDataWriter = measurementsDataWriterMock,
          requisitionsDataWriter = requisitionsDataWriterMock,
          computationParticipantsDataWriter = computationParticipantsDataWriterMock,
          latestMeasurementReadDataWriter = latestMeasurementReadDataWriterMock,
        )

      operationalMetricsExport.execute()
    }

  @Test
  fun `job fails when bigquery append fails`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val measurementsDataWriterMock: OperationalMetricsExport.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenThrow(IllegalStateException(""))
      }

      val requisitionsDataWriterMock: OperationalMetricsExport.DataWriter = mock { dataWriter ->
        whenever(dataWriter.appendRows(any())).thenReturn(async {})
      }

      val computationParticipantsDataWriterMock: OperationalMetricsExport.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenReturn(async {})
        }

      val latestMeasurementReadDataWriterMock: OperationalMetricsExport.DataWriter =
        mock { dataWriter ->
          whenever(dataWriter.appendRows(any())).thenReturn(async {})
        }

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQueryMock,
          datasetId = "dataset",
          latestMeasurementReadTableId = "table",
          measurementsDataWriter = measurementsDataWriterMock,
          requisitionsDataWriter = requisitionsDataWriterMock,
          computationParticipantsDataWriter = computationParticipantsDataWriterMock,
          latestMeasurementReadDataWriter = latestMeasurementReadDataWriterMock,
        )

      assertFailsWith<IllegalStateException> { operationalMetricsExport.execute() }
    }
  }

  companion object {
    private const val API_VERSION = "v2alpha"

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
      externalMeasurementConsumerId = 1234
      externalMeasurementConsumerCertificateId = 1234
      details =
        MeasurementKt.details {
          apiVersion = API_VERSION
          measurementSpec = PUBLIC_API_MEASUREMENT_SPEC.toByteString()
          measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
          measurementSpecSignatureAlgorithmOid = "2.9999"
        }
    }

    private val COMPUTATION_MEASUREMENT =
      MEASUREMENT.copy {
        externalMeasurementId = 123
        externalComputationId = 124
        providedMeasurementId = "computation-participant"
        state = Measurement.State.SUCCEEDED
        createTime = timestamp { seconds = 200 }
        updateTime = timestamp {
          seconds = 300
          nanos = 100
        }
        details =
          details.copy {
            protocolConfig = protocolConfig {
              liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
            }
          }

        requisitions += requisition {
          externalDataProviderId = 432
          externalRequisitionId = 433
          state = Requisition.State.UNFULFILLED
          updateTime = timestamp {
            seconds = 500
            nanos = 100
          }
        }

        computationParticipants += computationParticipant {
          externalDuchyId = "0"
          state = ComputationParticipant.State.CREATED
          updateTime = timestamp { seconds = 300 }
        }
        computationParticipants += computationParticipant {
          externalDuchyId = "1"
          state = ComputationParticipant.State.CREATED
          updateTime = timestamp { seconds = 400 }
        }
        computationParticipants += computationParticipant {
          externalDuchyId = "2"
          state = ComputationParticipant.State.CREATED
          updateTime = timestamp { seconds = 500 }
        }
      }

    private val DIRECT_MEASUREMENT =
      MEASUREMENT.copy {
        externalMeasurementId = 321
        externalComputationId = 0
        providedMeasurementId = "direct"
        state = Measurement.State.SUCCEEDED
        createTime = timestamp { seconds = 200 }
        updateTime = timestamp {
          seconds = 220
          nanos = 200
        }
        details =
          details.copy {
            protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }

        requisitions += requisition {
          externalDataProviderId = 432
          externalRequisitionId = 437
          state = Requisition.State.UNFULFILLED
          updateTime = timestamp {
            seconds = 600
            nanos = 100
          }
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
