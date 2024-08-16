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

package org.wfanet.measurement.kingdom.deploy.gcloud.job

import com.google.api.core.ApiFutures
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.TableResult
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.cloud.bigquery.storage.v1.ProtoSchema
import com.google.cloud.bigquery.storage.v1.StreamWriter
import com.google.cloud.bigquery.storage.v1.TableSchema
import com.google.cloud.bigquery.storage.v1.WriteStream
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import com.google.rpc.Code
import com.google.rpc.Status
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
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
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.bigquerytables.ComputationParticipantsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.LatestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementType
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.RequisitionsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.computationParticipantsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.latestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.measurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.requisitionsTableRow
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

  private val bigQueryWriteClientMock: BigQueryWriteClient = mock { bigQueryWriteClient ->
    val writeStreamMock: WriteStream = mock { writeStream ->
      whenever(writeStream.tableSchema).thenReturn(TableSchema.getDefaultInstance())
      whenever(writeStream.location).thenReturn("LOCATION")
    }
    whenever(bigQueryWriteClient.settings).thenReturn(BigQueryWriteSettings.newBuilder().build())
    whenever(
        bigQueryWriteClient.getWriteStream(ArgumentMatchers.isA(GetWriteStreamRequest::class.java))
      )
      .thenReturn(writeStreamMock)
  }

  private lateinit var measurementsStreamWriterMock: StreamWriter
  private lateinit var requisitionsStreamWriterMock: StreamWriter
  private lateinit var computationParticipantsStreamWriterMock: StreamWriter
  private lateinit var latestMeasurementReadStreamWriterMock: StreamWriter

  private lateinit var streamWriterFactoryTestImpl: StreamWriterFactory

  @Before
  fun init() {
    measurementsClient = MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel)

    measurementsStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    requisitionsStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    computationParticipantsStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    latestMeasurementReadStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    streamWriterFactoryTestImpl =
      StreamWriterFactory {
        _: String,
        _: String,
        tableId: String,
        _: BigQueryWriteClient,
        _: ProtoSchema ->
        when (tableId) {
          MEASUREMENTS_TABLE_ID -> measurementsStreamWriterMock
          REQUISITIONS_TABLE_ID -> requisitionsStreamWriterMock
          COMPUTATION_PARTICIPANTS_TABLE_ID -> computationParticipantsStreamWriterMock
          LATEST_MEASUREMENT_READ_TABLE_ID -> latestMeasurementReadStreamWriterMock
          else -> mock {}
        }
      }
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

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<ProtoRows>()) {
      verify(measurementsStreamWriterMock).append(capture())

      val protoRows: ProtoRows = allValues.first()
      assertThat(protoRows.serializedRowsList).hasSize(2)

      val computationMeasurementTableRow =
        MeasurementsTableRow.parseFrom(protoRows.serializedRowsList[1])
      assertThat(computationMeasurementTableRow)
        .isEqualTo(
          measurementsTableRow {
            measurementConsumerId =
              externalIdToApiId(computationMeasurement.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(computationMeasurement.externalMeasurementId)
            isDirect = false
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = MeasurementsTableRow.State.SUCCEEDED
            createTime = computationMeasurement.createTime
            updateTime = computationMeasurement.updateTime
            completionDurationSeconds =
              Durations.toSeconds(
                Timestamps.between(
                  computationMeasurement.createTime,
                  computationMeasurement.updateTime,
                )
              )
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
          }
        )

      val directMeasurementTableRow =
        MeasurementsTableRow.parseFrom(protoRows.serializedRowsList[0])
      assertThat(directMeasurementTableRow)
        .isEqualTo(
          measurementsTableRow {
            measurementConsumerId =
              externalIdToApiId(directMeasurement.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(directMeasurement.externalMeasurementId)
            isDirect = true
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = MeasurementsTableRow.State.SUCCEEDED
            createTime = directMeasurement.createTime
            updateTime = directMeasurement.updateTime
            completionDurationSeconds =
              Durations.toSeconds(
                Timestamps.between(directMeasurement.createTime, directMeasurement.updateTime)
              )
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
          }
        )
    }

    with(argumentCaptor<ProtoRows>()) {
      verify(requisitionsStreamWriterMock).append(capture())

      val protoRows: ProtoRows = allValues.first()
      assertThat(protoRows.serializedRowsList).hasSize(2)

      val computationRequisitionTableRow =
        RequisitionsTableRow.parseFrom(protoRows.serializedRowsList[1])
      assertThat(computationRequisitionTableRow)
        .isEqualTo(
          requisitionsTableRow {
            measurementConsumerId =
              externalIdToApiId(computationMeasurement.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(computationMeasurement.externalMeasurementId)
            requisitionId =
              externalIdToApiId(computationMeasurement.requisitionsList[0].externalRequisitionId)
            dataProviderId =
              externalIdToApiId(computationMeasurement.requisitionsList[0].externalDataProviderId)
            isDirect = false
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = RequisitionsTableRow.State.FULFILLED
            createTime = computationMeasurement.createTime
            updateTime = computationMeasurement.requisitionsList[0].updateTime
            completionDurationSeconds =
              Durations.toSeconds(
                Timestamps.between(
                  computationMeasurement.createTime,
                  computationMeasurement.requisitionsList[0].updateTime,
                )
              )
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
          }
        )

      val directRequisitionTableRow =
        RequisitionsTableRow.parseFrom(protoRows.serializedRowsList[0])
      assertThat(directRequisitionTableRow)
        .isEqualTo(
          requisitionsTableRow {
            measurementConsumerId =
              externalIdToApiId(directMeasurement.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(directMeasurement.externalMeasurementId)
            requisitionId =
              externalIdToApiId(directMeasurement.requisitionsList[0].externalRequisitionId)
            dataProviderId =
              externalIdToApiId(directMeasurement.requisitionsList[0].externalDataProviderId)
            isDirect = true
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = RequisitionsTableRow.State.FULFILLED
            createTime = directMeasurement.createTime
            updateTime = directMeasurement.requisitionsList[0].updateTime
            completionDurationSeconds =
              Durations.toSeconds(
                Timestamps.between(
                  directMeasurement.createTime,
                  directMeasurement.requisitionsList[0].updateTime,
                )
              )
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
          }
        )
    }

    with(argumentCaptor<ProtoRows>()) {
      verify(computationParticipantsStreamWriterMock).append(capture())

      val protoRows: ProtoRows = allValues.first()
      assertThat(protoRows.serializedRowsList).hasSize(3)

      for (serializedProtoRow in protoRows.serializedRowsList) {
        val computationParticipantsTableRow =
          ComputationParticipantsTableRow.parseFrom(serializedProtoRow)
        for (computationParticipant in computationMeasurement.computationParticipantsList) {
          if (computationParticipant.externalDuchyId == computationParticipantsTableRow.duchyId) {
            assertThat(computationParticipantsTableRow)
              .isEqualTo(
                computationParticipantsTableRow {
                  measurementConsumerId =
                    externalIdToApiId(computationMeasurement.externalMeasurementConsumerId)
                  measurementId = externalIdToApiId(computationMeasurement.externalMeasurementId)
                  computationId = externalIdToApiId(computationMeasurement.externalComputationId)
                  duchyId = computationParticipantsTableRow.duchyId
                  protocol = ComputationParticipantsTableRow.Protocol.PROTOCOL_UNSPECIFIED
                  measurementType = MeasurementType.REACH_AND_FREQUENCY
                  state = ComputationParticipantsTableRow.State.READY
                  createTime = computationMeasurement.createTime
                  updateTime = computationParticipant.updateTime
                  completionDurationSeconds =
                    Durations.toSeconds(
                      Timestamps.between(
                        computationMeasurement.createTime,
                        computationParticipant.updateTime,
                      )
                    )
                  completionDurationSecondsSquared =
                    completionDurationSeconds * completionDurationSeconds
                }
              )
          }
        }
      }
    }

    with(argumentCaptor<ProtoRows>()) {
      verify(latestMeasurementReadStreamWriterMock).append(capture())

      val protoRows: ProtoRows = allValues.first()
      assertThat(protoRows.serializedRowsList).hasSize(1)

      val latestMeasurementReadTableRow =
        LatestMeasurementReadTableRow.parseFrom(protoRows.serializedRowsList.first())
      assertThat(latestMeasurementReadTableRow)
        .isEqualTo(
          latestMeasurementReadTableRow {
            updateTime = Timestamps.toNanos(computationMeasurement.updateTime)
            externalMeasurementConsumerId = computationMeasurement.externalMeasurementConsumerId
            externalMeasurementId = computationMeasurement.externalMeasurementId
          }
        )
    }
  }

  @Test
  fun `job does not create protos for requisitions that are incomplete`() = runBlocking {
    whenever(measurementsMock.streamMeasurements(any()))
      .thenReturn(
        flowOf(
          DIRECT_MEASUREMENT,
          COMPUTATION_MEASUREMENT.copy {
            requisitions.clear()
            requisitions +=
              COMPUTATION_MEASUREMENT.requisitionsList[0].copy {
                state = Requisition.State.UNFULFILLED
              }
          },
        )
      )

    val directMeasurement = DIRECT_MEASUREMENT

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<ProtoRows>()) {
      verify(requisitionsStreamWriterMock).append(capture())

      val protoRows: ProtoRows = allValues.first()
      assertThat(protoRows.serializedRowsList).hasSize(1)

      val directRequisitionTableRow =
        RequisitionsTableRow.parseFrom(protoRows.serializedRowsList[0])
      assertThat(directRequisitionTableRow)
        .isEqualTo(
          requisitionsTableRow {
            measurementConsumerId =
              externalIdToApiId(directMeasurement.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(directMeasurement.externalMeasurementId)
            requisitionId =
              externalIdToApiId(directMeasurement.requisitionsList[0].externalRequisitionId)
            dataProviderId =
              externalIdToApiId(directMeasurement.requisitionsList[0].externalDataProviderId)
            isDirect = true
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = RequisitionsTableRow.State.FULFILLED
            createTime = directMeasurement.createTime
            updateTime = directMeasurement.requisitionsList[0].updateTime
            completionDurationSeconds =
              Durations.toSeconds(
                Timestamps.between(
                  directMeasurement.createTime,
                  directMeasurement.requisitionsList[0].updateTime,
                )
              )
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
          }
        )
    }
  }

  @Test
  fun `job does not create protos for computation participants that are incomplete`() =
    runBlocking {
      whenever(measurementsMock.streamMeasurements(any()))
        .thenReturn(
          flowOf(
            COMPUTATION_MEASUREMENT.copy {
              computationParticipants.clear()
              computationParticipants += COMPUTATION_MEASUREMENT.computationParticipantsList[0]
              computationParticipants += COMPUTATION_MEASUREMENT.computationParticipantsList[1]
              computationParticipants +=
                COMPUTATION_MEASUREMENT.computationParticipantsList[2].copy {
                  state = ComputationParticipant.State.CREATED
                }
            }
          )
        )

      val computationMeasurement = COMPUTATION_MEASUREMENT

      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      operationalMetricsExport.execute()

      with(argumentCaptor<ProtoRows>()) {
        verify(computationParticipantsStreamWriterMock).append(capture())

        val protoRows: ProtoRows = allValues.first()
        assertThat(protoRows.serializedRowsList).hasSize(2)

        for (serializedProtoRow in protoRows.serializedRowsList) {
          val computationParticipantsTableRow =
            ComputationParticipantsTableRow.parseFrom(serializedProtoRow)
          for (computationParticipant in computationMeasurement.computationParticipantsList) {
            if (computationParticipant.externalDuchyId == computationParticipantsTableRow.duchyId) {
              assertThat(computationParticipantsTableRow)
                .isEqualTo(
                  computationParticipantsTableRow {
                    measurementConsumerId =
                      externalIdToApiId(computationMeasurement.externalMeasurementConsumerId)
                    measurementId = externalIdToApiId(computationMeasurement.externalMeasurementId)
                    computationId = externalIdToApiId(computationMeasurement.externalComputationId)
                    duchyId = computationParticipantsTableRow.duchyId
                    protocol = ComputationParticipantsTableRow.Protocol.PROTOCOL_UNSPECIFIED
                    measurementType = MeasurementType.REACH_AND_FREQUENCY
                    state = ComputationParticipantsTableRow.State.READY
                    createTime = computationMeasurement.createTime
                    updateTime = computationParticipant.updateTime
                    completionDurationSeconds =
                      Durations.toSeconds(
                        Timestamps.between(
                          computationMeasurement.createTime,
                          computationParticipant.updateTime,
                        )
                      )
                    completionDurationSecondsSquared =
                      completionDurationSeconds * completionDurationSeconds
                  }
                )
            }
          }
        }
      }
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
              measurementView = Measurement.View.FULL
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
              limit = 3000
            }
          )
        flowOf(COMPUTATION_MEASUREMENT)
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      operationalMetricsExport.execute()
    }

  @Test
  fun `job recreates streamwriter if it is closed`() = runBlocking {
    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    whenever(measurementsStreamWriterMock.isClosed).thenReturn(true)
    whenever(measurementsStreamWriterMock.isUserClosed).thenReturn(false)

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()
  }

  @Test
  fun `job succeeds when bigquery append fails with internal only once`() = runBlocking {
    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    whenever(measurementsStreamWriterMock.append(any()))
      .thenReturn(
        ApiFutures.immediateFuture(
          AppendRowsResponse.newBuilder()
            .setError(Status.newBuilder().setCode(Code.INTERNAL_VALUE).build())
            .build()
        )
      )
      .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()
  }

  @Test
  fun `job fails when bigquery append fails with internal too many times`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(measurementsStreamWriterMock.append(any()))
        .thenReturn(
          ApiFutures.immediateFuture(
            AppendRowsResponse.newBuilder()
              .setError(Status.newBuilder().setCode(Code.INTERNAL_VALUE).build())
              .build()
          )
        )

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<IllegalStateException> { operationalMetricsExport.execute() }
    }
  }

  @Test
  fun `job fails when bigquery append fails with invalid argument`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(measurementsStreamWriterMock.append(any()))
        .thenReturn(
          ApiFutures.immediateFuture(
            AppendRowsResponse.newBuilder()
              .setError(Status.newBuilder().setCode(Code.INVALID_ARGUMENT_VALUE).build())
              .build()
          )
        )

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<IllegalStateException> { operationalMetricsExport.execute() }
    }
  }

  @Test
  fun `job fails when bigquery append fails with append serialization error`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(measurementsStreamWriterMock.append(any()))
        .thenThrow(AppendSerializationError(0, "", "", mapOf()))

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          latestMeasurementReadTableId = LATEST_MEASUREMENT_READ_TABLE_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantsTableId = COMPUTATION_PARTICIPANTS_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<AppendSerializationError> { operationalMetricsExport.execute() }
    }
  }

  companion object {
    private const val PROJECT_ID = "project"
    private const val DATASET_ID = "dataset"
    private const val MEASUREMENTS_TABLE_ID = "measurements"
    private const val REQUISITIONS_TABLE_ID = "requisitions"
    private const val COMPUTATION_PARTICIPANTS_TABLE_ID = "computation_participants"
    private const val LATEST_MEASUREMENT_READ_TABLE_ID = "latest_measurement_read"

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
          state = Requisition.State.FULFILLED
          updateTime = timestamp {
            seconds = 500
            nanos = 100
          }
        }

        computationParticipants += computationParticipant {
          externalDuchyId = "0"
          state = ComputationParticipant.State.READY
          updateTime = timestamp { seconds = 300 }
        }
        computationParticipants += computationParticipant {
          externalDuchyId = "1"
          state = ComputationParticipant.State.READY
          updateTime = timestamp { seconds = 400 }
        }
        computationParticipants += computationParticipant {
          externalDuchyId = "2"
          state = ComputationParticipant.State.READY
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
          state = Requisition.State.FULFILLED
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
