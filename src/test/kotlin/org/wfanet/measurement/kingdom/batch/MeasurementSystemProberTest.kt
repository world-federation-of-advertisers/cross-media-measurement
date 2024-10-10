package org.wfanet.measurement.kingdom.batch

import com.google.common.truth.Truth
import java.io.File
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.any
import org.mockito.Mockito.`when`
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.ListMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService

@RunWith(JUnit4::class)
class MeasurementSystemProberTest {
  private val measurementConsumerName = "measurementConsumers/some-mc"
  private val dataProviderNames = listOf("dataProviders/some-dp")
  private val apiAuthenticationKey = "some-api-key"
  private val privateKeyDerFile = File("some-private-key.der")
  private val measurementLookbackDuration = Duration.ofDays(1)
  private val durationBetweenMeasurement = Duration.ofDays(1)

  private val measurementConsumersMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
    }

  private val measurementsMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase = mockService {
    onBlocking { listMeasurements(any()) }.thenReturn(LIST_MEASUREMENTS_RESPONSE)
  }

  private val dataProvidersMock: DataProvidersGrpcKt.DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
  }

  private val eventGroupsMock: EventGroupsGrpcKt.EventGroupsCoroutineImplBase = mockService {
    onBlocking { listEventGroups(any()) }.thenReturn(LIST_EVENT_GROUPS_RESPONSE)
  }

  private val requisitionsMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }.thenReturn(LIST_REQUISITIONS_RESPONSE)
  }

  private lateinit var measurementConsumersClient: MeasurementConsumersCoroutineStub
  private lateinit var measurementsClient: MeasurementsCoroutineStub
  private lateinit var dataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub
  private lateinit var eventGroupsClient: EventGroupsGrpcKt.EventGroupsCoroutineStub
  private lateinit var requisitionsClient: RequisitionsGrpcKt.RequisitionsCoroutineStub
  private lateinit var prober: MeasurementSystemProber

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementConsumersMock)
    addService(measurementsMock)
    addService(dataProvidersMock)
    addService(eventGroupsMock)
    addService(requisitionsMock)
  }

  @Before
  fun initServices() {
    measurementConsumersClient = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
    measurementsClient = MeasurementsCoroutineStub(grpcTestServerRule.channel)
    dataProvidersClient = DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel)
    eventGroupsClient = EventGroupsGrpcKt.EventGroupsCoroutineStub(grpcTestServerRule.channel)
    requisitionsClient = RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
    prober =
      MeasurementSystemProber(
        measurementConsumerName,
        dataProviderNames,
        apiAuthenticationKey,
        privateKeyDerFile,
        measurementLookbackDuration,
        durationBetweenMeasurement,
        measurementConsumersClient,
        measurementsClient,
        dataProvidersClient,
        eventGroupsClient,
        requisitionsClient,
      )
  }

  @Test
  fun `run creates a new prober measurement when there are no previous prober measurement`(): Unit =
    runBlocking {
      `when`(measurementsMock.listMeasurements(any())).thenReturn(listMeasurementsResponse {})
      val measurement = prober.run()
      Truth.assertThat(measurement).isNotNull()
    }

  @Test
  fun `run creates a new prober measurement when most recent issued prober measurement is finished`():
    Unit = runBlocking {
    `when`(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += FINISHED_MEASUREMENT })
    val measurement = prober.run()
    Truth.assertThat(measurement).isNotNull()
  }

  @Test
  fun `run does not create a new prober measurement when the most recent issued prober measurement is not finished`():
    Unit = runBlocking {
    `when`(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += UNFINISHED_MEASUREMENT })
    val measurement = prober.run()
    Truth.assertThat(measurement).isNull()
  }

  companion object {
    private val MEASUREMENT_CONSUMER: MeasurementConsumer = measurementConsumer {}
    private val LIST_MEASUREMENTS_RESPONSE: ListMeasurementsResponse = listMeasurementsResponse {}
    private val DATA_PROVIDER: DataProvider = dataProvider {}
    private val LIST_EVENT_GROUPS_RESPONSE: ListEventGroupsResponse = listEventGroupsResponse {}
    private val LIST_REQUISITIONS_RESPONSE: ListRequisitionsResponse = listRequisitionsResponse {}

    private val FINISHED_MEASUREMENT = measurement { state = Measurement.State.SUCCEEDED }
    private val UNFINISHED_MEASUREMENT = measurement { state = Measurement.State.COMPUTING }
  }
}
