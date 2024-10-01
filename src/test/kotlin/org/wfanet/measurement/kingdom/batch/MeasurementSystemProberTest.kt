package org.wfanet.measurement.kingdom.batch

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status

import io.grpc.StatusException
import java.io.File
import java.security.PrivateKey

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlinx.coroutines.runBlocking

import org.mockito.Mockito.*

import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub

import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class MeasurementSystemProberTest {
  private val measurementConsumerName = "measurementConsumers/some-mc"
  private val dataProviderNames = listOf("dataProviders/some-dp")
  private val apiAuthenticationKey = "some-api-key"
  private val privateKeyDerFile = File("some-private-key.der") // Replace with actual file
  private val measurementLookbackDuration = Duration.ofDays(1)
  private val durationBetweenMeasurement = Duration.ofDays(1)

  private val measurementConsumersStub: MeasurementConsumersCoroutineStub =
    mock(MeasurementConsumersCoroutineStub::class.java)
  private val measurementsStub: MeasurementsCoroutineStub =
    mock(MeasurementsCoroutineStub::class.java)
  private val dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub =
    mock(DataProvidersGrpcKt.DataProvidersCoroutineStub::class.java)
  private val eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub =
    mock(EventGroupsGrpcKt.EventGroupsCoroutineStub::class.java)

  private val clock: Clock = mock(Clock::class.java)
  private val privateKey: PrivateKey = mock(PrivateKey::class.java)

  private lateinit var prober: MeasurementSystemProber

  private val fakeClock: Clock =
    Clock.fixed(Instant.parse("2024-09-30T10:00:00.00Z"), ZoneId.of("UTC"))

  @Before
  fun setUp() {
    prober =
      MeasurementSystemProber(
        measurementConsumerName,
        dataProviderNames,
        apiAuthenticationKey,
        privateKeyDerFile,
        measurementLookbackDuration,
        durationBetweenMeasurement,
        measurementConsumersStub,
        measurementsStub,
        dataProvidersStub,
        eventGroupsStub,
        fakeClock // Use the fake clock
      )
  }

  @Test
  fun `getLastUpdatedMeasurement returns null when list is empty`() = runBlocking {
    `when`(measurementsStub.listMeasurements(any()))
      .thenReturn(ListMeasurementsResponse.getDefaultInstance())

    val result = prober.getLastUpdatedMeasurement()

    verify(measurementsStub, times(1)).listMeasurements(any<ListMeasurementsRequest>())
    assertThat(result).isNull()
  }

  @Test
  fun `getLastUpdatedMeasurement returns the last measurement with pagination`() = runBlocking {
    val response1 =
      ListMeasurementsResponse.newBuilder()
      .addMeasurements(Measurement.newBuilder().setName("measurements/1").build())
      .setNextPageToken("token1")
      .build()
    val response2 =
      ListMeasurementsResponse.newBuilder()
        .addMeasurements(Measurement.newBuilder().setName("measurements/2").build())
        .build()


    `when`(measurementsStub.listMeasurements(any())).thenReturn(response1).thenReturn(response2)

    val result = prober.getLastUpdatedMeasurement()

    verify(measurementsStub, times(2)).listMeasurements(any<ListMeasurementsRequest>())
    assertThat(result?.name).isEqualTo("measurements/2")
  }

  @Test
  fun `getLastUpdatedMeasurement throws an exception when listMeasurements fails`() {
    runBlocking {
      `when`(measurementsStub.listMeasurements(any()))
        .thenThrow(StatusException(Status.INTERNAL))

      assertFailsWith<Exception> { prober.getLastUpdatedMeasurement() }
    }
  }
}
