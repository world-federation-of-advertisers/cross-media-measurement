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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.DoublePointData
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.io.File
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import kotlin.time.toKotlinDuration
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.kingdom.batch.MeasurementSystemProber
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessMeasurementSystemProberIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
) {

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule)

  private val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withPrincipalName(inProcessCmmsComponents.getMeasurementConsumerData().name)
  }

  private val publicDataProvidersClient by lazy {
    DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicRequisitionsClient by lazy {
    RequisitionsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private lateinit var prober: MeasurementSystemProber

  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader

  @Before
  fun startDaemons() {
    inProcessCmmsComponents.startDaemons()

    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
        .buildAndRegisterGlobal()

    initMeasurementSystemProber()
  }

  @Before
  fun initMeasurementSystemProber() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    prober =
      MeasurementSystemProber(
        measurementConsumerData.name,
        inProcessCmmsComponents.getDataProviderResourceNames(),
        measurementConsumerData.apiAuthenticationKey,
        PRIVATE_KEY_DER_FILE,
        MEASUREMENT_LOOKBACK_DURATION,
        DURATION_BETWEEN_MEASUREMENT,
        publicMeasurementConsumersClient,
        publicMeasurementsClient,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicRequisitionsClient,
        CLOCK,
      )
  }

  @After
  fun stopEdpSimulators() {
    inProcessCmmsComponents.stopEdpSimulators()
  }

  @After
  fun stopDuchyDaemons() {
    inProcessCmmsComponents.stopDuchyDaemons()
  }

  @Test
  fun `prober creates first two measurements`(): Unit = runBlocking {
    prober.run()

    delay(DURATION_BETWEEN_MEASUREMENT.toKotlinDuration())

    prober.run()

    val measurements = listMeasurements()
    assertThat(measurements.size).isEqualTo(2)

    val metricData: List<MetricData> = metricExporter.finishedMetricItems
    assertThat(metricData).hasSize(2)
    val metricNameToPoints: Map<String, List<DoublePointData>> =
      metricData.associateBy({ it.name }, { it.doubleGaugeData.points.map { point -> point } })
    assertThat(metricNameToPoints.keys)
      .containsExactly(
        LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME,
        LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME,
      )
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(measurements[0].updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND)

    val requisitions = getRequisitionsForMeasurement(measurements[0].name)
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(requisitions[0].updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(DATA_PROVIDER_ATTRIBUTE_KEY)
      )
      .isEqualTo(CanonicalRequisitionKey.fromName(requisitions[0].name)!!.dataProviderId)
  }

  private suspend fun listMeasurements(): List<Measurement> {
    var nextPageToken = ""
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    do {
      val response: ListMeasurementsResponse =
        try {
          publicMeasurementsClient
            .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
            .listMeasurements(
              listMeasurementsRequest {
                parent = measurementConsumerData.name
                this.pageSize = 1
                pageToken = nextPageToken
              }
            )
        } catch (e: StatusException) {
          throw Exception(
            "Unable to list measurements for measurement consumer ${measurementConsumerData.name}",
            e,
          )
        }
      if (response.measurementsList.isNotEmpty()) {
        return response.measurementsList
      }
      nextPageToken = response.nextPageToken
    } while (nextPageToken.isNotEmpty())
    return listOf()
  }

  private suspend fun getRequisitionsForMeasurement(measurementName: String): List<Requisition> {
    var nextPageToken = ""
    val requisitions = mutableListOf<Requisition>()
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    do {
      val response: ListRequisitionsResponse =
        try {
          publicRequisitionsClient
            .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
            .listRequisitions(
              listRequisitionsRequest {
                parent = measurementName
                pageToken = nextPageToken
              }
            )
        } catch (e: StatusException) {
          throw Exception("Unable to list requisitions for measurement $measurementName", e)
        }
      requisitions.addAll(response.requisitionsList)
      nextPageToken = response.nextPageToken
    } while (nextPageToken.isNotEmpty())
    return requisitions
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()
    private val PRIVATE_KEY_DER_FILE = SECRETS_DIR.resolve("${MC_DISPLAY_NAME}_cs_private.der")
    private val DURATION_BETWEEN_MEASUREMENT: Duration = Duration.ofMinutes(1)
    private val MEASUREMENT_LOOKBACK_DURATION = Duration.ofDays(1)
    private val CLOCK = Clock.systemUTC()

    private const val PROBER_NAMESPACE = "${Instrumentation.ROOT_NAMESPACE}.prober"
    private const val LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME =
      "${PROBER_NAMESPACE}.last_terminal_measurement.timestamp"
    private const val LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME =
      "${PROBER_NAMESPACE}.last_terminal_requisition.timestamp"
    private val DATA_PROVIDER_ATTRIBUTE_KEY =
      AttributeKey.stringKey("${Instrumentation.ROOT_NAMESPACE}.data_provider")
    private const val MILLISECONDS_PER_SECOND = 1000.0

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
