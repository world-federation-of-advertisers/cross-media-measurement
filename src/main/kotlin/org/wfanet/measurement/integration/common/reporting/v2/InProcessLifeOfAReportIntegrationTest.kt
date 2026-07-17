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

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.type.Interval
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.access.client.v1alpha.TrustedPrincipalAuthInterceptor
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.v1alpha.PoliciesGrpc
import org.wfanet.measurement.access.v1alpha.PolicyKt
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpc
import org.wfanet.measurement.access.v1alpha.RolesGrpc
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.HMSS_PROTOCOL_CONFIG_CONFIG
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.integration.common.TRUSTEE_PROTOCOL_CONFIG_CONFIG_NOISE_NO_THRESHOLDS
import org.wfanet.measurement.integration.crypto.testing.ThrowingKmsClient
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.job.BasicReportsReportsJob
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getMetricRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAReportIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<
      (
        String, ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
      ) -> InProcessDuchy.DuchyDependencies
    >,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
  private val duchyNames: List<String> = ALL_DUCHY_NAMES,
  private val hmssEnabled: Boolean,
  private val trusTeeEnabled: Boolean,
) {

  protected val expectedProtocol: ProtocolConfig.Protocol.ProtocolCase =
    when {
      hmssEnabled && trusTeeEnabled ->
        error("hmssEnabled and trusTeeEnabled are mutually exclusive")
      hmssEnabled -> ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE
      trusTeeEnabled -> ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE
      else -> ProtocolConfig.Protocol.ProtocolCase.DIRECT
    }

  protected val inProcessCmmsComponents: InProcessCmmsComponents =
    InProcessCmmsComponents(
      kingdomDataServicesRule,
      duchyDependenciesRule,
      useEdpSimulators = true,
      trusTeeKmsClient = ThrowingKmsClient,
      duchyNames = duchyNames,
      hmssEnabled = hmssEnabled,
      trusTeeEnabled = trusTeeEnabled,
    )

  private val inProcessCmmsComponentsStartup = TestRule { base, _ ->
    object : Statement() {
      override fun evaluate() {
        inProcessCmmsComponents.startDaemons()
        try {
          base.evaluate()
        } finally {
          inProcessCmmsComponents.stopDaemons()
        }
      }
    }
  }

  protected lateinit var measurementConsumerConfig: MeasurementConsumerConfig

  private val reportingServerRule =
    object : TestRule {
      lateinit var reportingServer: InProcessReportingServer
        private set

      private fun buildReportingServer(): InProcessReportingServer {
        val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
        val measurementConsumer = runBlocking {
          publicKingdomMeasurementConsumersClient
            .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
            .getMeasurementConsumer(
              getMeasurementConsumerRequest { name = measurementConsumerData.name }
            )
        }

        val encryptionKeyPairConfig = encryptionKeyPairConfig {
          principalKeyPairs += principalKeyPairs {
            principal = measurementConsumerData.name
            keyPairs += keyPair {
              publicKeyFile = "mc_enc_public.tink"
              privateKeyFile = "mc_enc_private.tink"
            }
          }
        }
        measurementConsumerConfig = measurementConsumerConfig {
          apiKey = measurementConsumerData.apiAuthenticationKey
          signingCertificateName = measurementConsumer.certificate
          signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
          offlinePrincipal = "principals/mc-user"
        }

        return InProcessReportingServer(
          reportingDataServicesProviderRule.value,
          accessServicesFactory,
          inProcessCmmsComponents.kingdom.publicApiChannel,
          encryptionKeyPairConfig,
          SECRETS_DIR,
          measurementConsumerConfig,
          TRUSTED_CERTIFICATES,
          TestEvent.getDescriptor(),
          defaultModelLineName = inProcessCmmsComponents.modelLineResourceName,
          verboseGrpcLogging = false,
          populationDataProviderName =
            inProcessCmmsComponents.getPopulationData().populationDataProviderName,
        )
      }

      override fun apply(base: Statement, description: Description): Statement {
        return object : Statement() {
          override fun evaluate() {
            reportingServer = buildReportingServer()
            reportingServer.apply(base, description).evaluate()
          }
        }
      }
    }

  protected val reportingServer: InProcessReportingServer
    get() = reportingServerRule.reportingServer

  @get:Rule
  val ruleChain: TestRule =
    chainRulesSequentially(
      inProcessCmmsComponents,
      inProcessCmmsComponentsStartup,
      reportingDataServicesProviderRule,
      reportingServerRule,
    )

  protected val syntheticEventQuery: SyntheticGeneratorEventQuery
    get() = inProcessCmmsComponents.eventQuery

  protected lateinit var credentials: TrustedPrincipalAuthInterceptor.Credentials

  @Before
  fun createAccessPolicy() {
    val measurementConsumerData: MeasurementConsumerData =
      inProcessCmmsComponents.getMeasurementConsumerData()
    val accessChannel = reportingServer.accessChannel

    val rolesStub = RolesGrpc.newBlockingStub(accessChannel)
    val mcResourceType = "halo.wfanet.org/MeasurementConsumer"
    val mcUserRole =
      rolesStub.createRole(
        createRoleRequest {
          roleId = "mcUser"
          role = role {
            resourceTypes += mcResourceType
            permissions +=
              PERMISSIONS_CONFIG.permissionsMap
                .filterValues { it.protectedResourceTypesList.contains(mcResourceType) }
                .keys
                .map { PermissionKey(it).toName() }
          }
        }
      )
    val rootResourceType = "reporting.halo-cmm.org/Root"
    val kingdomUserRole =
      rolesStub.createRole(
        createRoleRequest {
          roleId = "kingdomUser"
          role = role {
            resourceTypes += rootResourceType
            permissions +=
              PERMISSIONS_CONFIG.permissionsMap
                .filterValues { it.protectedResourceTypesList.contains(rootResourceType) }
                .keys
                .map { PermissionKey(it).toName() }
          }
        }
      )

    val principalsStub = PrincipalsGrpc.newBlockingStub(accessChannel)
    val principal =
      principalsStub.createPrincipal(
        createPrincipalRequest {
          principalId = "mc-user"
          this.principal = principal {
            user =
              PrincipalKt.oAuthUser {
                issuer = "example.com"
                subject = "mc-user@example.com"
              }
          }
        }
      )

    val policiesStub = PoliciesGrpc.newBlockingStub(accessChannel)
    policiesStub.createPolicy(
      createPolicyRequest {
        policyId = "test-mc-policy"
        policy = policy {
          protectedResource = measurementConsumerData.name
          bindings +=
            PolicyKt.binding {
              this.role = mcUserRole.name
              members += principal.name
            }
        }
      }
    )
    policiesStub.createPolicy(
      createPolicyRequest {
        policyId = "test-root-policy"
        policy = policy {
          protectedResource = "" // Root
          bindings +=
            PolicyKt.binding {
              this.role = kingdomUserRole.name
              members += principal.name
            }
        }
      }
    )

    credentials = TrustedPrincipalAuthInterceptor.Credentials(principal, setOf("reporting.*"))
  }

  protected val publicKingdomMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  protected val publicDataProvidersClient by lazy {
    DataProvidersCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicMetricCalculationSpecsClient by lazy {
    MetricCalculationSpecsCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicMetricsClient by lazy {
    MetricsCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicReportsClient by lazy {
    ReportsCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicReportingSetsClient by lazy {
    ReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicBasicReportsClient by lazy {
    BasicReportsCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicImpressionQualificationFiltersClient by lazy {
    ImpressionQualificationFiltersCoroutineStub(reportingServer.publicApiChannel)
  }

  protected val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  protected suspend fun listMeasurements(): List<Measurement> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    return publicMeasurementsClient
      .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
      .listMeasurements(listMeasurementsRequest { parent = measurementConsumerData.name })
      .measurementsList
  }

  protected suspend fun listEventGroups(): List<EventGroup> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    return publicEventGroupsClient
      .withCallCredentials(credentials)
      .listEventGroups(
        listEventGroupsRequest {
          parent = measurementConsumerData.name
          pageSize = 1000
        }
      )
      .eventGroupsList
  }

  protected suspend fun createPrimitiveReportingSets(
    eventGroupEntries: List<Pair<EventGroup, String>>,
    measurementConsumerName: String,
  ): List<ReportingSet> {
    val primitiveReportingSets: List<ReportingSet> =
      eventGroupEntries.mapIndexed { index, (eventGroup, filterExp) ->
        reportingSet {
          displayName = "primitive$index"
          if (filterExp.isNotBlank()) {
            filter = filterExp
          }
          primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
        }
      }

    return primitiveReportingSets.mapIndexed { index, primitiveReportingSet ->
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerName
            reportingSet = primitiveReportingSet
            reportingSetId = "abc$index"
          }
        )
    }
  }

  protected suspend fun pollForCompletedReport(reportName: String): Report {
    while (true) {
      val retrievedReport =
        publicReportsClient
          .withCallCredentials(credentials)
          .getReport(getReportRequest { name = reportName })

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedReport.state) {
        Report.State.SUCCEEDED,
        Report.State.FAILED -> return retrievedReport
        Report.State.RUNNING,
        Report.State.UNRECOGNIZED,
        Report.State.STATE_UNSPECIFIED -> delay(5000)
      }
    }
  }

  protected suspend fun pollForCompletedMetric(metricName: String): Metric {
    while (true) {
      val retrievedMetric =
        publicMetricsClient
          .withCallCredentials(credentials)
          .getMetric(getMetricRequest { name = metricName })

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedMetric.state) {
        Metric.State.SUCCEEDED,
        Metric.State.FAILED,
        Metric.State.INVALID -> return retrievedMetric
        Metric.State.RUNNING,
        Metric.State.UNRECOGNIZED,
        Metric.State.STATE_UNSPECIFIED -> delay(5000)
      }
    }
  }

  protected fun calculateExpectedReachMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ): Measurement.Result {
    val reach =
      MeasurementResults.computeReach(
        eventGroupSpecs.asSequence().flatMap { syntheticEventQuery.getUserVirtualIds(it) }
      )
    return MeasurementKt.result {
      this.reach = MeasurementKt.ResultKt.reach { value = reach.toLong() }
    }
  }

  protected fun calculateExpectedReachAndFrequencyMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    maxFrequency: Int,
  ): Measurement.Result {
    val reachAndFrequency =
      MeasurementResults.computeReachAndFrequency(
        eventGroupSpecs.asSequence().flatMap { syntheticEventQuery.getUserVirtualIds(it) },
        maxFrequency,
      )
    return MeasurementKt.result {
      reach = MeasurementKt.ResultKt.reach { value = reachAndFrequency.reach.toLong() }
      frequency =
        MeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(
            reachAndFrequency.relativeFrequencyDistribution.mapKeys { it.key.toLong() }
          )
        }
    }
  }

  protected fun calculateExpectedImpressionMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    maxFrequency: Int,
  ): Measurement.Result {
    val impression =
      MeasurementResults.computeImpression(
        eventGroupSpecs.asSequence().flatMap { syntheticEventQuery.getUserVirtualIds(it) },
        maxFrequency,
      )
    return MeasurementKt.result {
      this.impression = MeasurementKt.ResultKt.impression { value = impression }
    }
  }

  protected fun buildEventGroupSpec(
    eventGroup: EventGroup,
    filter: String,
    collectionInterval: Interval,
  ): EventQuery.EventGroupSpec {
    val cmmsEventGroup = cmmsEventGroup {
      name = eventGroup.cmmsEventGroup
      eventGroupReferenceId = eventGroup.eventGroupReferenceId
      eventTemplates +=
        eventGroup.eventTemplatesList.map { CmmsEventGroupKt.eventTemplate { type = it.type } }
    }

    val eventFilter = RequisitionSpecKt.eventFilter { expression = filter }

    return EventQuery.EventGroupSpec(
      cmmsEventGroup,
      RequisitionSpecKt.EventGroupEntryKt.value {
        this.collectionInterval = collectionInterval
        this.filter = eventFilter
      },
    )
  }

  /** Computes the margin of error, i.e. half width, of a 99.9% confidence interval. */
  protected fun computeErrorMargin(standardDeviation: Double): Double {
    return CONFIDENCE_INTERVAL_MULTIPLIER * standardDeviation
  }

  /** Get one EventGroup per DataProvider for cross-publisher tests. */
  protected suspend fun getMultiEdpEventGroups(): List<EventGroup> {
    return buildList {
      val includedDataProviders = mutableSetOf<String>()
      val eventGroups = listEventGroups()
      eventGroups.forEach { eventGroup ->
        if (includedDataProviders.add(eventGroup.cmmsDataProvider)) {
          add(eventGroup)
        }
      }
    }
  }

  protected suspend fun buildCreateBasicReportRequest(
    eventGroups: List<EventGroup>
  ): CreateBasicReportRequest {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val measurementConsumerKey = MeasurementConsumerKey.fromName(measurementConsumerData.name)!!

    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "abc123")
    val campaignGroup =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = reportingSet {
              displayName = "campaign group"
              campaignGroup = campaignGroupKey.toName()
              primitive =
                ReportingSetKt.primitive {
                  cmmsEventGroups += eventGroups.map { it.cmmsEventGroup }
                }
            }
            reportingSetId = campaignGroupKey.reportingSetId
          }
        )

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
        basicReportId = "basicreport123",
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroup.name
      campaignGroupDisplayName = campaignGroup.displayName
      modelLine = inProcessCmmsComponents.modelLineResourceName
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2021
          month = 3
          day = 14
          hours = 17
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2021
          month = 3
          day = 15
        }
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        custom =
          ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
            filterSpec += impressionQualificationFilterSpec {
              mediaType = MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
      }
    }

    return createBasicReportRequest {
      parent = measurementConsumerData.name
      basicReportId = basicReportKey.basicReportId
      this.basicReport = basicReport
    }
  }

  protected suspend fun executeBasicReportsReportsJob(basicReportName: String) {
    val basicReportKey = BasicReportKey.fromName(basicReportName)!!

    val internalBasicReport =
      reportingServer.internalBasicReportsClient.getBasicReport(
        internalGetBasicReportRequest {
          cmmsMeasurementConsumerId = basicReportKey.cmmsMeasurementConsumerId
          externalBasicReportId = basicReportKey.basicReportId
        }
      )

    val reportName =
      ReportKey(internalBasicReport.cmmsMeasurementConsumerId, internalBasicReport.externalReportId)
        .toName()

    // BasicReportsReportsJob requires the Report to be SUCCEEDED to advance the BasicReport to
    // the next internal state.
    pollForCompletedReport(reportName)

    val measurementConsumerName = inProcessCmmsComponents.getMeasurementConsumerData().name

    val basicReportsReportsJob =
      BasicReportsReportsJob(
        measurementConsumerConfigs { configs[measurementConsumerName] = measurementConsumerConfig },
        reportingServer.internalBasicReportsClient,
        publicReportsClient,
        reportingServer.internalMetricCalculationSpecsClient,
        reportingServer.internalReportResultsClient,
        EventMessageDescriptor(TestEvent.getDescriptor()),
      )

    basicReportsReportsJob.execute()
  }

  protected fun executeReportProcessorJob() {
    val processBuilder =
      ProcessBuilder("python3", POST_PROCESS_REPORT_RESULT_FILE.toPath().toString())

    processBuilder
      .command()
      .add("--internal-api-target=${"localhost:${reportingServer.internalReportingServer.port}"}")
    processBuilder.command().add("--tls-cert-file=${REPORTING_TLS_CERT_FILE.path}")
    processBuilder.command().add("--tls-key-file=${REPORTING_TLS_KEY_FILE.path}")
    processBuilder.command().add("--cert-collection-file=${ALL_ROOT_CERTS_FILE.path}")

    val process = processBuilder.start()

    process.waitFor()
  }

  companion object {
    internal val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    val ALL_ROOT_CERTS_FILE: File = SECRETS_DIR.resolve("all_root_certs.pem")

    internal val TRUSTED_CERTIFICATES =
      readCertificateCollection(ALL_ROOT_CERTS_FILE).associateBy { it.subjectKeyIdentifier!! }

    internal val REPORTING_TLS_CERT_FILE: File = SECRETS_DIR.resolve("reporting_tls.pem")
    internal val REPORTING_TLS_KEY_FILE: File = SECRETS_DIR.resolve("reporting_tls.key")

    internal const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"

    internal val EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))

    // Set epsilon and delta higher without exceeding privacy budget so the noise is smaller in the
    // integration test. Check sample values in CompositionTest.kt.
    internal val DP_PARAMS =
      MetricSpecKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1e-15
      }

    internal val SINGLE_DATA_PROVIDER_DP_PARAMS =
      MetricSpecKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1.01e-15
      }

    internal val VID_SAMPLING_INTERVAL =
      MetricSpecKt.vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }

    internal val SINGLE_DATA_PROVIDER_VID_SAMPLING_INTERVAL =
      MetricSpecKt.vidSamplingInterval {
        start = 0.0f
        width = 0.9f
      }

    // For a 99.9% Confidence Interval.
    internal const val CONFIDENCE_INTERVAL_MULTIPLIER = 3.291

    internal val POST_PROCESS_REPORT_RESULT_FILE: File =
      getRuntimePath(
          Paths.get(
            "wfa_measurement_system",
            "src",
            "main",
            "python",
            "wfa",
            "measurement",
            "reporting",
            "deploy",
            "v2",
            "common",
            "job",
            "post_process_report_result_job_executor.zip",
          )
        )!!
        .toFile()

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig(
        trusTeeProtocolConfigConfig = TRUSTEE_PROTOCOL_CONFIG_CONFIG_NOISE_NO_THRESHOLDS,
        hmssProtocolConfigConfig = HMSS_PROTOCOL_CONFIG_CONFIG,
      )
    }
  }
}
