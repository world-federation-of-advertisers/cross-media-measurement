package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.io.PrintWriter
import java.io.StringWriter
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDate
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.access.client.v1alpha.TrustedPrincipalAuthInterceptor
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
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessKingdom
import org.wfanet.measurement.integration.common.SECRET_FILES_PATH
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.integration.common.loadTestCertCollection
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.tools.ReadEventGroups
import org.wfanet.measurement.loadtest.dataprovider.tools.WriteEventGroups
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import picocli.CommandLine

abstract class InProcessEventGroupActivitiesPerformanceTest(
  private val kingdomDataServicesRule: ProviderRule<DataServices>,
  private val reportingInternalServicesRule: ProviderRule<Services>,
  private val accessServicesFactory: AccessServicesFactory,
) {

  private val kingdom =
    InProcessKingdom(
      dataServicesProvider = { kingdomDataServicesRule.value },
      verboseGrpcLogging = false,
      redirectUri = REDIRECT_URI,
    )

  private val reportingRule =
    object : TestRule {
      lateinit var reportingServer: InProcessReportingServer
        private set

      lateinit var measurementConsumerName: String
        private set

      lateinit var dataProvider1Name: String
        private set

      private fun buildReportingServer(): InProcessReportingServer {
        val cmmsAccountsStub = AccountsGrpcKt.AccountsCoroutineStub(kingdom.publicApiChannel)
        val cmmsApiKeysStub = ApiKeysGrpcKt.ApiKeysCoroutineStub(kingdom.publicApiChannel)
        val cmmsMeasurementConsumersStub =
          MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(kingdom.publicApiChannel)
        val resourceSetup =
          ResourceSetup(
            internalAccountsClient = kingdom.internalAccountsClient,
            internalDataProvidersClient = kingdom.internalDataProvidersClient,
            internalCertificatesClient = kingdom.internalCertificatesClient,
            accountsClient = cmmsAccountsStub,
            apiKeysClient = cmmsApiKeysStub,
            measurementConsumersClient = cmmsMeasurementConsumersStub,
            runId = "12345",
            requiredDuchies = emptyList(),
          )
        val internalAccount: Account = runBlocking { resourceSetup.createAccountWithRetries() }
        val (measurementConsumer, mcApiKey) =
          runBlocking {
            resourceSetup.createMeasurementConsumer(MC_ENTITY_CONTENT, internalAccount)
          }
        measurementConsumerName = measurementConsumer.name

        val dataProvider1: DataProvider = runBlocking {
          resourceSetup.createInternalDataProvider(EDP1_ENTITY_CONTENT)
        }
        dataProvider1Name =
          DataProviderKey(ExternalId(dataProvider1.externalDataProviderId).apiId.value).toName()

        val encryptionKeyPairConfig = encryptionKeyPairConfig {
          principalKeyPairs +=
            EncryptionKeyPairConfigKt.principalKeyPairs {
              principal = measurementConsumer.name
              keyPairs +=
                EncryptionKeyPairConfigKt.keyPair {
                  publicKeyFile = "mc_enc_public.tink"
                  privateKeyFile = "mc_enc_private.tink"
                }
            }
        }
        val measurementConsumerConfig = measurementConsumerConfig {
          apiKey = mcApiKey
          signingCertificateName = measurementConsumer.certificate
          signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
        }

        return InProcessReportingServer(
          reportingInternalServicesRule.value,
          accessServicesFactory,
          kingdom.publicApiChannel,
          encryptionKeyPairConfig,
          SECRET_FILES_PATH.toFile(),
          measurementConsumerConfig,
          TRUSTED_CERTIFICATES,
          kingdom.knownEventGroupMetadataTypes,
          TestEvent.getDescriptor(),
          "",
          verboseGrpcLogging = false,
          populationDataProviderName = "",
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

  @get:Rule
  val ruleChain: TestRule =
    chainRulesSequentially(
      kingdomDataServicesRule,
      kingdom,
      reportingInternalServicesRule,
      reportingRule,
    )

  private val kingdomEventGroupsStub by lazy {
    EventGroupsGrpcKt.EventGroupsCoroutineStub(kingdom.publicApiChannel)
  }

  private val kingdomEventGroupActivitiesStub by lazy {
    EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub(kingdom.publicApiChannel)
  }

  private val reportingEventGroupsStub by lazy {
    org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub(
      reportingRule.reportingServer.publicApiChannel
    )
  }

  private lateinit var callCredentials: TrustedPrincipalAuthInterceptor.Credentials

  @Before
  fun createAccessPolicy() {
    val accessChannel = reportingRule.reportingServer.accessChannel

    val rolesStub = RolesGrpc.newBlockingStub(accessChannel)
    val eventGroupReaderRole =
      rolesStub.createRole(
        createRoleRequest {
          roleId = "mcEventGroupReader"
          role = role {
            resourceTypes += "halo.wfanet.org/MeasurementConsumer"
            permissions += "permissions/reporting.eventGroups.get"
            permissions += "permissions/reporting.eventGroups.list"
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
          protectedResource = reportingRule.measurementConsumerName
          bindings +=
            PolicyKt.binding {
              this.role = eventGroupReaderRole.name
              members += principal.name
            }
        }
      }
    )

    callCredentials = TrustedPrincipalAuthInterceptor.Credentials(principal, setOf("reporting.*"))
  }

  @Test
  fun `update 10 weeks of activities every second for a minute`() = runBlocking {
    val eventGroupName =
      createEventGroup(
        reportingRule.dataProvider1Name,
        reportingRule.measurementConsumerName,
        "eg-write-perf",
      )

    val startDate = LocalDate.of(2024, 1, 1)
    val endDate = startDate.plusWeeks(10)

    val writer =
      WriteEventGroups(
        kingdomEventGroupsStub.withPrincipalName(reportingRule.dataProvider1Name),
        kingdomEventGroupActivitiesStub.withPrincipalName(reportingRule.dataProvider1Name),
      )

    val testDurationSeconds = 60
    val latencies = LongArray(testDurationSeconds)
    repeat(testDurationSeconds) { i ->
      val start = System.currentTimeMillis()
      val exitCode =
        CommandLine(writer)
          .registerConverter(Duration::class.java) { it.toDuration() }
          .execute(
            "--kingdom-public-api-target=ignored",
            "--tls-cert-file=ignored",
            "--tls-key-file=ignored",
            "--cert-collection-file=ignored",
            "--measurement-consumer=${reportingRule.measurementConsumerName}",
            "--max-qps=20",
            "--parallelism=5",
            "--rpc-timeout=60s",
            "create-activities",
            "--event-group=$eventGroupName",
            "--start-date=$startDate",
            "--end-date=$endDate",
            "--batch-size=7",
          )
      assertThat(exitCode).isEqualTo(0)
      val elapsed = System.currentTimeMillis() - start
      latencies[i] = elapsed
      val sleep = 1000 - elapsed
      if (sleep > 0) delay(sleep)
    }

    val mean = latencies.average()
    val max = latencies.maxOrNull() ?: 0
    println("Write Load Test Stats (over $testDurationSeconds seconds):")
    println("Mean Latency: %.2f ms".format(mean))
    println("Max Latency: $max ms")

    assertThat(mean).isLessThan(1000.0)
  }

  // @Test
  fun `list 100 event groups with activity_contains and summary view`() = runBlocking {
    val totalEventGroups = 100
    val startDate = LocalDate.of(2024, 1, 1)
    val endDate = startDate.plusDays(365)

    val eventGroupNames =
      (1..totalEventGroups).map {
        createEventGroup(
          reportingRule.dataProvider1Name,
          reportingRule.measurementConsumerName,
          "eg-list-perf-$it",
        )
      }

    val writer =
      WriteEventGroups(
        kingdomEventGroupsStub.withPrincipalName(reportingRule.dataProvider1Name),
        kingdomEventGroupActivitiesStub.withPrincipalName(reportingRule.dataProvider1Name),
      )

    val argsDaily =
      mutableListOf(
        "--kingdom-public-api-target=ignored",
        "--tls-cert-file=ignored",
        "--tls-key-file=ignored",
        "--cert-collection-file=ignored",
        "--measurement-consumer=${reportingRule.measurementConsumerName}",
        "--max-qps=50",
        "--parallelism=50",
        "--rpc-timeout=120s",
        "create-activities",
        "--start-date=$startDate",
        "--end-date=$endDate",
        "--step-days=1",
        "--batch-size=365",
      )
    eventGroupNames.take(totalEventGroups / 2).forEach { argsDaily.add("--event-group=$it") }
    val exitCodeDaily =
      CommandLine(writer)
        .registerConverter(Duration::class.java) { it.toDuration() }
        .execute(*argsDaily.toTypedArray())
    assertThat(exitCodeDaily).isEqualTo(0)

    val argsSparse =
      mutableListOf(
        "--kingdom-public-api-target=ignored",
        "--tls-cert-file=ignored",
        "--tls-key-file=ignored",
        "--cert-collection-file=ignored",
        "--measurement-consumer=${reportingRule.measurementConsumerName}",
        "--max-qps=50",
        "--parallelism=50",
        "--rpc-timeout=120s",
        "create-activities",
        "--start-date=$startDate",
        "--end-date=$endDate",
        "--step-days=2",
        "--batch-size=365",
      )
    eventGroupNames.drop(totalEventGroups / 2).forEach { argsSparse.add("--event-group=$it") }
    val exitCodeSparse =
      CommandLine(writer)
        .registerConverter(Duration::class.java) { it.toDuration() }
        .execute(*argsSparse.toTypedArray())
    assertThat(exitCodeSparse).isEqualTo(0)

    val reader =
      ReadEventGroups(
        reportingEventGroupsStub.withPrincipalName(reportingRule.measurementConsumerName)
      )

    // Filter for a 90-day window
    val filterStart = startDate.plusDays(100)
    val filterEnd = startDate.plusDays(190)

    val capture = StringWriter()
    val commandLine = CommandLine(reader)
    commandLine.registerConverter(Duration::class.java) { it.toDuration() }
    commandLine.out = PrintWriter(capture)
    val exitCode =
      commandLine.execute(
        "--reporting-public-api-target=ignored",
        "--tls-cert-file=ignored",
        "--tls-key-file=ignored",
        "--cert-collection-file=ignored",
        "list",
        "--parent=${reportingRule.measurementConsumerName}",
        "--activity-contains-start-date=$filterStart",
        "--activity-contains-end-date=$filterEnd",
        "--view=${org.wfanet.measurement.reporting.v2alpha.EventGroup.View.WITH_ACTIVITY_SUMMARY}",
      )
    assertThat(exitCode).isEqualTo(0)

    val output = capture.toString()
    println(output)
  }

  private suspend fun createEventGroup(parent: String, mc: String, refId: String): String {
    val response =
      kingdomEventGroupsStub
        .withPrincipalName(parent)
        .createEventGroup(
          createEventGroupRequest {
            this.parent = parent
            eventGroup = eventGroup {
              measurementConsumer = mc
              eventGroupReferenceId = refId
            }
          }
        )
    return response.name
  }

  companion object {
    private const val REDIRECT_URI = "https://localhost:2050"
    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"

    private val MC_ENTITY_CONTENT: EntityContent = createEntityContent("mc")
    private val EDP1_ENTITY_CONTENT = createEntityContent("edp1")
    private val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      loadTestCertCollection("all_root_certs.pem").associateBy { it.subjectKeyIdentifier!! }

    init {
      DuchyIds.setForTest(emptyList())
    }
  }
}
