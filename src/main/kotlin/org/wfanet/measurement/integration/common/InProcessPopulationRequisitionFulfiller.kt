package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import com.google.protobuf.TypeRegistry
import io.grpc.Channel
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.toList
import org.wfanet.measurement.kingdom.service.api.v2alpha.CertificatesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelReleasesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelRolloutsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.withApiKeyAuthenticationServerInterceptor
import org.wfanet.measurement.populationdataprovider.PopulationInfo
import org.wfanet.measurement.populationdataprovider.PopulationRequisitionFulfiller

class InProcessPopulationRequisitionFulfiller(
  val pdpData: DataProviderData,
  val resourceName: String,
  private val populationInfoMap: Map<PopulationKey, PopulationInfo>,
  val typeRegistry: TypeRegistry,
  private val kingdomPublicApiChannel: Channel,
  dataServicesProvider: () -> DataServices,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  val verboseGrpcLogging: Boolean = true,
  daemonContext: CoroutineContext = Dispatchers.Default,
) : TestRule {
  private val daemonScope = CoroutineScope(daemonContext)
  private lateinit var populationRequisitionFulfillerJob: Job
  private val kingdomDataServices by lazy { dataServicesProvider() }
  private val internalApiChannel by lazy { internalDataServer.channel }
  private val internalApiKeysClient by lazy { ApiKeysGrpcKt.ApiKeysCoroutineStub(internalApiChannel) }


  private val modelRolloutsClient by lazy {
    ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName)
  }

  private val modelReleasesClient by lazy {
    ModelReleasesGrpcKt.ModelReleasesCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName)
  }

  private val certificatesClient by lazy {
    CertificatesGrpcKt.CertificatesCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName)
  }

  private val requisitionsClient by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName)
  }

  private val internalCertificatesClient by lazy { org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub(internalApiChannel) }
  private val internalRequisitionsClient by lazy {
    org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub(internalApiChannel)
  }
  private val internalModelRolloutsClient by lazy { org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(internalApiChannel) }
  private val internalModelReleasesClient by lazy { org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub(internalApiChannel) }



  private val internalDataServer =
    GrpcTestServerRule(
      logAllRequests = verboseGrpcLogging,
      defaultServiceConfig = DEFAULT_SERVICE_CONFIG_MAP,
    ) {
      logger.info("Building Kingdom's internal Data services used by Population Requisition Fulfiller.")
      kingdomDataServices.buildDataServices().toList().forEach { addService(it) }
    }


  private val publicApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's public API services used by Population Requisition Fulfiller.")
      listOf(
        CertificatesService(internalCertificatesClient)
          .withMetadataPrincipalIdentities()
          .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
        RequisitionsService(internalRequisitionsClient)
          .withMetadataPrincipalIdentities()
          .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
        ModelRolloutsService(internalModelRolloutsClient)
          .withMetadataPrincipalIdentities()
          .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
        ModelReleasesService(internalModelReleasesClient)
          .withMetadataPrincipalIdentities()
          .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
      )
        .forEach { addService(it) }
    }

  fun start() {
    populationRequisitionFulfillerJob =
      daemonScope.launch(
        CoroutineName("Population Requisition Fulfiller Daemon") +
          CoroutineExceptionHandler { _, e ->
            logger.log(Level.SEVERE, e) { "Error in Population Requisition Fulfiller Daemon" }
          }
      ) {
        val populationRequisitionFulfiller =
          PopulationRequisitionFulfiller(
            pdpData,
            certificatesClient,
            requisitionsClient,
            MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
            trustedCertificates,
            modelRolloutsClient,
            modelReleasesClient,
            populationInfoMap,
            typeRegistry,
          )
        populationRequisitionFulfiller.run()
      }
  }

  suspend fun stop() {
    populationRequisitionFulfillerJob.cancelAndJoin()
  }

  /** Provides a gRPC channel to the duchy's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  override fun apply(statement: Statement, description: Description) =
    object : Statement() {
      override fun evaluate() {
        publicApiServer.apply(statement, description)
      }
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
