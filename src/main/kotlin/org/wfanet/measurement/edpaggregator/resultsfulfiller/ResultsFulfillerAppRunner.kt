package org.wfanet.measurement.edpaggregator.resultsfulfiller

import kotlinx.coroutines.runBlocking
import com.google.protobuf.Parser
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import picocli.CommandLine
import java.io.File
import java.util.logging.Logger
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.queue.QueueSubscriber

@CommandLine.Command(name = "results_fulfiller_app_runner")
class ResultsFulfillerAppRunner : Runnable {
  @CommandLine.Option(
    names = ["--edpa-tls-cert-file-path"],
    description = ["User's own TLS cert file path."],
    defaultValue = "",
  )
  lateinit var edpaCertFilePath: String
    private set

  @CommandLine.Option(
    names = ["--edpa-tls-key-file-path"],
    description = ["User's own TLS private key file path."],
    defaultValue = "",
  )
  lateinit var edpaPrivateKeyFilePath: String
    private set

  @CommandLine.Option(
    names = ["--secure-computation-cert-collection-file-path"],
    description = ["Trusted root Cert collection file path."],
    required = false,
  )
  var secureComputationCertCollectionFilePath: String? = null
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target of the Kingdom public API server"],
    required = true
  )
  private lateinit var kingdomPublicApiTarget: String

  @CommandLine.Option(
    names = ["--secure-computation-public-api-target"],
    description = ["gRPC target of the Secure Conmputation public API server"],
    required = true
  )
  private lateinit var secureComputationPublicApiTarget: String

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description = [
      "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
      "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target."
    ],
    required = false
  )
  private var kingdomPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--secure-computation-public-api-cert-host"],
    description = [
      "Expected hostname (DNS-ID) in the SecureComputation public API server's TLS certificate.",
      "This overrides derivation of the TLS DNS-ID from --secure-computation-public-api-target."
    ],
    required = false
  )
  private var secureComputationPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--kingdom-cert-collection-file-path"],
    description = ["Kingdom root collections file path"],
    required = true
  )
  private lateinit var kingdomCertCollectionFilePath: String


  @CommandLine.Option(
    names = ["--subscription-id"],
    description = ["Subscription ID for the queue"],
    required = true
  )
  private lateinit var subscriptionId: String

  @CommandLine.Option(
    names = ["--google-pub-sub-project-id"],
    description = ["Project ID of Google pub sub subscriber."],
    required = true,
  )
  lateinit var pubSubProjectId: String
    private set


  override fun run() {
    logger.info("~~~~~~~~~~~~ results fulfiller1")
    val queueSubscriber = createQueueSubscriber()
    logger.info("~~~~~~~~~~~~ results fulfiller2")
    val parser = createWorkItemParser()
    logger.info("~~~~~~~~~~~~ results fulfiller3")


    // Get client certificates from server flags
    val edpaCertFile = File(edpaCertFilePath)
    val edpaPrivateKeyFile = File(edpaPrivateKeyFilePath)
    val secureComputationCertCollectionFile = File(secureComputationCertCollectionFilePath)
    val secureComputationClientCerts = SigningCerts.fromPemFiles(
      certificateFile = edpaCertFile,
      privateKeyFile = edpaPrivateKeyFile,
      trustedCertCollectionFile = secureComputationCertCollectionFile
    )
    logger.info("~~~~~~~~~~~~ results fulfiller4")
    logger.info("~~~SEC comp TARGET: ${secureComputationPublicApiTarget}")
    logger.info("~~~secureComputationPublicApiCertHost: ${secureComputationPublicApiCertHost}")

    try {
      logger.info(edpaCertFile.readText())
      logger.info(edpaPrivateKeyFile.readText())
      logger.info(secureComputationCertCollectionFile.readText())
    }catch (e: Exception){
      e.printStackTrace()
    }
    // Build the mutual TLS channel for secure computation API
    val publicChannel = buildMutualTlsChannel(
      secureComputationPublicApiTarget,
      secureComputationClientCerts,
      secureComputationPublicApiCertHost
    )
    logger.info("~~~~~~~~~~~~ results fulfiller5")
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(publicChannel)
    val workItemAttemptsClient = WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(publicChannel)
    logger.info("~~~~~~~~~~~~ results fulfiller6")
    // Create and run the ResultsFulfillerApp
   val kingdomCertCollectionFile = File(kingdomCertCollectionFilePath)
    val resultsFulfillerApp = ResultsFulfillerAppImpl(
      subscriptionId = subscriptionId,
      queueSubscriber = queueSubscriber,
      parser = parser,
      workItemsClient = workItemsClient,
      workItemAttemptsClient = workItemAttemptsClient,
      cmmsTarget = kingdomPublicApiTarget,
      cmmsCertHost = kingdomPublicApiCertHost,
      trustedCertCollection = kingdomCertCollectionFile
    )

    logger.info("~~~~~~~~~~~~ results fulfiller7")
    runBlocking {
      resultsFulfillerApp.run()
    }
  }

  private fun createQueueSubscriber(): QueueSubscriber {
    val pubSubClient = DefaultGooglePubSubClient()
    logger.info("~~~~ creating pubSubclient: ${pubSubProjectId}, ${pubSubClient}")
    return Subscriber(projectId = pubSubProjectId, googlePubSubClient = pubSubClient)
  }

  private fun createWorkItemParser(): Parser<WorkItem> {
    return WorkItem.parser()
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(ResultsFulfillerAppRunner(), args)
  }
}
