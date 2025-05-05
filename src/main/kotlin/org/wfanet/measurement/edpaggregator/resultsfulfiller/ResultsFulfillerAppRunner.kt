package org.wfanet.measurement.edpaggregator.resultsfulfiller

import kotlinx.coroutines.runBlocking
import com.google.protobuf.Parser
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import picocli.CommandLine
import java.io.File
import java.security.cert.X509Certificate
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.queue.QueueSubscriber

@CommandLine.Command(name = "results_fulfiller_app_runner")
class ResultsFulfillerAppRunner : Runnable {
  @CommandLine.Mixin
  lateinit var workItemTlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target of the Kingdom public API server"],
    required = true
  )
  private lateinit var kingdomPublicApiTarget: String

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
    names = ["--kingdom-cert-collection-file"],
    description = ["Kingdom root collections file"],
    required = true
  )
  private lateinit var kingdomCertCollectionFile: File


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
    val queueSubscriber = createQueueSubscriber()
    val parser = createWorkItemParser()


    // Get client certificates from server flags
    val workItemClientCerts = SigningCerts.fromPemFiles(
      certificateFile = workItemTlsFlags.certFile,
      privateKeyFile = workItemTlsFlags.privateKeyFile,
      trustedCertCollectionFile = workItemTlsFlags.certCollectionFile
    )

    // Build the mutual TLS channel for kingdom public API
    val publicChannel = buildMutualTlsChannel(
      kingdomPublicApiTarget,
      workItemClientCerts,
      kingdomPublicApiCertHost
    )

    // TODO: may need to change if work items client and work item attempts clients use different certs and targets
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(publicChannel)
    val workItemAttemptsClient = WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(publicChannel)

    // Create and run the ResultsFulfillerApp
    val resultsFulfillerApp = ResultsFulfillerAppImpl(
      subscriptionId = subscriptionId,
      queueSubscriber = queueSubscriber,
      parser = parser,
      workItemsClient = workItemsClient,
      workItemAttemptsClient = workItemAttemptsClient,
      cmmsTarget = kingdomPublicApiTarget,
      cmmsCertHost = kingdomPublicApiCertHost!!,
      trustedCertCollection = kingdomCertCollectionFile
    )

    runBlocking {
      resultsFulfillerApp.run()
    }
  }

  private fun createQueueSubscriber(): QueueSubscriber {
    val pubSubClient = DefaultGooglePubSubClient()
    return Subscriber(projectId = pubSubProjectId, googlePubSubClient = pubSubClient)
  }

  private fun createWorkItemParser(): Parser<WorkItem> {
    return WorkItem.parser()
  }

  /** Loads a signing private key from DER files. */
  fun loadSigningKey(certificateDer: File, privateKeyDer: File): SigningKeyHandle {
    val certificate: X509Certificate =
      certificateDer.inputStream().use { input -> readCertificate(input) }
    return SigningKeyHandle(
      certificate,
      readPrivateKey(privateKeyDer.readByteString(), certificate.publicKey.algorithm),
    )
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(ResultsFulfillerAppRunner(), args)
  }
}
