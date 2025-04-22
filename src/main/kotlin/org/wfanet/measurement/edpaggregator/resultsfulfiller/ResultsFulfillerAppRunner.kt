package org.wfanet.measurement.edpaggregator.resultsfulfiller

import kotlinx.coroutines.runBlocking
import com.google.protobuf.Parser
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
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
  lateinit var tlsFlags: TlsFlags
    private set


  @CommandLine.Option(
    names = ["--subscription-id"],
    description = ["Subscription ID for the queue"],
    required = true
  )
  private lateinit var subscriptionId: String

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
    names = ["--edp-certificate-name"],
    description = ["Data provider certificate name"],
    required = true
  )
  private lateinit var edpCertificateName: String

  @CommandLine.Option(
    names = ["--private-encryption-key-file"],
    description = ["Path to the private encryption key file"],
    required = true
  )
  private lateinit var privateEncryptionKeyFile: File

  @CommandLine.Option(
    names = ["--edp-cert-der-file"],
    description = ["EDP cert (DER format) file."],
    required = true,
  )
  lateinit var edpCertDerFile: File
    private set

  @CommandLine.Option(
    names = ["--edp-key-der-file"],
    description = ["EDP private key (DER format) file."],
    required = true,
  )
  lateinit var edpKeyDerFile: File
    private set

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
    val clientCerts = SigningCerts.fromPemFiles(
      certificateFile = tlsFlags.certFile,
      privateKeyFile = tlsFlags.privateKeyFile,
      trustedCertCollectionFile = tlsFlags.certCollectionFile
    )

    // Build the mutual TLS channel for kingdom public API
    val publicChannel = buildMutualTlsChannel(
      kingdomPublicApiTarget,
      clientCerts,
      kingdomPublicApiCertHost
    )

    // TODO: may need to change if work items client and work item attempts clients use different certs and targets
    val requisitionsStub = RequisitionsGrpcKt.RequisitionsCoroutineStub(publicChannel)
    val workItemsClient = WorkItemsGrpcKt.WorkItemsCoroutineStub(publicChannel)
    val workItemAttemptsClient = WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(publicChannel)

    // Create data provider certificate key from name
    val dataProviderCertificateKey = DataProviderCertificateKey.fromName(edpCertificateName)
      ?: throw Exception("Invalid data provider certificate")

    // Load private encryption key from file
    val dataProviderPrivateEncryptionKey = loadPrivateKey(privateEncryptionKeyFile)

    // Create signing key handle using the same certificates used for the channel
    val dataProviderSigningKeyHandle = loadSigningKey(edpCertDerFile, edpKeyDerFile)

    // Create and run the ResultsFulfillerApp
    val resultsFulfillerApp = ResultsFulfillerAppImpl(
      subscriptionId = subscriptionId,
      queueSubscriber = queueSubscriber,
      parser = parser,
      workItemsClient = workItemsClient,
      workItemAttemptsClient = workItemAttemptsClient,
      requisitionsStub = requisitionsStub,
      dataProviderCertificateKey = dataProviderCertificateKey,
      dataProviderSigningKeyHandle = dataProviderSigningKeyHandle,
      dataProviderPrivateEncryptionKey = dataProviderPrivateEncryptionKey
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
