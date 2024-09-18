package org.wfanet.measurement.populationdataprovider

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.TypeRegistry
import java.io.File
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.dataprovider.DataProviderData
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option

class PopulationRequisitionFulfillerFlags {
  @CommandLine.Option(
    names = ["--kingdom-system-api-target"],
    description = ["gRPC target (authority) of the Kingdom system API server"],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-system-api-cert-host"],
    description =
    [
      "Expected hostname (DNS-ID) in the Kingdom system API server's TLS certificate.",
      "This overrides derivation of the TLS DNS-ID from --kingdom-system-api-target.",
    ],
    required = false,
  )
  var certHost: String? = null
    private set

  /** The PdpSimulator pod's own tls certificates. */
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--data-provider-resource-name"],
    description = ["The public API resource name of this data provider."],
    required = true,
  )
  lateinit var dataProviderResourceName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-certificate-resource-name"],
    description = ["The public API resource name for data provider consent signaling."],
    required = true,
  )
  lateinit var dataProviderCertificateResourceName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-display-name"],
    description = ["The display name of this data provider."],
    required = true,
  )
  lateinit var dataProviderDisplayName: String
    private set

  @CommandLine.Option(
    names = ["--data-provider-encryption-private-keyset"],
    description = ["The PDP's encryption private Tink Keyset."],
    required = true,
  )
  lateinit var pdpEncryptionPrivateKeyset: File
    private set

  @CommandLine.Option(
    names = ["--data-provider-consent-signaling-private-key-der-file"],
    description = ["The PDP's consent signaling private key (DER format) file."],
    required = true,
  )
  lateinit var pdpCsPrivateKeyDerFile: File
    private set

  @CommandLine.Option(
    names = ["--data-provider-consent-signaling-certificate-der-file"],
    description = ["The PDP's consent signaling private key (DER format) file."],
    required = true,
  )
  lateinit var pdpCsCertificateDerFile: File
    private set

  @CommandLine.Option(
    names = ["--throttler-minimum-interval"],
    description = ["Minimum throttle interval"],
    defaultValue = "2s",
  )
  lateinit var throttlerMinimumInterval: Duration
    private set

}
class PopulationRequisitionFulfillerDaemon: Runnable {
  @Command(
    name = "PopulationRequisitionFulfillerDaemon",
    mixinStandardHelpOptions = true,
    showDefaultValues = true,
  )

  @CommandLine.Mixin
  protected lateinit var flags: PopulationRequisitionFulfillerFlags
    private set

  @CommandLine.Option(
    names = ["--event-message-descriptor-set"],
    description =
    [
      "Serialized DescriptorSet for the event message and its dependencies.",
    ],
    required = false,
  )
  private lateinit var eventMessageDescriptorSetFiles: List<File>

  @Option(
    names = ["--pdp-data"],
    description = ["The Population DataProvider's public API resource name."],
    required = true,
  )
  private lateinit var pdpDataFile: File

  @Option(
    names = ["--measurement-consumer-name"],
    description = ["The Measurement Consumer's public API resource name."],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @Option(
    names = ["--population-info-map"],
    description = [
      "Key-value pair of PopulationKey and list of file paths" +
        "that represent the Population Info. The first element of the list is the Population Spec and " +
                  "the second element is the descriptor"],
    required = true,
  )
  private lateinit var populationInfoFileMap: Map<String, List<File>>


  override fun run() {
    val certificate: X509Certificate =
      flags.pdpCsCertificateDerFile.inputStream().use { input -> readCertificate(input) }
    val signingKeyHandle = SigningKeyHandle(
      certificate,
      readPrivateKey(flags.pdpCsPrivateKeyDerFile.readByteString(), certificate.publicKey.algorithm)
    )
    val certificateKey =
      DataProviderCertificateKey.fromName(flags.dataProviderCertificateResourceName)!!
    val pdpData =
      DataProviderData(
        flags.dataProviderResourceName,
        flags.dataProviderDisplayName,
        loadPrivateKey(flags.pdpEncryptionPrivateKeyset),
        signingKeyHandle,
        certificateKey,
      )

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
      )

    val channelTarget = flags.target
    val channelCertHost = flags.certHost

    val certificatesChannel = buildMutualTlsChannel(channelTarget, clientCerts, channelCertHost)
    val certificatesStub = CertificatesCoroutineStub(certificatesChannel)

    val requisitionsChannel = buildMutualTlsChannel(channelTarget, clientCerts, channelCertHost)
    val requisitionsStub = RequisitionsCoroutineStub(requisitionsChannel)

    val modelRolloutsChannel = buildMutualTlsChannel(channelTarget, clientCerts, channelCertHost)
    val modelRolloutsStub = ModelRolloutsCoroutineStub(modelRolloutsChannel)

    val modelReleasesChannel = buildMutualTlsChannel(channelTarget, clientCerts, channelCertHost)
    val modelReleasesStub = ModelReleasesCoroutineStub(modelReleasesChannel)

    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

    val typeRegistry = buildTypeRegistry()

    val populationInfoMap = buildPopulationInfoMap(populationInfoFileMap)

    var populationRequisitionFulfiller = PopulationRequisitionFulfiller(
      pdpData,
      certificatesStub,
      requisitionsStub,
      throttler,
      clientCerts.trustedCertificates,
      measurementConsumerName,
      modelRolloutsStub,
      modelReleasesStub,
      populationInfoMap,
      typeRegistry,
    )

    runBlocking { populationRequisitionFulfiller.run() }
  }

  private fun buildTypeRegistry(): TypeRegistry {
    val builder = TypeRegistry.newBuilder()
    if (::eventMessageDescriptorSetFiles.isInitialized) {
      val fileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
        eventMessageDescriptorSetFiles.map {
          parseTextProto(it, DescriptorProtos.FileDescriptorSet.getDefaultInstance())
        }
      val descriptors = fileDescriptorSets.map { it.descriptorForType }
      builder.add(descriptors)
    }
    return builder.build()
  }

  private fun buildPopulationInfoMap(populationInfoFileMap: Map<String, List<File>>): Map<PopulationKey, PopulationInfo> {
    return populationInfoFileMap.entries.associate {
      grpcRequireNotNull(PopulationKey.fromName(it.key)) to PopulationInfo(parseTextProto(it.value[0], PopulationSpec.getDefaultInstance()), parseTextProto(it.value[1], DescriptorProtos.FileDescriptorSet.getDefaultInstance()).descriptorForType)
    }
  }
}

fun main(args: Array<String>) = commandLineMain(PopulationRequisitionFulfillerDaemon(), args)

