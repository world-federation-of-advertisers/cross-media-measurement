// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

class PopulationRequisitionFulfillerDaemon : Runnable {
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
    description = ["Serialized FileDescriptorSet for the event message and its dependencies."],
    required = false,
  )
  private lateinit var eventMessageDescriptorSetFiles: List<File>

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
  lateinit var populationKeyAndInfo: List<PopulationKeyAndInfo>
    private set

  class PopulationKeyAndInfo {
    @Option(names = ["--population-key"], required = true)
    lateinit var populationKey: String
      private set

    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
    lateinit var populationInfo: PopulationInfo
      private set
  }

  class PopulationInfo {
    @Option(names = ["--population-spec"], required = true)
    lateinit var populationSpecFile: File
      private set
    @Option(names = ["--event-message-descriptor"], required = true)
    lateinit var eventMessageDescriptorFile: File
      private set
  }

  override fun run() {
    val certificate: X509Certificate =
      flags.pdpCsCertificateDerFile.inputStream().use { input -> readCertificate(input) }
    val signingKeyHandle =
      SigningKeyHandle(
        certificate,
        readPrivateKey(
          flags.pdpCsPrivateKeyDerFile.readByteString(),
          certificate.publicKey.algorithm,
        ),
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

    val populationInfoMap = buildPopulationInfoMap(populationKeyAndInfo)

    var populationRequisitionFulfiller =
      PopulationRequisitionFulfiller(
        pdpData,
        certificatesStub,
        requisitionsStub,
        throttler,
        clientCerts.trustedCertificates,
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

  private fun buildPopulationInfoMap(
    populationKeyAndInfoList: List<PopulationKeyAndInfo>
  ): Map<PopulationKey, org.wfanet.measurement.populationdataprovider.PopulationInfo> {
    return populationKeyAndInfoList.associate {
      grpcRequireNotNull(PopulationKey.fromName(it.populationKey)) to
        PopulationInfo(
          parseTextProto(it.populationInfo.populationSpecFile, PopulationSpec.getDefaultInstance()),
          parseTextProto(
              it.populationInfo.eventMessageDescriptorFile,
              DescriptorProtos.FileDescriptorSet.getDefaultInstance()
            )
            .descriptorForType,
        )
    }
  }
}

fun main(args: Array<String>) = commandLineMain(PopulationRequisitionFulfillerDaemon(), args)
