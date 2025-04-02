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
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import java.io.File
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.ProtoReflection
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

@Command(
  name = "PopulationRequisitionFulfillerDaemon",
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
  description = ["Fulfills a population requisition."],
)
class PopulationRequisitionFulfillerDaemon : Runnable {
  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  private lateinit var target: String

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom system API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  private var certHost: String? = null

  /** The PdpSimulator pod's own tls certificates. */
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--data-provider-resource-name"],
    description = ["The public API resource name of this data provider."],
    required = true,
  )
  private lateinit var dataProviderResourceName: String

  @Option(
    names = ["--data-provider-certificate-resource-name"],
    description = ["The public API resource name for data provider consent signaling."],
    required = true,
  )
  private lateinit var dataProviderCertificateResourceName: String

  @Option(
    names = ["--data-provider-display-name"],
    description = ["The display name of this data provider."],
    required = true,
  )
  private lateinit var dataProviderDisplayName: String

  @Option(
    names = ["--data-provider-encryption-private-keyset"],
    description = ["The PDP's encryption private Tink Keyset."],
    required = true,
  )
  private lateinit var pdpEncryptionPrivateKeyset: File

  @Option(
    names = ["--data-provider-consent-signaling-private-key-der-file"],
    description = ["The PDP's consent signaling private key (DER format) file."],
    required = true,
  )
  private lateinit var pdpCsPrivateKeyDerFile: File

  @Option(
    names = ["--data-provider-consent-signaling-certificate-der-file"],
    description = ["The PDP's consent signaling private key (DER format) file."],
    required = true,
  )
  private lateinit var pdpCsCertificateDerFile: File

  @Option(
    names = ["--throttler-minimum-interval"],
    description = ["Minimum throttle interval"],
    defaultValue = "2s",
  )
  private lateinit var throttlerMinimumInterval: Duration

  @Option(
    names = ["--event-message-descriptor-set"],
    description =
      [
        "Serialized FileDescriptorSet for the event message and its dependencies. This can be specified multiple times"
      ],
    required = false,
  )
  private lateinit var eventMessageDescriptorSetFiles: List<File>

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
  private lateinit var populationKeyAndInfo: List<PopulationKeyAndInfo>

  private class PopulationKeyAndInfo {
    @Option(names = ["--population-resource-name"], required = true)
    lateinit var populationResourceName: String
      private set

    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
    lateinit var populationInfo: PopulationInfo
      private set
  }

  private class PopulationInfo {
    @Option(names = ["--population-spec"], required = true)
    lateinit var populationSpecFile: File
      private set

    @Option(names = ["--event-message-type-url"], required = true)
    lateinit var eventMessageTypeUrl: String
      private set
  }

  override fun run() {
    val certificate: X509Certificate =
      pdpCsCertificateDerFile.inputStream().use { input -> readCertificate(input) }
    val signingKeyHandle =
      SigningKeyHandle(
        certificate,
        readPrivateKey(pdpCsPrivateKeyDerFile.readByteString(), certificate.publicKey.algorithm),
      )
    val certificateKey = DataProviderCertificateKey.fromName(dataProviderCertificateResourceName)!!
    val pdpData =
      DataProviderData(
        dataProviderResourceName,
        dataProviderDisplayName,
        loadPrivateKey(pdpEncryptionPrivateKeyset),
        signingKeyHandle,
        certificateKey,
      )

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )

    val channelTarget: String = target
    val channelCertHost: String? = certHost

    val publicApiChannel = buildMutualTlsChannel(channelTarget, clientCerts, channelCertHost)

    val certificatesStub = CertificatesCoroutineStub(publicApiChannel)
    val requisitionsStub = RequisitionsCoroutineStub(publicApiChannel)
    val modelRolloutsStub = ModelRolloutsCoroutineStub(publicApiChannel)
    val modelReleasesStub = ModelReleasesCoroutineStub(publicApiChannel)

    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval)

    val typeRegistry = buildTypeRegistry()

    val populationInfoMap = buildPopulationInfoMap(typeRegistry)

    val populationRequisitionFulfiller =
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
    return TypeRegistry.newBuilder()
      .apply {
        if (::eventMessageDescriptorSetFiles.isInitialized) {
          add(
            ProtoReflection.buildDescriptors(loadFileDescriptorSets(eventMessageDescriptorSetFiles))
          )
        }
      }
      .build()
  }

  private fun loadFileDescriptorSets(
    files: Iterable<File>
  ): List<DescriptorProtos.FileDescriptorSet> {
    return files.map { file ->
      file.inputStream().use { input ->
        DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
      }
    }
  }

  private fun buildPopulationInfoMap(
    typeRegistry: TypeRegistry
  ): Map<PopulationKey, org.wfanet.measurement.populationdataprovider.PopulationInfo> {
    return populationKeyAndInfo.associate {
      grpcRequireNotNull(PopulationKey.fromName(it.populationResourceName)) to
        PopulationInfo(
          parseTextProto(it.populationInfo.populationSpecFile, PopulationSpec.getDefaultInstance()),
          typeRegistry.getDescriptorForTypeUrl(it.populationInfo.eventMessageTypeUrl),
        )
    }
  }

  companion object {
    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable
  }
}

fun main(args: Array<String>) = commandLineMain(PopulationRequisitionFulfillerDaemon(), args)
