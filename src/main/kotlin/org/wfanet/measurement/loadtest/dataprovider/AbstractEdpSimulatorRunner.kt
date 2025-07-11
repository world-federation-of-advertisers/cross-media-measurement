// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import io.grpc.ManagedChannel
import java.io.File
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.ZoneId
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.FileExistsHealth
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.SettableHealth
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import picocli.CommandLine

/** The base class of the EdpSimulator runner. */
@CommandLine.Command(mixinStandardHelpOptions = true)
abstract class AbstractEdpSimulatorRunner<T : EdpSimulator> : Runnable {
  @CommandLine.Mixin private lateinit var flags: EdpSimulatorFlags

  protected val syntheticPopulationSpec: SyntheticPopulationSpec by lazy {
    parseTextProto(flags.populationSpecFile, SyntheticPopulationSpec.getDefaultInstance())
  }

  protected val typeRegistry: TypeRegistry by lazy {
    TypeRegistry.newBuilder()
      .apply {
        add(COMPILED_PROTOBUF_TYPES.flatMap { it.messageTypes })
        addEventMessageDescriptors()
        add(additionalMessageTypes)
      }
      .build()
  }

  protected val edpData: DataProviderData by lazy {
    val signingKeyHandle =
      loadSigningKey(flags.edpCsCertificateDerFile, flags.edpCsPrivateKeyDerFile)
    val certificateKey =
      DataProviderCertificateKey.fromName(flags.dataProviderCertificateResourceName)!!

    DataProviderData(
      flags.dataProviderResourceName,
      flags.dataProviderDisplayName,
      loadPrivateKey(flags.edpEncryptionPrivateKeyset),
      signingKeyHandle,
      certificateKey,
    )
  }

  protected val syntheticDataTimeZone: ZoneId
    get() = flags.syntheticDataTimeZone

  open val additionalMessageTypes: Collection<Descriptors.Descriptor> = emptyList()

  abstract suspend fun T.ensureEventGroups()

  abstract fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec

  abstract fun buildEdpSimulator(
    measurementConsumerName: String,
    kingdomPublicApiChannel: ManagedChannel,
    requisitionFulfillmentStubsByDuchyId: Map<String, RequisitionFulfillmentCoroutineStub>,
    trustedCertificates: Map<ByteString, X509Certificate>,
    eventQuery: SyntheticGeneratorEventQuery,
    hmssVidIndexMap: InMemoryVidIndexMap?,
    logSketchDetails: Boolean,
    throttler: MinimumIntervalThrottler,
    health: SettableHealth,
    random: Random,
  ): T

  private fun TypeRegistry.Builder.addEventMessageDescriptors() {
    if (flags.eventMessageDescriptorSetFiles.isEmpty()) {
      return
    }

    val eventMessageFileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
      loadFileDescriptorSets(flags.eventMessageDescriptorSetFiles)
    add(ProtoReflection.buildDescriptors(eventMessageFileDescriptorSets, COMPILED_PROTOBUF_TYPES))
  }

  override fun run() {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
      )
    val kingdomPublicApiChannel: ManagedChannel =
      buildMutualTlsChannel(
        flags.kingdomPublicApiFlags.target,
        clientCerts,
        flags.kingdomPublicApiFlags.certHost,
      )
    val requisitionFulfillmentStubsByDuchyId =
      flags.requisitionFulfillmentServiceFlags.associate {
        val channel = buildMutualTlsChannel(it.target, clientCerts, it.certHost)
        val stub = RequisitionFulfillmentCoroutineStub(channel)
        it.duchyId to stub
      }

    val hmssVidIndexMap: InMemoryVidIndexMap? =
      if (flags.supportHmss) {
        InMemoryVidIndexMap.build(syntheticPopulationSpec.toPopulationSpec())
      } else {
        null
      }

    val randomSeed = flags.randomSeed
    val random =
      if (randomSeed != null) {
        Random(randomSeed)
      } else {
        Random.Default
      }

    val healthFile = flags.healthFile
    val health = if (healthFile == null) SettableHealth() else FileExistsHealth(healthFile)

    val eventQuery =
      object :
        SyntheticGeneratorEventQuery(syntheticPopulationSpec, typeRegistry, syntheticDataTimeZone) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec =
          this@AbstractEdpSimulatorRunner.getSyntheticDataSpec(eventGroup)
      }

    val edpSimulator: T =
      buildEdpSimulator(
        flags.mcResourceName,
        kingdomPublicApiChannel,
        requisitionFulfillmentStubsByDuchyId,
        clientCerts.trustedCertificates,
        eventQuery,
        hmssVidIndexMap,
        flags.logSketchDetails,
        MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval),
        health,
        random,
      )
    runBlocking {
      edpSimulator.ensureEventGroups()
      edpSimulator.run()
    }
  }

  protected fun loadFileDescriptorSets(files: Collection<File>) =
    Companion.loadFileDescriptorSets(files)

  companion object {
    const val SYNTHETIC_EVENT_GROUP_SPEC_MESSAGE_TYPE =
      "wfa.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec"

    init {
      check(TestEvent.getDescriptor().fullName == EdpSimulatorFlags.TEST_EVENT_MESSAGE_TYPE)
      check(
        SyntheticEventGroupSpec.getDescriptor().fullName == SYNTHETIC_EVENT_GROUP_SPEC_MESSAGE_TYPE
      )
    }

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    /**
     * [Descriptors.FileDescriptor]s of protobuf types known at compile-time that may be loaded from
     * a [DescriptorProtos.FileDescriptorSet].
     */
    val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence() +
          SyntheticEventGroupSpec.getDescriptor().file +
          EventAnnotationsProto.getDescriptor() +
          TestEvent.getDescriptor().file)
        .asIterable()

    private fun loadFileDescriptorSets(
      files: Collection<File>
    ): List<DescriptorProtos.FileDescriptorSet> {
      if (files.isEmpty()) {
        return emptyList() // Optimization.
      }

      return files.map { file ->
        file.inputStream().use { input ->
          DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
        }
      }
    }
  }
}
