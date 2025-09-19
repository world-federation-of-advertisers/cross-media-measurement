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

import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
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
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
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
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusTeeFulfillRequisitionRequestBuilder
import picocli.CommandLine

/** The base class of the EdpSimulator runner. */
@CommandLine.Command(mixinStandardHelpOptions = true)
abstract class AbstractEdpSimulatorRunner : Runnable {
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

  protected val eventMessageDescriptor: Descriptors.Descriptor
    get() = typeRegistry.getNonNullDescriptorForTypeUrl(syntheticPopulationSpec.eventMessageTypeUrl)

  protected val edpData: DataProviderData by lazy {
    val signingKeyHandle =
      loadSigningKey(flags.edpCsCertificateDerFile, flags.edpCsPrivateKeyDerFile)
    val certificateKey =
      DataProviderCertificateKey.fromName(flags.dataProviderCertificateResourceName)!!

    DataProviderData(
      flags.dataProviderResourceName,
      loadPrivateKey(flags.edpEncryptionPrivateKeyset),
      signingKeyHandle,
      certificateKey,
    )
  }

  protected val syntheticDataTimeZone: ZoneId
    get() = flags.syntheticDataTimeZone

  protected abstract val eventGroupsOptions: List<AbstractEdpSimulator.EventGroupOptions>

  open val additionalMessageTypes: Collection<Descriptors.Descriptor> = emptyList()

  abstract fun buildEdpSimulator(
    edpDisplayName: String,
    measurementConsumerName: String,
    kingdomPublicApiChannel: ManagedChannel,
    requisitionFulfillmentStubsByDuchyId:
      Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
    trustedCertificates: Map<ByteString, X509Certificate>,
    eventQuery: SyntheticGeneratorEventQuery,
    vidIndexMap: InMemoryVidIndexMap?,
    logSketchDetails: Boolean,
    throttler: MinimumIntervalThrottler,
    health: SettableHealth,
    random: Random,
    trusTeeEncryptionParams: TrusTeeFulfillRequisitionRequestBuilder.EncryptionParams?,
  ): AbstractEdpSimulator

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
        val stub = RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub(channel)
        it.duchyId to stub
      }

    val vidIndexMap: InMemoryVidIndexMap? =
      if (flags.supportHmss) {
        InMemoryVidIndexMap.build(syntheticPopulationSpec.toPopulationSpecWithoutAttributes())
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
      EventQuery(
        flags.dataProviderDisplayName,
        syntheticPopulationSpec,
        syntheticDataTimeZone,
        typeRegistry.getDescriptorForTypeUrl(syntheticPopulationSpec.eventMessageTypeUrl),
        eventGroupsOptions,
      )

    val params = flags.trusTeeParams
    val trusTeeEncryptionParams: TrusTeeFulfillRequisitionRequestBuilder.EncryptionParams? =
      if (params != null) {
        // TODO(@roaminggypsy): Swap the KMS client for local cluster runs.
        TrusTeeFulfillRequisitionRequestBuilder.EncryptionParams(
          GcpKmsClient(),
          params.kmsKekUri,
          params.workloadIdentityProvider,
          params.impersonatedServiceAccount,
        )
      } else {
        null
      }

    val edpSimulator: AbstractEdpSimulator =
      buildEdpSimulator(
        flags.dataProviderDisplayName,
        flags.mcResourceName,
        kingdomPublicApiChannel,
        requisitionFulfillmentStubsByDuchyId,
        clientCerts.trustedCertificates,
        eventQuery,
        vidIndexMap,
        flags.logSketchDetails,
        MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval),
        health,
        random,
        trusTeeEncryptionParams,
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

  protected class EventQuery(
    edpDisplayName: String,
    syntheticPopulationSpec: SyntheticPopulationSpec,
    syntheticDataTimeZone: ZoneId,
    eventMessageDescriptor: Descriptors.Descriptor,
    eventGroupsOptions: Iterable<AbstractEdpSimulator.EventGroupOptions>,
  ) :
    SyntheticGeneratorEventQuery(
      syntheticPopulationSpec,
      eventMessageDescriptor,
      syntheticDataTimeZone,
    ) {
    private val syntheticDataSpecByReferenceId: Map<String, SyntheticEventGroupSpec>

    init {
      val referenceIdPrefix = AbstractEdpSimulator.getEventGroupReferenceIdPrefix(edpDisplayName)
      syntheticDataSpecByReferenceId =
        eventGroupsOptions
          .associateBy { referenceIdPrefix + it.referenceIdSuffix }
          .mapValues { it.value.syntheticDataSpec }
    }

    override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
      return syntheticDataSpecByReferenceId.getValue(eventGroup.eventGroupReferenceId)
    }
  }
}

fun TypeRegistry.getNonNullDescriptorForTypeUrl(typeUrl: String): Descriptors.Descriptor =
  checkNotNull(getDescriptorForTypeUrl(typeUrl)) { "Descriptor not found for type URL $typeUrl" }
