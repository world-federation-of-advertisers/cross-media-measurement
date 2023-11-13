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

package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import io.grpc.Channel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.CompositionMechanism
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.InMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.api.v2alpha.tools.CertificateRegistrar
import org.wfanet.measurement.loadtest.dataprovider.EdpData
import org.wfanet.measurement.loadtest.dataprovider.EdpSimulator
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery

/** An in process EDP simulator. */
class InProcessEdpSimulator(
  val displayName: String,
  resourceName: String,
  certResourceName: String,
  mcResourceName: String,
  kingdomPublicApiChannel: Channel,
  duchyPublicApiChannel: Channel,
  trustedCertificates: Map<ByteString, X509Certificate>,
  private val syntheticDataSpec: SyntheticEventGroupSpec,
  coroutineContext: CoroutineContext = Dispatchers.Default,
) {
  private val loggingName = "${javaClass.simpleName} $displayName"
  private val backgroundScope =
    CoroutineScope(
      coroutineContext +
        CoroutineName(loggingName) +
        CoroutineExceptionHandler { _, e ->
          logger.log(Level.SEVERE, e) { "Error in $loggingName" }
        }
    )

  private val certificatesStub =
    CertificatesCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName)

  private val delegate =
    EdpSimulator(
      edpData = createEdpData(displayName, resourceName),
      measurementConsumerName = mcResourceName,
      measurementConsumersStub =
        MeasurementConsumersCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
      certificatesStub = certificatesStub,
      eventGroupsStub =
        EventGroupsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
      eventGroupMetadataDescriptorsStub =
        EventGroupMetadataDescriptorsCoroutineStub(kingdomPublicApiChannel)
          .withPrincipalName(resourceName),
      requisitionsStub =
        RequisitionsCoroutineStub(kingdomPublicApiChannel).withPrincipalName(resourceName),
      requisitionFulfillmentStub =
        RequisitionFulfillmentCoroutineStub(duchyPublicApiChannel).withPrincipalName(resourceName),
      eventQuery =
        object :
          SyntheticGeneratorEventQuery(
            SyntheticGenerationSpecs.POPULATION_SPEC,
            TestEvent.getDescriptor()
          ) {
          override fun getSyntheticDataSpec(eventGroup: EventGroup) = syntheticDataSpec
        },
      throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
      privacyBudgetManager =
        PrivacyBudgetManager(
          PrivacyBucketFilter(TestPrivacyBucketMapper()),
          InMemoryBackingStore(),
          100.0f,
          100.0f
        ),
      trustedCertificates = trustedCertificates,
      random = random,
      compositionMechanism = COMPOSITION_MECHANISM,
    )

  private lateinit var edpJob: Job

  fun start() {
    edpJob = backgroundScope.launch { delegate.run() }
  }

  suspend fun stop() {
    edpJob.cancelAndJoin()
  }

  suspend fun ensureEventGroup() = delegate.ensureEventGroup(syntheticDataSpec)

  private val SECRET_FILES_PATH: Path =
    checkNotNull(
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )
    )

  private val edpPublicKeySigningKey =
    loadSigningKey("${displayName}_cs_cert.der", "${displayName}_cs_private.der")
  private val separateResultSigningKey =
    if (Files.exists(SECRET_FILES_PATH.resolve("${displayName}_result_cs_cert.der"))) {
      loadSigningKey("${displayName}_result_cs_cert.der", "${displayName}_result_cs_private.der")
    } else {
      null
    }
  private val separateResultCertificateKey =
    if (separateResultSigningKey != null) {
      runBlocking {
        DataProviderCertificateKey.fromName(
          CertificateRegistrar(
              parentResourceName = resourceName,
              certificatesStub = certificatesStub,
            )
            .registerCertificate(separateResultSigningKey.certificate)
            .name
        )!!
      }
    } else {
      null
    }

  private val dataProviderCertificateKey = DataProviderCertificateKey.fromName(certResourceName)!!

  private val registeredCertificates by lazy {
    val certificateMap = mutableMapOf(dataProviderCertificateKey to edpPublicKeySigningKey)
    if (separateResultSigningKey != null && separateResultCertificateKey != null) {
      certificateMap.put(separateResultCertificateKey, separateResultSigningKey)
    }
    certificateMap
  }

  /** Builds a [EdpData] object for the Edp with a certain [displayName] and [resourceName]. */
  @Blocking
  private fun createEdpData(displayName: String, resourceName: String) =
    EdpData(
      name = resourceName,
      displayName = displayName,
      encryptionKey = loadEncryptionPrivateKey("${displayName}_enc_private.tink"),
      resultSigningResource = separateResultCertificateKey ?: dataProviderCertificateKey,
      registeredCertificates = registeredCertificates,
    )

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val RANDOM_SEED: Long = 1
    private val random = Random(RANDOM_SEED)
    private val COMPOSITION_MECHANISM = CompositionMechanism.ACDP
  }
}
