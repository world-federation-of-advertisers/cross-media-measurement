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

package org.wfanet.measurement.loadtest.resourcesetup

import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.time.Clock
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createApiKeyRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.withIdToken
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens.generateIdToken
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMapKt
import org.wfanet.measurement.config.authorityKeyToPrincipalMap
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvider as InternalModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelRelease as InternalModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelRollout as InternalModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelSuite as InternalModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt
import org.wfanet.measurement.internal.kingdom.Population as InternalPopulation
import org.wfanet.measurement.internal.kingdom.PopulationKt
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt
import org.wfanet.measurement.internal.kingdom.account as internalAccount
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.dataProvider as internalDataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderDetails
import org.wfanet.measurement.internal.kingdom.eventTemplate
import org.wfanet.measurement.internal.kingdom.modelLine as internalModelLine
import org.wfanet.measurement.internal.kingdom.modelRelease as internalModelRelease
import org.wfanet.measurement.internal.kingdom.modelRollout as internalModelRollout
import org.wfanet.measurement.internal.kingdom.modelProvider as internalModelProvider
import org.wfanet.measurement.internal.kingdom.population as internalPopulation
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.PopulationKey
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.kingdom.service.api.v2alpha.fillCertificateFromDer
import org.wfanet.measurement.kingdom.service.api.v2alpha.parseCertificateDer
import org.wfanet.measurement.loadtest.common.ConsoleOutput
import org.wfanet.measurement.loadtest.common.FileOutput
import org.wfanet.measurement.loadtest.resourcesetup.ResourcesKt.resource

private val API_VERSION = Version.V2_ALPHA

/**
 * Maximum number of times that we will retry the first request to the Kingdom. We allow retries
 * because the resource setup step is usually executed immediately after the step that launches the
 * Kingdom, but the Kingdom typically takes some time to launch. Therefore, the first few attempts
 * to communicate with the Kingdom may fail because it is still initializing.
 */
private const val MAX_RETRY_COUNT = 30L

/** Amount of time in milliseconds between retries. */
private const val SLEEP_INTERVAL_MILLIS = 10000L

/** A Job preparing resources required for the correctness test. */
class ResourceSetup(
  private val internalAccountsClient: AccountsGrpcKt.AccountsCoroutineStub,
  private val internalDataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val accountsClient: AccountsCoroutineStub,
  private val apiKeysClient: ApiKeysCoroutineStub,
  private val internalCertificatesClient: CertificatesGrpcKt.CertificatesCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val runId: String,
  private val requiredDuchies: List<String>,
  private val bazelConfigName: String = DEFAULT_BAZEL_CONFIG_NAME,
  private val outputDir: File? = null,
  private val internalModelProvidersClient: ModelProvidersGrpcKt.ModelProvidersCoroutineStub? = null,
  private val internalPopulationsClient: PopulationsGrpcKt.PopulationsCoroutineStub? = null,
  private val internalModelSuitesClient: ModelSuitesGrpcKt.ModelSuitesCoroutineStub? = null,
  private val internalModelLinesClient: ModelLinesGrpcKt.ModelLinesCoroutineStub? = null,
  private val internalModelReleasesClient: ModelReleasesGrpcKt.ModelReleasesCoroutineStub? = null,
  private val internalModelRolloutsClient: ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub? = null,
) {
  data class MeasurementConsumerAndKey(
    val measurementConsumer: MeasurementConsumer,
    val apiAuthenticationKey: String,
  )

  /** Process to create resources. */
  suspend fun process(
    edpContents: List<EntityContent>,
    measurementConsumerContent: EntityContent,
    duchyCerts: List<DuchyCert>,
    pdpContent: EntityContent,
  ): List<Resources.Resource> {
    logger.info("Starting with RunID: $runId ...")
    val resources = mutableListOf<Resources.Resource>()

    // Step 0: Setup communications with Kingdom and create the Account.
    val internalAccount = createAccountWithRetries()

    // Step 1: Create the MC.
    val (measurementConsumer, apiAuthenticationKey) =
      createMeasurementConsumer(measurementConsumerContent, internalAccount)
    logger.info("Successfully created measurement consumer: ${measurementConsumer.name}")
    logger.info(
      "Successfully created measurement consumer signing certificate: " +
        measurementConsumer.certificate
    )
    logger.info(
      "API key for measurement consumer ${measurementConsumer.name}: $apiAuthenticationKey"
    )
    resources.add(
      resource {
        name = measurementConsumer.name
        this.measurementConsumer =
          ResourcesKt.ResourceKt.measurementConsumer {
            apiKey = apiAuthenticationKey
            certificate = measurementConsumer.certificate

            // Assume signing cert uses same issuer as TLS client cert.
            authorityKeyIdentifier =
              checkNotNull(measurementConsumerContent.signingKey.certificate.authorityKeyIdentifier)
          }
      }
    )

    // Step 2: Create the EDPs.
    edpContents.forEach {
      val internalDataProvider: InternalDataProvider = createInternalDataProvider(it)
      val dataProviderId: String = externalIdToApiId(internalDataProvider.externalDataProviderId)
      val dataProviderResourceName: String = DataProviderKey(dataProviderId).toName()
      val certificateId: String =
        externalIdToApiId(internalDataProvider.certificate.externalCertificateId)
      val dataProviderCertificateKeyName: String =
        DataProviderCertificateKey(dataProviderId, certificateId).toName()
      logger.info("Successfully created internal data provider: $dataProviderResourceName")
      resources.add(
        resource {
          name = dataProviderResourceName
          dataProvider =
            ResourcesKt.ResourceKt.dataProvider {
              displayName = it.displayName
              certificate = dataProviderCertificateKeyName
              // Assume signing cert uses same issuer as TLS client cert.
              authorityKeyIdentifier =
                checkNotNull(it.signingKey.certificate.authorityKeyIdentifier)
            }
        }
      )
    }

    // Step 3: Create certificate for each duchy.
    duchyCerts.forEach {
      val certificate = createDuchyCertificate(it)
      logger.info("Successfully created certificate ${certificate.name}")
      resources.add(
        resource {
          name = certificate.name
          duchyCertificate = ResourcesKt.ResourceKt.duchyCertificate { duchyId = it.duchyId }
        }
      )
    }

    // Step 4: Create the resources related to Population.
    val internalPopulationDataProvider = createInternalDataProvider(pdpContent)
    val dataProviderId: String = externalIdToApiId(internalPopulationDataProvider.externalDataProviderId)
    val dataProviderResourceName: String = DataProviderKey(dataProviderId).toName()
    val certificateId: String =
      externalIdToApiId(internalPopulationDataProvider.certificate.externalCertificateId)
    val dataProviderCertificateKeyName: String =
      DataProviderCertificateKey(dataProviderId, certificateId).toName()
    logger.info("Successfully created internal population data provider: $dataProviderResourceName")
    resources.add(
      resource {
        name = dataProviderResourceName
        populationDataProvider =
          ResourcesKt.ResourceKt.populationDataProvider {
            displayName = pdpContent.displayName
            certificate = dataProviderCertificateKeyName
            // Assume signing cert uses same issuer as TLS client cert.
            authorityKeyIdentifier =
              checkNotNull(pdpContent.signingKey.certificate.authorityKeyIdentifier)
          }
      }
    )

    val internalPopulation = createInternalPopulation(internalPopulationDataProvider)
    val populationId = externalIdToApiId(internalPopulation.externalPopulationId)
    val populationResourceName = PopulationKey(dataProviderId, populationId).toName()
    resources.add(
      resource {
        name = populationResourceName
        population = ResourcesKt.ResourceKt.population { this.populationId = populationId}
      }
    )
    logger.info("Successfully created internal population: $populationResourceName")

    val internalModelProvider = createInternalModelProvider()
    val internalModelSuite = createInternalModelSuite(internalModelProvider)
    val internalModelLine = createInternalModelLine(internalModelSuite)
    val modelProvider = externalIdToApiId(internalModelProvider.externalModelProviderId)
    val modelSuite = externalIdToApiId(internalModelSuite.externalModelSuiteId)
    val modelLineId = externalIdToApiId(internalModelLine.externalModelLineId)
    val modelLineResourceName = ModelLineKey(modelProvider, modelSuite, modelLineId).toName()

    val internalModelRelease = createInternalModelRelease(internalModelSuite, internalPopulation)
    createInternalModelRollout(internalModelLine, internalModelRelease)
    logger.info("Successfully created internal model line: $dataProviderResourceName")

    resources.add(
      resource {
        name = modelLineResourceName
        modelLine = ResourcesKt.ResourceKt.modelLine { this.modelLineId = modelLineId }
      }
    )

    withContext(Dispatchers.IO) { writeOutput(resources) }
    logger.info("Resource setup was successful.")

    return resources
  }

  @Blocking
  private fun writeOutput(resources: Iterable<Resources.Resource>) {
    val output = outputDir?.let { FileOutput(it) } ?: ConsoleOutput

    output.resolve(RESOURCES_OUTPUT_FILE).writer().use { writer ->
      TextFormat.printer().print(resources { this.resources += resources }, writer)
    }

    val akidMap = authorityKeyToPrincipalMap {
      for (resource in resources) {
        val akid =
          when (resource.resourceCase) {
            Resources.Resource.ResourceCase.DATA_PROVIDER ->
              resource.dataProvider.authorityKeyIdentifier
            Resources.Resource.ResourceCase.MEASUREMENT_CONSUMER ->
              resource.measurementConsumer.authorityKeyIdentifier
            Resources.Resource.ResourceCase.POPULATION_DATA_PROVIDER ->
              resource.populationDataProvider.authorityKeyIdentifier
            else -> continue
          }
        entries +=
          AuthorityKeyToPrincipalMapKt.entry {
            principalResourceName = resource.name
            authorityKeyIdentifier = akid
          }
      }
    }
    output.resolve(AKID_PRINCIPAL_MAP_FILE).writer().use { writer ->
      TextFormat.printer().print(akidMap, writer)
    }

    val measurementConsumerConfig = measurementConsumerConfigs {
      for (resource in resources) {
        when (resource.resourceCase) {
          Resources.Resource.ResourceCase.MEASUREMENT_CONSUMER ->
            configs.put(
              resource.name,
              measurementConsumerConfig {
                apiKey = resource.measurementConsumer.apiKey
                signingCertificateName = resource.measurementConsumer.certificate
                signingPrivateKeyPath = MEASUREMENT_CONSUMER_SIGNING_PRIVATE_KEY_PATH
              },
            )
          else -> continue
        }
      }
    }
    output.resolve(MEASUREMENT_CONSUMER_CONFIG_FILE).writer().use { writer ->
      TextFormat.printer().print(measurementConsumerConfig, writer)
    }

    val encryptionKeyPairConfig = encryptionKeyPairConfig {
      for (resource in resources) {
        when (resource.resourceCase) {
          Resources.Resource.ResourceCase.MEASUREMENT_CONSUMER ->
            principalKeyPairs +=
              EncryptionKeyPairConfigKt.principalKeyPairs {
                principal = resource.name
                keyPairs +=
                  EncryptionKeyPairConfigKt.keyPair {
                    publicKeyFile = MEASUREMENT_CONSUMER_ENCRYPTION_PUBLIC_KEY_PATH
                    privateKeyFile = MEASUREMENT_CONSUMER_ENCRYPTION_PRIVATE_KEY_PATH
                  }
              }
          else -> continue
        }
      }
    }
    output.resolve(ENCRYPTION_KEY_PAIR_CONFIG_FILE).writer().use { writer ->
      TextFormat.printer().print(encryptionKeyPairConfig, writer)
    }

    val configName = bazelConfigName
    output.resolve(BAZEL_RC_FILE).writer().use { writer ->
      for (resource in resources) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (resource.resourceCase) {
          Resources.Resource.ResourceCase.DATA_PROVIDER -> {
            val displayName = resource.dataProvider.displayName
            writer.appendLine("build:$configName --define=${displayName}_name=${resource.name}")
            writer.appendLine(
              "build:$configName --define=${displayName}_cert_name=${resource.dataProvider.certificate}"
            )
          }
          Resources.Resource.ResourceCase.MEASUREMENT_CONSUMER -> {
            with(resource) {
              writer.appendLine("build:$configName --define=mc_name=$name")
              writer.appendLine(
                "build:$configName --define=mc_api_key=${measurementConsumer.apiKey}"
              )
              writer.appendLine(
                "build:$configName --define=mc_cert_name=${measurementConsumer.certificate}"
              )
            }
          }
          Resources.Resource.ResourceCase.DUCHY_CERTIFICATE -> {
            val duchyId = resource.duchyCertificate.duchyId
            writer.appendLine("build:$configName --define=${duchyId}_cert_name=${resource.name}")
          }
          Resources.Resource.ResourceCase.POPULATION -> {
            val populationId = resource.population.populationId
            writer.appendLine("build:$configName --define=${populationId}_cert_name=${resource.name}")
          }
          Resources.Resource.ResourceCase.POPULATION_DATA_PROVIDER -> {
            val displayName = resource.populationDataProvider.displayName
            writer.appendLine("build:$configName --define=${displayName}_name=${resource.name}")
            writer.appendLine(
              "build:$configName --define=${displayName}_cert_name=${resource.dataProvider.certificate}"
            )
          }
          Resources.Resource.ResourceCase.MODEL_LINE -> {
            val modelLineId = resource.modelLine.modelLineId
            writer.appendLine("build:$configName --define=${modelLineId}_cert_name=${resource.name}")
          }
          Resources.Resource.ResourceCase.RESOURCE_NOT_SET -> error("Bad resource case")
        }
      }
    }
  }

  /** Create an internal dataProvider, and return its corresponding public API resource name. */
  suspend fun createInternalDataProvider(dataProviderContent: EntityContent): InternalDataProvider {
    val encryptionPublicKey = dataProviderContent.encryptionPublicKey
    val signedPublicKey =
      signEncryptionPublicKey(encryptionPublicKey, dataProviderContent.signingKey)
    return try {
      internalDataProvidersClient.createDataProvider(
        internalDataProvider {
          certificate =
            parseCertificateDer(dataProviderContent.signingKey.certificate.encoded.toByteString())
          details = dataProviderDetails {
            apiVersion = API_VERSION.string
            publicKey = signedPublicKey.message.value
            publicKeySignature = signedPublicKey.signature
            publicKeySignatureAlgorithmOid = signedPublicKey.signatureAlgorithmOid
          }
          requiredExternalDuchyIds += requiredDuchies
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error creating DataProvider", e)
    }
  }

  /** Create an internal modelProvider. */
  suspend fun createInternalModelProvider(): InternalModelProvider {
    require(internalModelProvidersClient != null)
    return try {
      internalModelProvidersClient.createModelProvider(internalModelProvider {})
    } catch (e: StatusException) {
      throw Exception("Error creating ModelProvider", e)
    }
  }

  suspend fun createInternalPopulation(dataProvider: InternalDataProvider): InternalPopulation {
    require(internalPopulationsClient != null)
    return internalPopulationsClient.createPopulation(
      internalPopulation {
        externalDataProviderId = dataProvider.externalDataProviderId
        description = "DESCRIPTION"
        populationBlob = PopulationKt.populationBlob { modelBlobUri = "BLOB_URI" }
        eventTemplate = eventTemplate { fullyQualifiedType = "TYPE" }
      }
    )
  }

  /** Create a modelSuite. */
  suspend fun createInternalModelSuite(modelProvider: InternalModelProvider): InternalModelSuite {
    require(internalModelSuitesClient != null)
    return internalModelSuitesClient.createModelSuite(
      org.wfanet.measurement.internal.kingdom.modelSuite {
        externalModelProviderId = modelProvider.externalModelProviderId
        displayName = "displayName"
        description = "description"
      }
    )
  }

  suspend fun createInternalModelLine(modelSuite: InternalModelSuite): InternalModelLine {
    require(internalModelLinesClient != null)
    return internalModelLinesClient.createModelLine(
      internalModelLine {
        externalModelProviderId = modelSuite.externalModelProviderId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        displayName = "displayName"
        description = "description"
        activeStartTime = Instant.now().plusSeconds(2000L).toProtoTime()
        type = InternalModelLine.Type.DEV
      }
    )
  }

  suspend fun createInternalModelRelease(
    modelSuite: InternalModelSuite,
    population: InternalPopulation,
  ): InternalModelRelease {
    require(internalModelReleasesClient != null)
    return internalModelReleasesClient.createModelRelease(
      internalModelRelease {
        externalModelProviderId = modelSuite.externalModelProviderId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        externalDataProviderId = population.externalDataProviderId
        externalPopulationId = population.externalPopulationId
      }
    )
  }

  suspend fun createInternalModelRollout(modelLine: InternalModelLine, modelRelease: InternalModelRelease): InternalModelRollout {
    require(internalModelRolloutsClient != null)
    return internalModelRolloutsClient.createModelRollout(
      internalModelRollout {
        externalModelProviderId = modelLine.externalModelProviderId
        externalModelSuiteId = modelLine.externalModelSuiteId
        externalModelLineId = modelLine.externalModelLineId
        rolloutPeriodStartTime = Instant.now().plusSeconds(100L).toProtoTime()
        rolloutPeriodEndTime = Instant.now().plusSeconds(200L).toProtoTime()
        externalModelReleaseId = modelRelease.externalModelReleaseId
      }
    )
  }


  suspend fun createAccountWithRetries(): InternalAccount {
    // The initial account is created via the Kingdom Internal API by the Kingdom operator.
    // This is our first attempt to contact the Kingdom.  If it fails, we will retry it.
    // This is to allow the Kingdom more time to start up.

    fun isRetriable(e: Throwable) =
      (e is StatusException) && (e.status.code == Status.Code.UNAVAILABLE)

    // TODO(@SanjayVas):  Remove this polling behavior after the readiness probe for the Kingdom
    // is fixed.
    var retryCount = 0L
    val internalAccount =
      flow { emit(internalAccountsClient.createAccount(internalAccount {})) }
        .retry(MAX_RETRY_COUNT) { e ->
          isRetriable(e).also {
            if (it) {
              retryCount += 1
              logger.info(
                "Try #$retryCount to communicate with Kingdom failed.  " +
                  "Retrying in ${SLEEP_INTERVAL_MILLIS / 1000} seconds ..."
              )
              delay(SLEEP_INTERVAL_MILLIS)
            }
          }
        }
        .catch { cause ->
          if (cause is StatusException) {
            throw Exception("Error creating account", cause)
          }
          throw cause
        }
        .single()
    return internalAccount
  }

  suspend fun createMeasurementConsumer(
    measurementConsumerContent: EntityContent,
    internalAccount: InternalAccount,
  ): MeasurementConsumerAndKey {
    val accountName = AccountKey(externalIdToApiId(internalAccount.externalAccountId)).toName()
    val accountActivationToken = externalIdToApiId(internalAccount.activationToken)
    val mcCreationToken =
      try {
        externalIdToApiId(
          internalAccountsClient
            .createMeasurementConsumerCreationToken(
              createMeasurementConsumerCreationTokenRequest {}
            )
            .measurementConsumerCreationToken
        )
      } catch (e: StatusException) {
        throw Exception("Error creating MC creation token", e)
      }

    // Account activation and MC creation are done via the public API.
    val authenticationResponse =
      try {
        accountsClient.authenticate(authenticateRequest { issuer = "https://self-issued.me" })
      } catch (e: StatusException) {
        throw Exception("Error authenticating account", e)
      }
    val idToken =
      generateIdToken(authenticationResponse.authenticationRequestUri, Clock.systemUTC())
    try {
      accountsClient
        .withIdToken(idToken)
        .activateAccount(
          activateAccountRequest {
            name = accountName
            activationToken = accountActivationToken
          }
        )
    } catch (e: StatusException) {
      throw Exception("Error activating account $accountName", e)
    }

    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer {
        certificateDer = measurementConsumerContent.signingKey.certificate.encoded.toByteString()
        publicKey =
          signEncryptionPublicKey(
            measurementConsumerContent.encryptionPublicKey,
            measurementConsumerContent.signingKey,
          )
        displayName = measurementConsumerContent.displayName
        measurementConsumerCreationToken = mcCreationToken
      }
    }
    val measurementConsumer =
      try {
        measurementConsumersClient.withIdToken(idToken).createMeasurementConsumer(request)
      } catch (e: StatusException) {
        throw Exception("Error creating MC", e)
      }

    // API key for MC is created to act as MC caller
    val apiAuthenticationKey =
      try {
        apiKeysClient
          .withIdToken(idToken)
          .createApiKey(
            createApiKeyRequest {
              parent = measurementConsumer.name
              apiKey = apiKey { nickname = "test_key" }
            }
          )
          .authenticationKey
      } catch (e: StatusException) {
        throw Exception("Error creating API key for ${measurementConsumer.name}", e)
      }

    return MeasurementConsumerAndKey(measurementConsumer, apiAuthenticationKey)
  }

  suspend fun createDuchyCertificate(duchyCert: DuchyCert): Certificate {
    val internalCertificate =
      try {
        internalCertificatesClient.createCertificate(
          internalCertificate {
            fillCertificateFromDer(duchyCert.consentSignalCertificateDer)
            externalDuchyId = duchyCert.duchyId
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating certificate for Duchy ${duchyCert.duchyId}", e)
      }

    return certificate {
      name =
        DuchyCertificateKey(
            internalCertificate.externalDuchyId,
            externalIdToApiId(internalCertificate.externalCertificateId),
          )
          .toName()
      x509Der = internalCertificate.details.x509Der
    }
  }

  companion object {
    const val DEFAULT_BAZEL_CONFIG_NAME = "halo"
    const val RESOURCES_OUTPUT_FILE = "resources.textproto"
    const val AKID_PRINCIPAL_MAP_FILE = "authority_key_identifier_to_principal_map.textproto"
    const val BAZEL_RC_FILE = "resource-setup.bazelrc"
    const val MEASUREMENT_CONSUMER_CONFIG_FILE = "measurement_consumer_config.textproto"
    const val ENCRYPTION_KEY_PAIR_CONFIG_FILE = "encryption_key_pair_config.textproto"
    const val MEASUREMENT_CONSUMER_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"
    const val MEASUREMENT_CONSUMER_ENCRYPTION_PUBLIC_KEY_PATH = "mc_enc_public.tink"
    const val MEASUREMENT_CONSUMER_ENCRYPTION_PRIVATE_KEY_PATH = "mc_enc_private.tink"

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

/** Relevant data required to create entity like EDP or MC. */
data class EntityContent(
  /** The display name of the entity. */
  val displayName: String,
  /** The consent signaling encryption key. */
  val encryptionPublicKey: EncryptionPublicKey,
  /** The consent signaling signing key. */
  val signingKey: SigningKeyHandle,
)

data class DuchyCert(
  /** The external duchy Id. */
  val duchyId: String,
  /** The consent signaling certificate in DER format. */
  val consentSignalCertificateDer: ByteString,
)
