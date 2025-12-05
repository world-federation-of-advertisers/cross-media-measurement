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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.gson.JsonParser
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.type.Interval
import com.google.type.interval
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.tink.PublicJwkHandle
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens.generateIdToken
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.openid.createRequestUri
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt.openIdConnectIdentity
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementDetails
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.PopulationDetails
import org.wfanet.measurement.internal.kingdom.PopulationDetailsKt
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.activateAccountRequest
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.certificateDetails
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementRequest
import org.wfanet.measurement.internal.kingdom.createPopulationRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderDetails
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.generateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.measurementConsumerDetails
import org.wfanet.measurement.internal.kingdom.measurementDetails
import org.wfanet.measurement.internal.kingdom.modelLine
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.modelRelease
import org.wfanet.measurement.internal.kingdom.modelSuite
import org.wfanet.measurement.internal.kingdom.population
import org.wfanet.measurement.internal.kingdom.populationDetails
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds

private const val API_VERSION = "v2alpha"

class Population(val clock: Clock, val idGenerator: IdGenerator) {
  companion object {
    private val VALID_ACTIVE_START_TIME = Instant.now().minusSeconds(100L)
    private val VALID_ACTIVE_END_TIME = Instant.now().plusSeconds(2000L)
    val AGGREGATOR_DUCHY =
      DuchyIds.Entry(1, "aggregator", VALID_ACTIVE_START_TIME..VALID_ACTIVE_END_TIME)
    val WORKER1_DUCHY = DuchyIds.Entry(2, "worker1", VALID_ACTIVE_START_TIME..VALID_ACTIVE_END_TIME)
    val WORKER2_DUCHY = DuchyIds.Entry(3, "worker2", VALID_ACTIVE_START_TIME..VALID_ACTIVE_END_TIME)
    val DUCHIES = listOf(AGGREGATOR_DUCHY, WORKER1_DUCHY, WORKER2_DUCHY)

    val DEFAULT_POPULATION_SPEC =
      PopulationDetailsKt.populationSpec {
        subpopulations +=
          PopulationDetailsKt.PopulationSpecKt.subPopulation {
            vidRanges +=
              PopulationDetailsKt.PopulationSpecKt.vidRange {
                startVid = 1
                endVidInclusive = 10_000
              }
          }
      }
  }

  private fun buildRequestCertificate(
    derUtf8: String,
    skidUtf8: String,
    notValidBefore: Instant,
    notValidAfter: Instant,
  ) = certificate { fillRequestCertificate(derUtf8, skidUtf8, notValidBefore, notValidAfter) }

  private fun CertificateKt.Dsl.fillRequestCertificate(
    derUtf8: String,
    skidUtf8: String,
    notValidBefore: Instant,
    notValidAfter: Instant,
  ) {
    this.notValidBefore = notValidBefore.toProtoTime()
    this.notValidAfter = notValidAfter.toProtoTime()
    subjectKeyIdentifier = skidUtf8.toByteStringUtf8()
    details = certificateDetails { x509Der = derUtf8.toByteStringUtf8() }
  }

  suspend fun createMeasurementConsumer(
    measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    accountsService: AccountsCoroutineImplBase,
    notValidBefore: Instant = clock.instant(),
    notValidAfter: Instant = notValidBefore.plus(365L, ChronoUnit.DAYS),
  ): MeasurementConsumer {
    val account = createAccount(accountsService)
    activateAccount(accountsService, account)
    val measurementConsumerCreationTokenHash =
      Hashing.hashSha256(createMeasurementConsumerCreationToken(accountsService))
    return measurementConsumersService.createMeasurementConsumer(
      createMeasurementConsumerRequest {
        measurementConsumer = measurementConsumer {
          certificate =
            buildRequestCertificate(
              "MC cert",
              "MC SKID " + idGenerator.generateExternalId().value,
              notValidBefore,
              notValidAfter,
            )
          details = measurementConsumerDetails {
            apiVersion = API_VERSION
            publicKey = "MC public key".toByteStringUtf8()
            publicKeySignature = "MC public key signature".toByteStringUtf8()
            publicKeySignatureAlgorithmOid = "2.9999"
          }
        }
        externalAccountId = account.externalAccountId
        this.measurementConsumerCreationTokenHash = measurementConsumerCreationTokenHash
      }
    )
  }

  suspend fun createDataProvider(
    dataProvidersService: DataProvidersCoroutineImplBase,
    requiredDuchiesList: List<String> =
      listOf(WORKER1_DUCHY.externalDuchyId, WORKER2_DUCHY.externalDuchyId),
    notValidBefore: Instant = clock.instant(),
    notValidAfter: Instant = notValidBefore.plus(365L, ChronoUnit.DAYS),
    customize: (DataProviderKt.Dsl.() -> Unit)? = null,
  ): DataProvider {
    return dataProvidersService.createDataProvider(
      dataProvider {
        certificate =
          buildRequestCertificate(
            "EDP cert",
            "EDP SKID " + idGenerator.generateExternalId().value,
            notValidBefore,
            notValidAfter,
          )
        details = dataProviderDetails {
          apiVersion = API_VERSION
          publicKey = "EDP public key".toByteStringUtf8()
          publicKeySignature = "EDP public key signature".toByteStringUtf8()
          publicKeySignatureAlgorithmOid = "2.9999"
        }
        requiredExternalDuchyIds += requiredDuchiesList
        customize?.invoke(this)
      }
    )
  }

  suspend fun createModelProvider(
    modelProvidersService: ModelProvidersCoroutineImplBase
  ): ModelProvider {

    return modelProvidersService.createModelProvider(modelProvider {})
  }

  private suspend fun createMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    dataProviders: Map<Long, Measurement.DataProviderValue> = mapOf(),
    details: MeasurementDetails,
  ): Measurement {
    return measurementsService.createMeasurement(
      createMeasurementRequest {
        measurement = measurement {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          this.providedMeasurementId = providedMeasurementId
          externalMeasurementConsumerCertificateId =
            measurementConsumer.certificate.externalCertificateId
          this.details = details
          this.dataProviders.putAll(dataProviders)
        }
      }
    )
  }

  suspend fun createModelRelease(
    modelSuite: ModelSuite,
    population: Population,
    modelReleasesService: ModelReleasesCoroutineImplBase,
  ): ModelRelease {
    val modelRelease = modelRelease {
      externalModelProviderId = modelSuite.externalModelProviderId
      externalModelSuiteId = modelSuite.externalModelSuiteId
      externalDataProviderId = population.externalDataProviderId
      externalPopulationId = population.externalPopulationId
    }
    return modelReleasesService.createModelRelease(modelRelease)
  }

  suspend fun createPopulation(
    dataProvider: DataProvider,
    populationsService: PopulationsGrpcKt.PopulationsCoroutineImplBase,
    populationSpec: PopulationDetails.PopulationSpec = DEFAULT_POPULATION_SPEC,
  ): Population {
    val population =
      populationsService.createPopulation(
        createPopulationRequest {
          population = population {
            externalDataProviderId = dataProvider.externalDataProviderId
            description = "DESCRIPTION"
            details = populationDetails { this.populationSpec = populationSpec }
          }
        }
      )
    return population
  }

  /**
   * Creates a [ModelLine] and [ModelSuite], along with optionally creating a holdback [ModelLine].
   *
   * Prefer calling the other overload of this method to create the [ModelSuite] separately.
   */
  suspend fun createModelLine(
    modelProvidersService: ModelProvidersCoroutineImplBase,
    modelSuitesService: ModelSuitesCoroutineImplBase,
    modelLinesService: ModelLinesCoroutineImplBase,
    modelLineType: ModelLine.Type = ModelLine.Type.PROD,
    createHoldbackModelLine: Boolean = false,
  ): ModelLine {
    val modelSuite: ModelSuite = createModelSuite(modelProvidersService, modelSuitesService)

    val holdbackModelLine: ModelLine? =
      if (createHoldbackModelLine) {
        createModelLine(modelLinesService, modelSuite, modelLineType = ModelLine.Type.HOLDBACK)
      } else {
        null
      }

    return createModelLine(
      modelLinesService,
      modelSuite,
      modelLineType = modelLineType,
      holdbackModelLine = holdbackModelLine,
    )
  }

  suspend fun createModelLine(
    modelLinesService: ModelLinesCoroutineImplBase,
    modelSuite: ModelSuite,
    modelLineType: ModelLine.Type = ModelLine.Type.PROD,
    holdbackModelLine: ModelLine? = null,
    activeInterval: Interval = interval {
      startTime = Instant.now().plusSeconds(2000L).toProtoTime()
    },
  ): ModelLine {
    return modelLinesService.createModelLine(
      modelLine {
        externalModelProviderId = modelSuite.externalModelProviderId
        externalModelSuiteId = modelSuite.externalModelSuiteId
        displayName = "displayName"
        description = "description"
        activeStartTime = activeInterval.startTime
        if (activeInterval.hasEndTime()) {
          activeEndTime = activeInterval.endTime
        }
        if (holdbackModelLine != null) {
          externalHoldbackModelLineId = holdbackModelLine.externalModelLineId
        }
        type = modelLineType
      }
    )
  }

  /**
   * Creates a [ModelProvider] and [ModelSuite].
   *
   * Prefer creating the [ModelProvider] separately rather than calling this overload.
   */
  suspend fun createModelSuite(
    modelProvidersService: ModelProvidersCoroutineImplBase,
    modelSuitesService: ModelSuitesCoroutineImplBase,
  ): ModelSuite {

    val modelProvider = modelProvidersService.createModelProvider(modelProvider {})
    return createModelSuite(modelSuitesService, modelProvider)
  }

  /** Creates a [ModelSuite] under [modelProvider]. */
  suspend fun createModelSuite(
    modelSuitesService: ModelSuitesCoroutineImplBase,
    modelProvider: ModelProvider,
  ): ModelSuite {
    return modelSuitesService.createModelSuite(
      modelSuite {
        externalModelProviderId = modelProvider.externalModelProviderId
        displayName = "displayName"
        description = "description"
      }
    )
  }

  suspend fun createLlv2Measurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    dataProviders: Map<Long, Measurement.DataProviderValue> = mapOf(),
  ): Measurement {
    val details = measurementDetails {
      apiVersion = API_VERSION
      measurementSpec = "MeasurementSpec".toByteStringUtf8()
      measurementSpecSignature = "MeasurementSpec signature".toByteStringUtf8()
      measurementSpecSignatureAlgorithmOid = "2.9999"
      duchyProtocolConfig = duchyProtocolConfig {
        liquidLegionsV2 = DuchyProtocolConfigKt.liquidLegionsV2 {}
      }
      protocolConfig = protocolConfig { liquidLegionsV2 = ProtocolConfigKt.liquidLegionsV2 {} }
    }
    return createMeasurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders,
      details,
    )
  }

  suspend fun createLlv2Measurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    vararg dataProviders: DataProvider,
  ): Measurement {
    return createLlv2Measurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders.associate { it.externalDataProviderId to it.toDataProviderValue() },
    )
  }

  suspend fun createHmssMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    dataProviders: Map<Long, Measurement.DataProviderValue> = mapOf(),
  ): Measurement {
    val details = measurementDetails {
      apiVersion = API_VERSION
      measurementSpec = "MeasurementSpec".toByteStringUtf8()
      measurementSpecSignature = "MeasurementSpec signature".toByteStringUtf8()
      measurementSpecSignatureAlgorithmOid = "2.9999"
      protocolConfig = protocolConfig {
        honestMajorityShareShuffle = ProtocolConfig.HonestMajorityShareShuffle.getDefaultInstance()
      }
    }
    return createMeasurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders,
      details,
    )
  }

  suspend fun createHmssMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    vararg dataProviders: DataProvider,
  ): Measurement {
    return createHmssMeasurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders.associate { it.externalDataProviderId to it.toDataProviderValue() },
    )
  }

  suspend fun createTrusTeeMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    dataProviders: Map<Long, Measurement.DataProviderValue> = mapOf(),
  ): Measurement {
    val details = measurementDetails {
      apiVersion = API_VERSION
      measurementSpec = "MeasurementSpec".toByteStringUtf8()
      measurementSpecSignature = "MeasurementSpec signature".toByteStringUtf8()
      measurementSpecSignatureAlgorithmOid = "2.9999"
      protocolConfig = protocolConfig { trusTee = ProtocolConfig.TrusTee.getDefaultInstance() }
    }
    return createMeasurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders,
      details,
    )
  }

  suspend fun createTrusTeeMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    vararg dataProviders: DataProvider,
  ): Measurement {
    return createTrusTeeMeasurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders.associate { it.externalDataProviderId to it.toDataProviderValue() },
    )
  }

  suspend fun createDirectMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    dataProviders: Map<Long, Measurement.DataProviderValue> = mapOf(),
  ): Measurement {
    val details = measurementDetails {
      apiVersion = API_VERSION
      measurementSpec = "MeasurementSpec".toByteStringUtf8()
      measurementSpecSignature = "MeasurementSpec signature".toByteStringUtf8()
      measurementSpecSignatureAlgorithmOid = "2.9999"
      protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
    }
    return createMeasurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders,
      details,
    )
  }

  suspend fun createDirectMeasurement(
    measurementsService: MeasurementsCoroutineImplBase,
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    vararg dataProviders: DataProvider,
  ): Measurement {
    return createDirectMeasurement(
      measurementsService,
      measurementConsumer,
      providedMeasurementId,
      dataProviders.associate { it.externalDataProviderId to it.toDataProviderValue() },
    )
  }

  suspend fun createDuchyCertificate(
    certificatesService: CertificatesCoroutineImplBase,
    externalDuchyId: String,
    notValidBefore: Instant = clock.instant(),
    notValidAfter: Instant = notValidBefore.plus(365L, ChronoUnit.DAYS),
  ): Certificate {
    return certificatesService.createCertificate(
      certificate {
        this.externalDuchyId = externalDuchyId
        fillRequestCertificate(
          "Duchy cert",
          "Duchy SKID " + idGenerator.generateExternalId().value,
          notValidBefore,
          notValidAfter,
        )
      }
    )
  }

  suspend fun createMeasurementConsumerCertificate(
    certificatesService: CertificatesCoroutineImplBase,
    parent: MeasurementConsumer,
    notValidBefore: Instant = clock.instant(),
    notValidAfter: Instant = notValidBefore.plus(365L, ChronoUnit.DAYS),
  ): Certificate {
    return certificatesService.createCertificate(
      certificate {
        externalMeasurementConsumerId = parent.externalMeasurementConsumerId
        fillRequestCertificate(
          "MC cert",
          "MC SKID " + idGenerator.generateExternalId().value,
          notValidBefore,
          notValidAfter,
        )
      }
    )
  }

  suspend fun createDataProviderCertificate(
    certificatesService: CertificatesCoroutineImplBase,
    parent: DataProvider,
    notValidBefore: Instant = clock.instant(),
    notValidAfter: Instant = notValidBefore.plus(365L, ChronoUnit.DAYS),
  ): Certificate {
    return certificatesService.createCertificate(
      certificate {
        externalDataProviderId = parent.externalDataProviderId
        fillRequestCertificate(
          "EDP cert",
          "EDP SKID " + idGenerator.generateExternalId().value,
          notValidBefore,
          notValidAfter,
        )
      }
    )
  }

  /** Creates an [Account] and returns it. */
  suspend fun createAccount(
    accountsService: AccountsCoroutineImplBase,
    externalCreatorAccountId: Long = 0L,
    externalOwnedMeasurementConsumerId: Long = 0L,
  ): Account {
    return accountsService.createAccount(
      account {
        this.externalCreatorAccountId = externalCreatorAccountId
        this.externalOwnedMeasurementConsumerId = externalOwnedMeasurementConsumerId
      }
    )
  }

  /**
   * Generates a self-issued ID token and uses it to activate the [Account].
   *
   * @return generated self-issued ID token used for activation
   */
  suspend fun activateAccount(
    accountsService: AccountsCoroutineImplBase,
    account: Account,
  ): String {
    val openIdRequestParams =
      accountsService.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    val idToken =
      generateIdToken(
        createRequestUri(
          state = openIdRequestParams.state,
          nonce = openIdRequestParams.nonce,
          redirectUri = "",
          isSelfIssued = true,
        ),
        clock,
      )
    val openIdConnectIdentity = parseIdToken(idToken)

    accountsService.activateAccount(
      activateAccountRequest {
        externalAccountId = account.externalAccountId
        activationToken = account.activationToken
        identity = openIdConnectIdentity
      }
    )

    return idToken
  }

  fun parseIdToken(idToken: String, redirectUri: String = ""): Account.OpenIdConnectIdentity {
    val tokenParts = idToken.split(".")
    val claims =
      JsonParser.parseString(tokenParts[1].base64UrlDecode().toString(Charsets.UTF_8)).asJsonObject
    val subJwk = claims.get("sub_jwk")
    val jwk = subJwk.asJsonObject
    val publicJwkHandle = PublicJwkHandle.fromJwk(jwk)

    val verifiedJwt =
      SelfIssuedIdTokens.validateJwt(
        redirectUri = redirectUri,
        idToken = idToken,
        publicJwkHandle = publicJwkHandle,
      )

    return openIdConnectIdentity {
      issuer = verifiedJwt.issuer!!
      subject = verifiedJwt.subject!!
    }
  }

  suspend fun createMeasurementConsumerCreationToken(
    accountsService: AccountsCoroutineImplBase
  ): Long {
    val createMeasurementConsumerCreationTokenResponse =
      accountsService.createMeasurementConsumerCreationToken(
        createMeasurementConsumerCreationTokenRequest {}
      )

    return createMeasurementConsumerCreationTokenResponse.measurementConsumerCreationToken
  }
}

fun DataProvider.toDataProviderValue(nonce: Long = Random.nextLong()) = dataProviderValue {
  externalDataProviderCertificateId = certificate.externalCertificateId
  dataProviderPublicKey = details.publicKey
  encryptedRequisitionSpec = "Encrypted RequisitionSpec $nonce".toByteStringUtf8()
  nonceHash = Hashing.hashSha256(nonce)

  // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting these fields.
  dataProviderPublicKeySignature = details.publicKeySignature
  dataProviderPublicKeySignatureAlgorithmOid = details.publicKeySignatureAlgorithmOid
}
