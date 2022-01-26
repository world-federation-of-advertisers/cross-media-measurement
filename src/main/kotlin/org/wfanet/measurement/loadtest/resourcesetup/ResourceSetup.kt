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
import com.google.protobuf.kotlin.toByteString
import java.time.Clock
import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createApiKeyRequest
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens.generateIdToken
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub as InternalAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProviderKt as InternalDataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.account as internalAccount
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.dataProvider as internalDataProvider
import org.wfanet.measurement.kingdom.service.api.v2alpha.parseCertificateDer
import org.wfanet.measurement.kingdom.service.api.v2alpha.withIdToken

private val API_VERSION = Version.V2_ALPHA

/** A Job preparing resources required for the correctness test. */
class ResourceSetup(
  private val internalAccountsClient: InternalAccountsCoroutineStub,
  private val internalDataProvidersClient: InternalDataProvidersCoroutineStub,
  private val accountsClient: AccountsCoroutineStub,
  private val apiKeysClient: ApiKeysCoroutineStub,
  private val certificatesClient: CertificatesCoroutineStub,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val runId: String
) {
  data class MeasurementConsumerAndKey(
    val measurementConsumer: MeasurementConsumer,
    val apiAuthenticationKey: String
  )

  /** Process to create resources. */
  suspend fun process(
    dataProviderContents: List<EntityContent>,
    measurementConsumerContent: EntityContent,
    duchyCerts: List<DuchyCert>,
  ) {
    logger.info("Starting with RunID: $runId ...")

    // Step 1: Create the EDPs.
    dataProviderContents.forEach {
      val dataProviderName = createInternalDataProvider(it)
      logger.info("Successfully created data provider: $dataProviderName")
    }

    // Step 2: Create the MC.
    val (measurementConsumer, apiAuthenticationKey) =
      createMeasurementConsumer(measurementConsumerContent)
    logger.info("Successfully created measurement consumer: ${measurementConsumer.name}")
    logger.info(
      "API key for measurement consumer ${measurementConsumer.name}: $apiAuthenticationKey"
    )

    // Step 3: Create certificate for each duchy.
    duchyCerts.forEach {
      val certificate = createDuchyCertificate(it)
      logger.info("Successfully created certificate ${certificate.name}")
    }
  }

  /** Create an internal dataProvider, and return its corresponding public API resource name. */
  suspend fun createInternalDataProvider(dataProviderContent: EntityContent): String {
    val encryptionPublicKey = dataProviderContent.encryptionPublicKey
    val signedPublicKey =
      signEncryptionPublicKey(encryptionPublicKey, dataProviderContent.signingKey)
    val internalDataProvider =
      internalDataProvidersClient.createDataProvider(
        internalDataProvider {
          certificate =
            parseCertificateDer(dataProviderContent.signingKey.certificate.encoded.toByteString())
          details =
            InternalDataProviderKt.details {
              apiVersion = API_VERSION.string
              publicKey = signedPublicKey.data
              publicKeySignature = signedPublicKey.signature
            }
        }
      )
    return DataProviderKey(externalIdToApiId(internalDataProvider.externalDataProviderId)).toName()
  }

  suspend fun createMeasurementConsumer(
    measurementConsumerContent: EntityContent,
  ): MeasurementConsumerAndKey {
    // The initial account is created via the Kingdom Internal API by the Kingdom operator.
    val internalAccount = internalAccountsClient.createAccount(internalAccount {})
    val accountName = AccountKey(externalIdToApiId(internalAccount.externalAccountId)).toName()
    val accountActivationToken = externalIdToApiId(internalAccount.activationToken)
    val mcCreationToken =
      externalIdToApiId(
        internalAccountsClient.createMeasurementConsumerCreationToken(
            createMeasurementConsumerCreationTokenRequest {}
          )
          .measurementConsumerCreationToken
      )

    // Account activation and MC creation are done via the public API.
    val authenticationResponse =
      accountsClient.authenticate(authenticateRequest { issuer = "https://self-issued.me" })
    val idToken =
      generateIdToken(authenticationResponse.authenticationRequestUri, Clock.systemUTC())
    accountsClient
      .withIdToken(idToken)
      .activateAccount(
        activateAccountRequest {
          name = accountName
          activationToken = accountActivationToken
        }
      )

    val request = createMeasurementConsumerRequest {
      measurementConsumer =
        measurementConsumer {
          certificateDer = measurementConsumerContent.signingKey.certificate.encoded.toByteString()
          publicKey =
            signEncryptionPublicKey(
              measurementConsumerContent.encryptionPublicKey,
              measurementConsumerContent.signingKey
            )
          displayName = measurementConsumerContent.displayName
        }
      measurementConsumerCreationToken = mcCreationToken
    }
    val measurementConsumer =
      measurementConsumersClient.withIdToken(idToken).createMeasurementConsumer(request)

    // API key for MC is created to act as MC caller
    val apiAuthenticationKey =
      apiKeysClient
        .withIdToken(idToken)
        .createApiKey(
          createApiKeyRequest {
            parent = measurementConsumer.name
            apiKey = apiKey { nickname = "test_key" }
          }
        )
        .authenticationKey

    return MeasurementConsumerAndKey(measurementConsumer, apiAuthenticationKey)
  }

  // TODO(@wangyaopw): Create duchy certificate using the internal API instead of public API.
  suspend fun createDuchyCertificate(duchyCert: DuchyCert): Certificate {
    val name = DuchyKey(duchyCert.duchyId).toName()
    val request = createCertificateRequest {
      parent = name
      certificate = certificate { x509Der = duchyCert.consentSignalCertificateDer }
    }
    return certificatesClient.withPrincipalName(name).createCertificate(request)
  }

  companion object {
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
  val signingKey: SigningKeyHandle
)

data class DuchyCert(
  /** The external duchy Id. */
  val duchyId: String,
  /** The consent signaling certificate in DER format. */
  val consentSignalCertificateDer: ByteString
)
