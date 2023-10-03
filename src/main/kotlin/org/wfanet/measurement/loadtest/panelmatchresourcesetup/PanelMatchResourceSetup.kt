// Copyright 2023 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.loadtest.panelmatchresourcesetup

import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.toByteString
import com.google.type.Date
import io.grpc.StatusException
import java.io.File
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeKey
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMapKt
import org.wfanet.measurement.config.authorityKeyToPrincipalMap
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.kingdom.service.api.v2alpha.parseCertificateDer
import org.wfanet.measurement.kingdom.service.api.v2alpha.toInternal
import org.wfanet.measurement.loadtest.panelmatch.resourcesetup.Resources
import org.wfanet.measurement.loadtest.panelmatch.resourcesetup.ResourcesKt
import org.wfanet.measurement.loadtest.panelmatch.resourcesetup.ResourcesKt.resource
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent

private val API_VERSION = Version.V2_ALPHA

class PanelMatchResourceSetup(
  private val internalDataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val internalModelProvidersClient: ModelProvidersGrpcKt.ModelProvidersCoroutineStub,
  private val recurringExchangesStub: RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub,
  private val outputDir: File? = null,
  private val bazelConfigName: String = DEFAULT_BAZEL_CONFIG_NAME,
) {

  /*data class DataProviderAndKey(
    val dataProvider: DataProvider,
    val apiAuthenticationKey: String
  )*/
  suspend fun process(
    dataProviderContent: EntityContent,
    modelProviderContent: EntityContent,
    exchangeDate: Date,
    exchangeWorkflow: ExchangeWorkflow,
    exchangeSchedule: String,
    publicApiVersion: String,
  ): PanelMatchResourceKeys {
    logger.info("Starting resource setup ...")
    val resources = mutableListOf<Resources.Resource>()

    val externalDataProviderId = createDataProvider(dataProviderContent /*, internalAccount*/)

    logger.info("Successfully created data provider: ${externalDataProviderId}")

    val dataProviderKey = DataProviderKey(externalIdToApiId(externalDataProviderId))
    resources.add(
      resource {
        name = dataProviderKey.toName()
        dataProvider =
          ResourcesKt.ResourceKt.dataProvider {
            displayName = dataProviderContent.displayName
            // Assume signing cert uses same issuer as TLS client cert.
            authorityKeyIdentifier =
              checkNotNull(dataProviderContent.signingKey.certificate.authorityKeyIdentifier)
          }
      }
    )

    val externalModelProviderId = createModelProvider()
    val modelProviderKey = ModelProviderKey(externalIdToApiId(externalModelProviderId))

    resources.add(
      resource {
        name = modelProviderKey.toName()
        modelProvider =
          ResourcesKt.ResourceKt.modelProvider {
            displayName = modelProviderContent.displayName

            // Assume signing cert uses same issuer as TLS client cert.
            authorityKeyIdentifier =
              checkNotNull(modelProviderContent.signingKey.certificate.authorityKeyIdentifier)
          }
      }
    )

    val externalRecurringExchangeId =
      createRecurringExchange(
        externalDataProviderId,
        externalModelProviderId,
        exchangeDate,
        exchangeSchedule,
        publicApiVersion,
        exchangeWorkflow
      )

    val recurringExchangeKey = RecurringExchangeKey(externalIdToApiId(externalRecurringExchangeId))
    withContext(Dispatchers.IO) { writeOutput(resources) }
    logger.info("Resource setup was successful.")
    return PanelMatchResourceKeys(
      dataProviderKey,
      modelProviderKey,
      recurringExchangeKey,
      resources
    )
  }

  suspend fun createDataProvider(
    dataProviderContent: EntityContent /*, internalAccount: Account*/
  ): Long {
    val encryptionPublicKey = dataProviderContent.encryptionPublicKey
    val signedPublicKey =
      signEncryptionPublicKey(encryptionPublicKey, dataProviderContent.signingKey)
    val internalDataProvider =
      try {
        internalDataProvidersClient.createDataProvider(
          dataProvider {
            certificate =
              parseCertificateDer(dataProviderContent.signingKey.certificate.encoded.toByteString())
            details =
              DataProviderKt.details {
                apiVersion = API_VERSION.string
                publicKey = signedPublicKey.data
                publicKeySignature = signedPublicKey.signature
              }
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating DataProvider", e)
      }
    logger.info("InternalDataProvider: ${internalDataProvider.externalDataProviderId}")

    return internalDataProvider.externalDataProviderId
  }

  suspend fun createModelProvider(): Long {
    val internalModelProvider =
      try {
        internalModelProvidersClient.createModelProvider(modelProvider {})
      } catch (e: StatusException) {
        throw Exception("Error creating ModelProvider", e)
      }
    logger.info("InternalModelProvider: ${internalModelProvider.externalModelProviderId}")
    return internalModelProvider.externalModelProviderId
  }

  private suspend fun createRecurringExchange(
    externalDataProvider: Long,
    externalModelProvider: Long,
    exchangeDate: Date,
    exchangeSchedule: String,
    publicApiVersion: String,
    exchangeWorkflow: ExchangeWorkflow
  ): Long {
    val recurringExchangeId =
      recurringExchangesStub
        .createRecurringExchange(
          createRecurringExchangeRequest {
            recurringExchange = recurringExchange {
              externalDataProviderId = externalDataProvider
              externalModelProviderId = externalModelProvider
              state = RecurringExchange.State.ACTIVE
              details = recurringExchangeDetails {
                this.exchangeWorkflow = exchangeWorkflow.toInternal()
                cronSchedule = exchangeSchedule
                externalExchangeWorkflow = exchangeWorkflow.toByteString()
                apiVersion = publicApiVersion
              }
              nextExchangeDate = exchangeDate
            }
          }
        )
        .externalRecurringExchangeId
    logger.info("recurringExchangeId: ${recurringExchangeId}")
    return recurringExchangeId
  }

  @Blocking
  private fun writeOutput(resources: Iterable<Resources.Resource>) {
    val output = outputDir?.let { FileOutput(it) } ?: ConsoleOutput

    output.resolve(RESOURCES_OUTPUT_FILE).writer().use { writer ->
      TextFormat.printer()
        .print(
          org.wfanet.measurement.loadtest.panelmatch.resourcesetup.resources {
            this.resources += resources
          },
          writer
        )
    }

    val akidMap = authorityKeyToPrincipalMap {
      for (resource in resources) {
        val akid =
          when (resource.resourceCase) {
            Resources.Resource.ResourceCase.DATA_PROVIDER ->
              resource.dataProvider.authorityKeyIdentifier
            Resources.Resource.ResourceCase.MODEL_PROVIDER ->
              resource.modelProvider.authorityKeyIdentifier
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
    val configName = bazelConfigName
    output.resolve(BAZEL_RC_FILE).writer().use { writer ->
      for (resource in resources) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (resource.resourceCase) {
          Resources.Resource.ResourceCase.DATA_PROVIDER -> {
            val displayName = resource.dataProvider.displayName
            writer.appendLine("build:$configName --define=edp_name=${resource.name}")
          }
          Resources.Resource.ResourceCase.MODEL_PROVIDER -> {
            with(resource) { writer.appendLine("build:$configName --define=mp_name=$name") }
          }
          Resources.Resource.ResourceCase.RESOURCE_NOT_SET -> error("Bad resource case")
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    const val DEFAULT_BAZEL_CONFIG_NAME = "halo"
    const val RESOURCES_OUTPUT_FILE = "resources.textproto"
    const val AKID_PRINCIPAL_MAP_FILE = "authority_key_identifier_to_principal_map.textproto"
    const val BAZEL_RC_FILE = "resource-setup.bazelrc"
  }
}

data class PanelMatchResourceKeys(
  val dataProviderKey: DataProviderKey,
  val modelProviderKey: ModelProviderKey,
  val recurringExchangeKey: RecurringExchangeKey,
  val resources: List<Resources.Resource>,
)
