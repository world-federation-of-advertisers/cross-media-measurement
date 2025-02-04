/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.securecomputation.requisitions

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteString
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient

@RunWith(JUnit4::class)
class RequisitionFetcherTest {
  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `fetch new requisitions and store in GCS bucket`() {
    val storage = LocalStorageHelper.getOptions().service
    val storageClient = GcsStorageClient(storage, BUCKET)
    val fetcher = RequisitionFetcher(requisitionsStub, storageClient, BUCKET, DATA_PROVIDER_NAME)
    val persistedRequisition = runBlocking {
      fetcher.executeRequisitionFetchingWorkflow()
      storageClient
        .getBlob("gs://${BUCKET}/${REQUISITION.name}")
        ?.read()
        ?.toByteArray()
        ?.toByteString()
    }

    assertThat(REQUISITION.toByteString()).isEqualTo(persistedRequisition)
  }

  companion object {
    private const val BUCKET = "requisition-storage-test-bucket"
    private const val MC_ID = "mc"
    private const val MC_NAME = "measurementConsumers/$MC_ID"
    private const val PDP_DISPLAY_NAME = "pdp1"
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private const val PDP_ID = "somePopulationDataProvider"
    private const val PDP_NAME = "dataProviders/$PDP_ID"

    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val MEASUREMENT_NAME = "$MC_NAME/measurements/BBBBBBBBBHs"
    private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
      "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"

    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"

    private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
    private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"

    private const val MODEL_LINE_NAME = "${MODEL_SUITE_NAME}/modelLines/AAAAAAAAAHs"

    private val MC_SIGNING_KEY: SigningKeyHandle =
      loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")
    private val PDP_SIGNING_KEY: SigningKeyHandle =
      loadSigningKey("${PDP_DISPLAY_NAME}_cs_cert.der", "${PDP_DISPLAY_NAME}_cs_private.der")
    private val DATA_PROVIDER_CERTIFICATE_KEY: DataProviderCertificateKey =
      DataProviderCertificateKey(PDP_ID, externalIdToApiId(8L))

    private val DATA_PROVIDER_CERTIFICATE: Certificate = certificate {
      name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
      x509Der = PDP_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = PDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }

    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${PDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val REQUISITION_SPEC = requisitionSpec {
      population =
        RequisitionSpecKt.population {
          filter =
            RequisitionSpecKt.eventFilter {
              expression = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
            }
        }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = Random.nextLong()
    }
    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
          reachPrivacyParams = OUTPUT_DP_PARAMS
          frequencyPrivacyParams = OUTPUT_DP_PARAMS
          maximumFrequency = 10
        }
      vidSamplingInterval =
        MeasurementSpecKt.vidSamplingInterval {
          start = 0.0f
          width = 1.0f
        }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
      modelLine = MODEL_LINE_NAME
    }

    private val REQUISITION = requisition {
      name = "${PDP_NAME}/requisitions/foo"
      measurement = MEASUREMENT_NAME
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            direct =
              ProtocolConfigKt.direct {
                deterministicCountDistinct =
                  ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                deterministicDistribution =
                  ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    }

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }
  }
}
