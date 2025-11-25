/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha.tools

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Descriptors
import com.google.protobuf.any
import com.google.protobuf.duration
import com.google.protobuf.empty
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.protobuf.value
import com.google.type.date
import com.google.type.interval
import io.grpc.Server
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.AccountConstants
import org.wfanet.measurement.api.ApiKeyConstants
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.AccountKt
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ApiKey
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.CreateModelLineRequest
import org.wfanet.measurement.api.v2alpha.CreateModelOutageRequest
import org.wfanet.measurement.api.v2alpha.CreateModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.CreateModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.CreateModelShardRequest
import org.wfanet.measurement.api.v2alpha.CreateModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.DeleteModelOutageRequest
import org.wfanet.measurement.api.v2alpha.DeleteModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.DeleteModelShardRequest
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.GetModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.GetModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListModelOutagesRequest
import org.wfanet.measurement.api.v2alpha.ListModelOutagesRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelShardsRequest
import org.wfanet.measurement.api.v2alpha.ListModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.population
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.watchDuration
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultOutput
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelOutage
import org.wfanet.measurement.api.v2alpha.ModelOutagesGrpcKt.ModelOutagesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.PublicKeysGrpcKt
import org.wfanet.measurement.api.v2alpha.ReplaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.ScheduleModelRolloutFreezeRequest
import org.wfanet.measurement.api.v2alpha.SetModelLineActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.account
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.authenticateResponse
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createApiKeyRequest
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.createModelOutageRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createModelShardRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.dateInterval
import org.wfanet.measurement.api.v2alpha.deleteModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.getModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listModelOutagesRequest
import org.wfanet.measurement.api.v2alpha.listModelOutagesResponse
import org.wfanet.measurement.api.v2alpha.listModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.listModelReleasesResponse
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.listModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelOutage
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.publicKey
import org.wfanet.measurement.api.v2alpha.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.api.v2alpha.replaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.revokeCertificateRequest
import org.wfanet.measurement.api.v2alpha.scheduleModelRolloutFreezeRequest
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.setModelLineActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.v2alpha.updatePublicKeyRequest
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.openid.createRequestUri
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult

private val SECRETS_DIR: File =
  getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )!!
    .toFile()

private val KINGDOM_TLS_CERT: File = SECRETS_DIR.resolve("kingdom_tls.pem")
private val KINGDOM_TLS_KEY: File = SECRETS_DIR.resolve("kingdom_tls.key")
private val KINGDOM_TRUSTED_CERTS: File = SECRETS_DIR.resolve("all_root_certs.pem")

private val CLIENT_TLS_CERT: File = SECRETS_DIR.resolve("mc_tls.pem")
private val CLIENT_TLS_KEY: File = SECRETS_DIR.resolve("mc_tls.key")
private val CLIENT_TRUSTED_CERTS: File = SECRETS_DIR.resolve("mc_trusted_certs.pem")

private val SIOP_KEY: File = SECRETS_DIR.resolve("account1_siop_private.tink")

private val kingdomSigningCerts =
  SigningCerts.fromPemFiles(KINGDOM_TLS_CERT, KINGDOM_TLS_KEY, KINGDOM_TRUSTED_CERTS)

private const val AUTHENTICATION_KEY = "nR5QPN7ptx"

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
private val MEASUREMENT_CONSUMER_CERTIFICATE_FILE: File = SECRETS_DIR.resolve("mc_cs_cert.der")
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME = "measurementConsumers/1/certificates/1"
private val MEASUREMENT_CONSUMER_PUBLIC_KEY_FILE: File = SECRETS_DIR.resolve("mc_enc_public.pb")
private val MEASUREMENT_CONSUMER_PUBLIC_KEY_SIG_FILE: File =
  SECRETS_DIR.resolve("mc_enc_public.pb.sig")
private val MEASUREMENT_CONSUMER_ROOT_CERTIFICATE_FILE: File = SECRETS_DIR.resolve("mc_root.pem")

private const val DATA_PROVIDER_NAME = "dataProviders/1"
private const val DATA_PROVIDER_CERTIFICATE_NAME = "dataProviders/1/certificates/1"
private val DATA_PROVIDER_PUBLIC_KEY =
  loadPublicKey(SECRETS_DIR.resolve("edp1_enc_public.tink")).toEncryptionPublicKey()
private val DATA_PROVIDER_PRIVATE_KEY_HANDLE =
  loadPrivateKey(SECRETS_DIR.resolve("edp1_enc_private.tink"))
private val DUCHY_KEY = DuchyKey("worker1")
private val DUCHY_NAMES = listOf(DUCHY_KEY).map { it.toName() }

private const val MODEL_LINE_ACTIVE_START_TIME = "2025-05-24T05:00:00.000Z"
private const val MODEL_LINE_ACTIVE_END_TIME = "2030-05-24T05:00:00.000Z"

private const val MODEL_OUTAGE_ACTIVE_START_TIME = "2026-05-24T05:00:00.000Z"
private const val MODEL_OUTAGE_ACTIVE_END_TIME = "2026-05-29T05:00:00.000Z"

private val MODEL_ROLLOUT_ACTIVE_START_DATE = date {
  year = 2026
  month = 5
  day = 24
}
private val MODEL_ROLLOUT_ACTIVE_END_DATE = date {
  year = 2026
  month = 9
  day = 24
}
private val MODEL_ROLLOUT_FREEZE_DATE = date {
  year = 2026
  month = 7
  day = 24
}

private val DATA_PROVIDER = dataProvider {
  name = DATA_PROVIDER_NAME
  certificate = DATA_PROVIDER_CERTIFICATE_NAME
  publicKey = signedMessage { setMessage(DATA_PROVIDER_PUBLIC_KEY.pack()) }
}
private const val MODEL_PROVIDER_NAME = "modelProvider/1"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/1"
private const val MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/1"
private const val HOLDBACK_MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/2"
private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/1"

private const val MODEL_OUTAGE_NAME = "$MODEL_LINE_NAME/modelOutages/1"
private const val MODEL_SHARD_NAME = "$DATA_PROVIDER_NAME/modelShards/1"
private const val MODEL_BLOB_PATH = "model_blob_path"

private const val LIST_PAGE_TOKEN = "token"
private const val LIST_PAGE_SIZE = 10

private const val MODEL_ROLLOUT_NAME = "$MODEL_LINE_NAME/modelRollouts/1"

private val MODEL_SUITE = modelSuite {
  name = MODEL_SUITE_NAME
  displayName = "Display name"
  description = "Description"
  createTime = timestamp { seconds = 3000 }
}

private val MODEL_LINE = modelLine {
  name = MODEL_LINE_NAME
  displayName = "Display name"
  description = "Description"
  activeStartTime = timestamp {
    seconds = Instant.parse(MODEL_LINE_ACTIVE_START_TIME).toProtoTime().seconds
  }
  activeEndTime = timestamp {
    seconds = Instant.parse(MODEL_LINE_ACTIVE_END_TIME).toProtoTime().seconds
  }
  type = ModelLine.Type.PROD
  holdbackModelLine = HOLDBACK_MODEL_LINE_NAME
  createTime = timestamp { seconds = 3000 }
  updateTime = timestamp { seconds = 3000 }
}

private val MODEL_RELEASE = modelRelease {
  name = MODEL_RELEASE_NAME
  createTime = timestamp { seconds = 3000 }
}

private val MODEL_OUTAGE = modelOutage {
  name = MODEL_OUTAGE_NAME
  outageInterval = interval {
    startTime = Instant.parse(MODEL_OUTAGE_ACTIVE_START_TIME).toProtoTime()
    endTime = Instant.parse(MODEL_OUTAGE_ACTIVE_END_TIME).toProtoTime()
  }
  state = ModelOutage.State.ACTIVE
  createTime = timestamp { seconds = 3000 }
}

private val MODEL_SHARD = modelShard {
  name = MODEL_SHARD_NAME
  modelRelease = MODEL_RELEASE_NAME
  modelBlob = modelBlob { modelBlobPath = MODEL_BLOB_PATH }
  createTime = timestamp { seconds = 3000 }
}

private val MODEL_ROLLOUT = modelRollout {
  name = MODEL_ROLLOUT_NAME
  gradualRolloutPeriod = dateInterval {
    this.startDate = MODEL_ROLLOUT_ACTIVE_START_DATE
    this.endDate = MODEL_ROLLOUT_ACTIVE_END_DATE
  }
  previousModelRollout = "previous model"
  modelRelease = MODEL_RELEASE_NAME
}

private val MODEL_ROLLOUT_WITH_FREEZE_TIME = modelRollout {
  name = MODEL_ROLLOUT_NAME
  gradualRolloutPeriod = dateInterval {
    this.startDate = MODEL_ROLLOUT_ACTIVE_START_DATE
    this.endDate = MODEL_ROLLOUT_ACTIVE_END_DATE
  }
  rolloutFreezeDate = MODEL_ROLLOUT_FREEZE_DATE
  previousModelRollout = "previous model"
  modelRelease = MODEL_RELEASE_NAME
}

private const val TIME_STRING_1 = "2022-05-22T01:00:00.000Z"
private const val TIME_STRING_2 = "2022-05-24T05:00:00.000Z"
private const val TIME_STRING_3 = "2022-05-22T01:22:32.122Z"
private const val TIME_STRING_4 = "2022-05-23T03:14:55.876Z"
private const val TIME_STRING_5 = "2022-04-22T01:19:42.567Z"
private const val TIME_STRING_6 = "2022-05-22T01:56:12.483Z"

private val AGGREGATOR_CERTIFICATE_DER =
  SECRETS_DIR.resolve("aggregator_cs_cert.der").readByteString()
private val AGGREGATOR_PRIVATE_KEY_DER =
  SECRETS_DIR.resolve("aggregator_cs_private.der").readByteString()
private val AGGREGATOR_SIGNING_KEY: SigningKeyHandle by lazy {
  val consentSignal509Cert = readCertificate(AGGREGATOR_CERTIFICATE_DER)
  SigningKeyHandle(
    consentSignal509Cert,
    readPrivateKey(AGGREGATOR_PRIVATE_KEY_DER, consentSignal509Cert.publicKey.algorithm),
  )
}
private val AGGREGATOR_CERTIFICATE = certificate { x509Der = AGGREGATOR_CERTIFICATE_DER }

private const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/100"
private val MEASUREMENT = measurement { name = MEASUREMENT_NAME }

private val LIST_MEASUREMENT_RESPONSE = listMeasurementsResponse {
  measurements += measurement {
    name = "$MEASUREMENT_CONSUMER_NAME/measurements/101"
    state = Measurement.State.AWAITING_REQUISITION_FULFILLMENT
  }
  measurements += measurement {
    name = "$MEASUREMENT_CONSUMER_NAME/measurements/102"
    state = Measurement.State.SUCCEEDED
  }
  measurements += measurement {
    name = "$MEASUREMENT_CONSUMER_NAME/measurements/102"
    state = Measurement.State.FAILED
    failure = failure {
      reason = Measurement.Failure.Reason.REQUISITION_REFUSED
      message = "Privacy budget exceeded."
    }
  }
}

private val API_KEY = apiKey {
  name = "$MEASUREMENT_CONSUMER_NAME/apiKeys/103"
  nickname = "Reporting"
  description = "Reporting server"
  authenticationKey = AUTHENTICATION_KEY
}

@RunWith(JUnit4::class)
class MeasurementSystemTest {
  private val accountsServiceMock: AccountsCoroutineImplBase = mockService()
  private val headerInterceptor = HeaderCapturingInterceptor()

  private val measurementConsumersServiceMock: MeasurementConsumersCoroutineImplBase = mockService {
    onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
  }
  private val apiKeysMock: ApiKeysGrpcKt.ApiKeysCoroutineImplBase = mockService {
    onBlocking { createApiKey(any()) }.thenReturn(API_KEY)
  }
  private val dataProvidersServiceMock: DataProvidersGrpcKt.DataProvidersCoroutineImplBase =
    mockService {
      onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
    }
  private val measurementsServiceMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase =
    mockService {
      onBlocking { createMeasurement(any()) }.thenReturn(MEASUREMENT)
      onBlocking { listMeasurements(any()) }.thenReturn(LIST_MEASUREMENT_RESPONSE)
      onBlocking { getMeasurement(any()) }.thenReturn(SUCCEEDED_MEASUREMENT)
    }
  private val certificatesServiceMock: CertificatesGrpcKt.CertificatesCoroutineImplBase =
    mockService {
      onBlocking { getCertificate(any()) }.thenReturn(AGGREGATOR_CERTIFICATE)
    }
  private val publicKeysServiceMock: PublicKeysGrpcKt.PublicKeysCoroutineImplBase = mockService()
  private val modelLinesServiceMock: ModelLinesCoroutineImplBase = mockService {
    onBlocking { createModelLine(any()) }.thenReturn(MODEL_LINE)
    onBlocking { setModelLineHoldbackModelLine(any()) }.thenReturn(MODEL_LINE)
    onBlocking { setModelLineActiveEndTime(any()) }.thenReturn(MODEL_LINE)
    onBlocking { listModelLines(any()) }
      .thenReturn(listModelLinesResponse { modelLines += listOf(MODEL_LINE) })
  }
  private val modelReleasesServiceMock: ModelReleasesCoroutineImplBase = mockService {
    onBlocking { createModelRelease(any()) }.thenReturn(MODEL_RELEASE)
    onBlocking { getModelRelease(any()) }.thenReturn(MODEL_RELEASE)
    onBlocking { listModelReleases(any()) }
      .thenReturn(listModelReleasesResponse { modelReleases += listOf(MODEL_RELEASE) })
  }
  private val modelOutagesServiceMock: ModelOutagesCoroutineImplBase = mockService {
    onBlocking { createModelOutage(any()) }.thenReturn(MODEL_OUTAGE)
    onBlocking { listModelOutages(any()) }
      .thenReturn(listModelOutagesResponse { modelOutages += listOf(MODEL_OUTAGE) })
    onBlocking { deleteModelOutage(any()) }.thenReturn(MODEL_OUTAGE)
  }
  private val modelShardsServiceMock: ModelShardsCoroutineImplBase = mockService {
    onBlocking { createModelShard(any()) }.thenReturn(MODEL_SHARD)
    onBlocking { listModelShards(any()) }
      .thenReturn(listModelShardsResponse { modelShards += listOf(MODEL_SHARD) })
    onBlocking { deleteModelShard(any()) }.thenReturn(empty {})
  }
  private val modelRolloutsServiceMock: ModelRolloutsCoroutineImplBase = mockService {
    onBlocking { createModelRollout(any()) }.thenReturn(MODEL_ROLLOUT)
    onBlocking { listModelRollouts(any()) }
      .thenReturn(listModelRolloutsResponse { modelRollouts += listOf(MODEL_ROLLOUT) })
    onBlocking { scheduleModelRolloutFreeze(any()) }.thenReturn(MODEL_ROLLOUT_WITH_FREEZE_TIME)
    onBlocking { deleteModelRollout(any()) }.thenReturn(empty {})
  }
  private val modelSuitesServiceMock: ModelSuitesCoroutineImplBase = mockService {
    onBlocking { createModelSuite(any()) }.thenReturn(MODEL_SUITE)
    onBlocking { getModelSuite(any()) }.thenReturn(MODEL_SUITE)
    onBlocking { listModelSuites(any()) }
      .thenReturn(listModelSuitesResponse { modelSuites += listOf(MODEL_SUITE) })
  }

  val services: List<ServerServiceDefinition> =
    listOf(
      ServerInterceptors.intercept(accountsServiceMock, headerInterceptor),
      ServerInterceptors.intercept(measurementsServiceMock, headerInterceptor),
      ServerInterceptors.intercept(measurementConsumersServiceMock, headerInterceptor),
      ServerInterceptors.intercept(apiKeysMock, headerInterceptor),
      dataProvidersServiceMock.bindService(),
      ServerInterceptors.intercept(certificatesServiceMock, headerInterceptor),
      ServerInterceptors.intercept(publicKeysServiceMock, headerInterceptor),
      ServerInterceptors.intercept(modelOutagesServiceMock, headerInterceptor),
      ServerInterceptors.intercept(modelShardsServiceMock, headerInterceptor),
      ServerInterceptors.intercept(modelLinesServiceMock, headerInterceptor),
      ServerInterceptors.intercept(modelReleasesServiceMock, headerInterceptor),
      ServerInterceptors.intercept(modelRolloutsServiceMock, headerInterceptor),
      ServerInterceptors.intercept(modelSuitesServiceMock, headerInterceptor),
    )

  private val publicApiServer: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(kingdomSigningCerts.toServerTlsContext())
      .addServices(services)
      .build()

  @Before
  fun startServer() {
    publicApiServer.start()
  }

  @After
  fun shutdownServer() {
    publicApiServer.shutdown()
    publicApiServer.awaitTermination()
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$CLIENT_TLS_CERT",
        "--tls-key-file=$CLIENT_TLS_KEY",
        "--cert-collection-file=$CLIENT_TRUSTED_CERTS",
        "--kingdom-public-api-target=localhost:${publicApiServer.port}",
      )

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, MeasurementSystem::main)
    assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  @Test
  fun `accounts authenticate prints ID token`() {
    val args =
      commonArgs +
        arrayOf("accounts", "authenticate", "--self-issued-openid-provider-key=$SIOP_KEY")
    accountsServiceMock.stub {
      onBlocking { authenticate(any()) }
        .thenReturn(
          authenticateResponse {
            authenticationRequestUri = createRequestUri(1234L, 5678L, "https://example.com", true)
          }
        )
    }

    val output: String = callCli(args)

    verifyProtoArgument(accountsServiceMock, AccountsCoroutineImplBase::authenticate)
      .isEqualTo(authenticateRequest { issuer = MeasurementSystem.SELF_ISSUED_ISSUER })
    assertThat(output).matches("ID Token: [-_a-zA-Z0-9.]+\\s*")
  }

  @Test
  fun `accounts activate prints response`() {
    val accountName = "accounts/KcuXSjfBx9E"
    val idToken = "fake-id-token"
    val activationToken = "vzmtXavLdk4"
    val args =
      commonArgs +
        arrayOf(
          "accounts",
          "activate",
          accountName,
          "--id-token=$idToken",
          "--activation-token=$activationToken",
        )
    val account = account {
      name = accountName
      activationState = Account.ActivationState.ACTIVATED
      openId =
        AccountKt.openIdConnectIdentity {
          issuer = MeasurementSystem.SELF_ISSUED_ISSUER
          subject = "fake-oid-subject"
        }
    }
    accountsServiceMock.stub { onBlocking { activateAccount(any()) }.thenReturn(account) }

    val output: String = callCli(args)

    assertThat(
        headerInterceptor
          .captured(AccountsGrpcKt.activateAccountMethod)
          .single()
          .get(AccountConstants.ID_TOKEN_METADATA_KEY)
      )
      .isEqualTo(idToken)
    verifyProtoArgument(accountsServiceMock, AccountsCoroutineImplBase::activateAccount)
      .isEqualTo(
        activateAccountRequest {
          name = accountName
          this.activationToken = activationToken
        }
      )
    assertThat(parseTextProto(output.reader(), Account.getDefaultInstance())).isEqualTo(account)
  }

  @Test
  fun `measurement-consumers create prints response`() {
    measurementConsumersServiceMock.stub {
      onBlocking { createMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
    }
    val idToken = "fake-id-token"
    val creationToken = "vzmtXavLdk4"
    val args =
      commonArgs +
        arrayOf(
          "measurement-consumers",
          "create",
          "--id-token=$idToken",
          "--creation-token=$creationToken",
          "--certificate=${MEASUREMENT_CONSUMER_CERTIFICATE_FILE.path}",
          "--public-key=${MEASUREMENT_CONSUMER_PUBLIC_KEY_FILE.path}",
          "--public-key-signature=${MEASUREMENT_CONSUMER_PUBLIC_KEY_SIG_FILE.path}",
          "--display-name=${MEASUREMENT_CONSUMER.displayName}",
        )

    val output: String = callCli(args)

    assertThat(
        headerInterceptor
          .captured(MeasurementConsumersGrpcKt.createMeasurementConsumerMethod)
          .single()
          .get(AccountConstants.ID_TOKEN_METADATA_KEY)
      )
      .isEqualTo(idToken)
    verifyProtoArgument(
        measurementConsumersServiceMock,
        MeasurementConsumersCoroutineImplBase::createMeasurementConsumer,
      )
      .isEqualTo(
        createMeasurementConsumerRequest {
          measurementConsumer =
            MEASUREMENT_CONSUMER.copy {
              clearName()
              clearCertificate()
              measurementConsumerCreationToken = creationToken
            }
        }
      )
    assertThat(parseTextProto(output.reader(), MeasurementConsumer.getDefaultInstance()))
      .isEqualTo(MEASUREMENT_CONSUMER)
  }

  @Test
  fun `api-keys create prints response`() {
    val idToken = "fake-id-token"
    val args =
      commonArgs +
        arrayOf(
          "api-keys",
          "--id-token=$idToken",
          "create",
          "--measurement-consumer=$MEASUREMENT_CONSUMER_NAME",
          "--nickname=${API_KEY.nickname}",
          "--description=${API_KEY.description}",
        )

    val output: String = callCli(args)

    assertThat(
        headerInterceptor
          .captured(ApiKeysGrpcKt.createApiKeyMethod)
          .single()
          .get(AccountConstants.ID_TOKEN_METADATA_KEY)
      )
      .isEqualTo(idToken)
    verifyProtoArgument(apiKeysMock, ApiKeysGrpcKt.ApiKeysCoroutineImplBase::createApiKey)
      .isEqualTo(
        createApiKeyRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          apiKey =
            API_KEY.copy {
              clearName()
              clearAuthenticationKey()
            }
        }
      )
    assertThat(parseTextProto(output.reader(), ApiKey.getDefaultInstance())).isEqualTo(API_KEY)
  }

  @Test
  fun `certificates create prints resource name`() {
    val args =
      commonArgs +
        arrayOf(
          "certificates",
          "--api-key=$AUTHENTICATION_KEY",
          "create",
          "--parent=$MEASUREMENT_CONSUMER_NAME",
          "--certificate=${MEASUREMENT_CONSUMER_CERTIFICATE_FILE.path}",
        )
    val certificate = certificate {
      name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      x509Der = MEASUREMENT_CONSUMER.certificateDer
    }
    certificatesServiceMock.stub { onBlocking { createCertificate(any()) }.thenReturn(certificate) }

    val output: String = callCli(args)

    assertThat(
        headerInterceptor
          .captured(CertificatesGrpcKt.createCertificateMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(AUTHENTICATION_KEY)
    verifyProtoArgument(
        certificatesServiceMock,
        CertificatesGrpcKt.CertificatesCoroutineImplBase::createCertificate,
      )
      .isEqualTo(
        createCertificateRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          this.certificate = certificate.copy { clearName() }
        }
      )
    assertThat(output).isEqualTo("Certificate name: ${certificate.name}\n")
  }

  @Test
  fun `certificates revoke prints response`() {
    val certificate = certificate {
      name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      x509Der = MEASUREMENT_CONSUMER.certificateDer
      revocationState = Certificate.RevocationState.REVOKED
    }
    certificatesServiceMock.stub { onBlocking { revokeCertificate(any()) }.thenReturn(certificate) }
    val args =
      commonArgs +
        arrayOf(
          "certificates",
          "--api-key=$AUTHENTICATION_KEY",
          "revoke",
          "--revocation-state=REVOKED",
          certificate.name,
        )

    val output: String = callCli(args)

    assertThat(
        headerInterceptor
          .captured(CertificatesGrpcKt.revokeCertificateMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(AUTHENTICATION_KEY)
    verifyProtoArgument(
        certificatesServiceMock,
        CertificatesGrpcKt.CertificatesCoroutineImplBase::revokeCertificate,
      )
      .isEqualTo(
        revokeCertificateRequest {
          name = certificate.name
          revocationState = certificate.revocationState
        }
      )
    assertThat(parseTextProto(output.reader(), Certificate.getDefaultInstance()))
      .isEqualTo(certificate)
  }

  @Test
  fun `public-keys update prints response`() {
    val publicKey = publicKey {
      name = "$MEASUREMENT_CONSUMER_NAME/publicKey"
      publicKey = MEASUREMENT_CONSUMER.publicKey
      certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
    }
    publicKeysServiceMock.stub { onBlocking { updatePublicKey(any()) }.thenReturn(publicKey) }
    val args =
      commonArgs +
        arrayOf(
          "public-keys",
          "--api-key=$AUTHENTICATION_KEY",
          "update",
          "--public-key=${MEASUREMENT_CONSUMER_PUBLIC_KEY_FILE.path}",
          "--public-key-signature=${MEASUREMENT_CONSUMER_PUBLIC_KEY_SIG_FILE.path}",
          "--certificate=${publicKey.certificate}",
          publicKey.name,
        )

    val output: String = callCli(args)

    assertThat(
        headerInterceptor
          .captured(PublicKeysGrpcKt.updatePublicKeyMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(AUTHENTICATION_KEY)
    verifyProtoArgument(
        publicKeysServiceMock,
        PublicKeysGrpcKt.PublicKeysCoroutineImplBase::updatePublicKey,
      )
      .isEqualTo(updatePublicKeyRequest { this.publicKey = publicKey })
    assertThat(parseTextProto(output.reader(), PublicKey.getDefaultInstance())).isEqualTo(publicKey)
  }

  @Test
  fun `measurements create calls CreateMeasurement with valid request for ReachAndFrequency`() {
    val requestId = "foo"
    val measurementReferenceId = "9999"
    val measurementConsumerName = "measurementConsumers/777"
    val args =
      commonArgs +
        arrayOf(
          "measurements",
          "--api-key=$AUTHENTICATION_KEY",
          "create",
          "--reach-and-frequency",
          "--rf-reach-privacy-epsilon=0.015",
          "--rf-reach-privacy-delta=0.0",
          "--rf-frequency-privacy-epsilon=0.02",
          "--rf-frequency-privacy-delta=0.0",
          "--max-frequency=5",
          "--vid-sampling-start=0.1",
          "--vid-sampling-width=0.2",
          "--measurement-consumer=$measurementConsumerName",
          "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
          "--measurement-ref-id=$measurementReferenceId",
          "--request-id=$requestId",
          "--event-data-provider=dataProviders/1",
          "--event-filter=abcd",
          "--event-group=dataProviders/1/eventGroups/1",
          "--event-start-time=$TIME_STRING_1",
          "--event-end-time=$TIME_STRING_2",
          "--event-group=dataProviders/1/eventGroups/2",
          "--event-start-time=$TIME_STRING_3",
          "--event-end-time=$TIME_STRING_4",
          "--event-data-provider=dataProviders/2",
          "--event-filter=ijk",
          "--event-group=dataProviders/2/eventGroups/1",
          "--event-start-time=$TIME_STRING_5",
          "--event-end-time=$TIME_STRING_6",
        )

    val output = callCli(args)

    assertThat(output).matches(("Measurement Name: [-_a-zA-Z0-9./]+\\s*"))

    // verify api key
    assertThat(
        headerInterceptor
          .captured(MeasurementsGrpcKt.createMeasurementMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(AUTHENTICATION_KEY)

    // Verify request.
    val request: CreateMeasurementRequest = captureFirst {
      runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
    }
    assertThat(request)
      .ignoringFieldDescriptors(
        MEASUREMENT_SPEC_FIELD,
        ENCRYPTED_REQUISITION_SPEC_FIELD,
        NONCE_HASH_FIELD,
      )
      .isEqualTo(
        createMeasurementRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          measurement = measurement {
            measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
            dataProviders += dataProviderEntry {
              key = "dataProviders/1"
              value =
                MeasurementKt.DataProviderEntryKt.value {
                  dataProviderCertificate = DATA_PROVIDER.certificate
                  dataProviderPublicKey = DATA_PROVIDER.publicKey.message
                }
            }
            dataProviders += dataProviderEntry {
              key = "dataProviders/2"
              value =
                MeasurementKt.DataProviderEntryKt.value {
                  dataProviderCertificate = DATA_PROVIDER.certificate
                  dataProviderPublicKey = DATA_PROVIDER.publicKey.message
                }
            }
            this.measurementReferenceId = measurementReferenceId
          }
          this.requestId = requestId
        }
      )

    // Verify MeasurementSpec.
    verifyMeasurementSpec(
      request.measurement.measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )
    val measurementSpec: MeasurementSpec = request.measurement.measurementSpec.unpack()
    assertThat(measurementSpec)
      .isEqualTo(
        measurementSpec {
          measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop reading this
          // field.
          serializedMeasurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message.value
          this.nonceHashes += request.measurement.dataProvidersList.map { it.value.nonceHash }
          reachAndFrequency =
            MeasurementSpecKt.reachAndFrequency {
              reachPrivacyParams = differentialPrivacyParams {
                epsilon = 0.015
                delta = 0.0
              }
              frequencyPrivacyParams = differentialPrivacyParams {
                epsilon = 0.02
                delta = 0.0
              }
              maximumFrequency = 5
            }
          vidSamplingInterval =
            MeasurementSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.2f
            }
        }
      )

    // Verify first RequisitionSpec.
    val signedRequisitionSpec1 =
      decryptRequisitionSpec(
        request.measurement.dataProvidersList[0].value.encryptedRequisitionSpec,
        DATA_PROVIDER_PRIVATE_KEY_HANDLE,
      )
    val requisitionSpec1: RequisitionSpec = signedRequisitionSpec1.unpack()
    verifyRequisitionSpec(
      signedRequisitionSpec1,
      requisitionSpec1,
      measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )
    assertThat(requisitionSpec1)
      .ignoringFields(RequisitionSpec.NONCE_FIELD_NUMBER)
      .isEqualTo(
        requisitionSpec {
          measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
          // field.
          serializedMeasurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message.value
          events =
            RequisitionSpecKt.events {
              eventGroups +=
                RequisitionSpecKt.eventGroupEntry {
                  key = "dataProviders/1/eventGroups/1"
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      collectionInterval = interval {
                        startTime = Instant.parse(TIME_STRING_1).toProtoTime()
                        endTime = Instant.parse(TIME_STRING_2).toProtoTime()
                      }
                      filter = RequisitionSpecKt.eventFilter { expression = "abcd" }
                    }
                }
              eventGroups +=
                RequisitionSpecKt.eventGroupEntry {
                  key = "dataProviders/1/eventGroups/2"
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      collectionInterval = interval {
                        startTime = Instant.parse(TIME_STRING_3).toProtoTime()
                        endTime = Instant.parse(TIME_STRING_4).toProtoTime()
                      }
                      filter = RequisitionSpecKt.eventFilter { expression = "abcd" }
                    }
                }
            }
        }
      )

    // Verify second RequisitionSpec.
    val dataProviderEntry2 = request.measurement.dataProvidersList[1]
    assertThat(dataProviderEntry2.key).isEqualTo("dataProviders/2")
    val signedRequisitionSpec2 =
      decryptRequisitionSpec(
        dataProviderEntry2.value.encryptedRequisitionSpec,
        DATA_PROVIDER_PRIVATE_KEY_HANDLE,
      )
    val requisitionSpec2: RequisitionSpec = signedRequisitionSpec2.unpack()
    verifyRequisitionSpec(
      signedRequisitionSpec2,
      requisitionSpec2,
      measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )
    assertThat(requisitionSpec2)
      .ignoringFields(RequisitionSpec.NONCE_FIELD_NUMBER)
      .isEqualTo(
        requisitionSpec {
          measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
          // field.
          serializedMeasurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message.value
          events =
            RequisitionSpecKt.events {
              eventGroups +=
                RequisitionSpecKt.eventGroupEntry {
                  key = "dataProviders/2/eventGroups/1"
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      collectionInterval = interval {
                        startTime = Instant.parse(TIME_STRING_5).toProtoTime()
                        endTime = Instant.parse(TIME_STRING_6).toProtoTime()
                      }
                      filter = RequisitionSpecKt.eventFilter { expression = "ijk" }
                    }
                }
            }
        }
      )
  }

  @Test
  fun `measurements create calls CreateMeasurement with valid request for Reach`() {
    val requestId = "foo"
    val measurementReferenceId = "7777"
    val measurementConsumerName = "measurementConsumers/777"
    val args =
      commonArgs +
        arrayOf(
          "measurements",
          "--api-key=$AUTHENTICATION_KEY",
          "create",
          "--reach",
          "--reach-privacy-epsilon=0.015",
          "--reach-privacy-delta=0.0",
          "--vid-sampling-start=0.1",
          "--vid-sampling-width=0.2",
          "--measurement-consumer=$measurementConsumerName",
          "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
          "--measurement-ref-id=$measurementReferenceId",
          "--request-id=$requestId",
          "--event-data-provider=dataProviders/1",
          "--event-filter=abcd",
          "--event-group=dataProviders/1/eventGroups/1",
          "--event-start-time=$TIME_STRING_1",
          "--event-end-time=$TIME_STRING_2",
          "--event-group=dataProviders/1/eventGroups/2",
          "--event-start-time=$TIME_STRING_3",
          "--event-end-time=$TIME_STRING_4",
          "--event-data-provider=dataProviders/2",
          "--event-filter=ijk",
          "--event-group=dataProviders/2/eventGroups/1",
          "--event-start-time=$TIME_STRING_5",
          "--event-end-time=$TIME_STRING_6",
        )

    val output = callCli(args)

    assertThat(output).matches(("Measurement Name: [-_a-zA-Z0-9./]+\\s*"))

    // verify api key
    assertThat(
        headerInterceptor
          .captured(MeasurementsGrpcKt.createMeasurementMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(AUTHENTICATION_KEY)

    // Verify request.
    val request: CreateMeasurementRequest = captureFirst {
      runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
    }
    assertThat(request)
      .ignoringFieldDescriptors(
        MEASUREMENT_SPEC_FIELD,
        ENCRYPTED_REQUISITION_SPEC_FIELD,
        NONCE_HASH_FIELD,
      )
      .isEqualTo(
        createMeasurementRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          measurement = measurement {
            measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
            dataProviders += dataProviderEntry {
              key = "dataProviders/1"
              value =
                MeasurementKt.DataProviderEntryKt.value {
                  dataProviderCertificate = DATA_PROVIDER.certificate
                  dataProviderPublicKey = DATA_PROVIDER.publicKey.message
                }
            }
            dataProviders += dataProviderEntry {
              key = "dataProviders/2"
              value =
                MeasurementKt.DataProviderEntryKt.value {
                  dataProviderCertificate = DATA_PROVIDER.certificate
                  dataProviderPublicKey = DATA_PROVIDER.publicKey.message
                }
            }
            this.measurementReferenceId = measurementReferenceId
          }
          this.requestId = requestId
        }
      )

    // Verify MeasurementSpec.
    verifyMeasurementSpec(
      request.measurement.measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )
    val measurementSpec: MeasurementSpec = request.measurement.measurementSpec.unpack()
    assertThat(measurementSpec)
      .isEqualTo(
        measurementSpec {
          measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop reading this
          // field.
          serializedMeasurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message.value
          this.nonceHashes += request.measurement.dataProvidersList.map { it.value.nonceHash }
          reach =
            MeasurementSpecKt.reach {
              privacyParams = differentialPrivacyParams {
                epsilon = 0.015
                delta = 0.0
              }
            }
          vidSamplingInterval =
            MeasurementSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.2f
            }
        }
      )

    // Verify first RequisitionSpec.
    val signedRequisitionSpec1 =
      decryptRequisitionSpec(
        request.measurement.dataProvidersList[0].value.encryptedRequisitionSpec,
        DATA_PROVIDER_PRIVATE_KEY_HANDLE,
      )
    val requisitionSpec1: RequisitionSpec = signedRequisitionSpec1.unpack()
    verifyRequisitionSpec(
      signedRequisitionSpec1,
      requisitionSpec1,
      measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )
    assertThat(requisitionSpec1)
      .ignoringFields(RequisitionSpec.NONCE_FIELD_NUMBER)
      .isEqualTo(
        requisitionSpec {
          measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
          // field.
          serializedMeasurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message.value
          events =
            RequisitionSpecKt.events {
              eventGroups +=
                RequisitionSpecKt.eventGroupEntry {
                  key = "dataProviders/1/eventGroups/1"
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      collectionInterval = interval {
                        startTime = Instant.parse(TIME_STRING_1).toProtoTime()
                        endTime = Instant.parse(TIME_STRING_2).toProtoTime()
                      }
                      filter = RequisitionSpecKt.eventFilter { expression = "abcd" }
                    }
                }
              eventGroups +=
                RequisitionSpecKt.eventGroupEntry {
                  key = "dataProviders/1/eventGroups/2"
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      collectionInterval = interval {
                        startTime = Instant.parse(TIME_STRING_3).toProtoTime()
                        endTime = Instant.parse(TIME_STRING_4).toProtoTime()
                      }
                      filter = RequisitionSpecKt.eventFilter { expression = "abcd" }
                    }
                }
            }
        }
      )

    // Verify second RequisitionSpec.
    val dataProviderEntry2 = request.measurement.dataProvidersList[1]
    assertThat(dataProviderEntry2.key).isEqualTo("dataProviders/2")
    val signedRequisitionSpec2 =
      decryptRequisitionSpec(
        dataProviderEntry2.value.encryptedRequisitionSpec,
        DATA_PROVIDER_PRIVATE_KEY_HANDLE,
      )
    val requisitionSpec2: RequisitionSpec = signedRequisitionSpec2.unpack()
    verifyRequisitionSpec(
      signedRequisitionSpec2,
      requisitionSpec2,
      measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )
    assertThat(requisitionSpec2)
      .ignoringFields(RequisitionSpec.NONCE_FIELD_NUMBER)
      .isEqualTo(
        requisitionSpec {
          measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
          // field.
          serializedMeasurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message.value
          events =
            RequisitionSpecKt.events {
              eventGroups +=
                RequisitionSpecKt.eventGroupEntry {
                  key = "dataProviders/2/eventGroups/1"
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      collectionInterval = interval {
                        startTime = Instant.parse(TIME_STRING_5).toProtoTime()
                        endTime = Instant.parse(TIME_STRING_6).toProtoTime()
                      }
                      filter = RequisitionSpecKt.eventFilter { expression = "ijk" }
                    }
                }
            }
        }
      )
  }

  @Test
  fun `measurements create calls CreateMeasurement with correct impression params`() {
    val args =
      commonArgs +
        arrayOf(
          "measurements",
          "--api-key=$AUTHENTICATION_KEY",
          "create",
          "--measurement-consumer=measurementConsumers/777",
          "--impression",
          "--impression-privacy-epsilon=0.015",
          "--impression-privacy-delta=0.0",
          "--max-frequency-per-user=1000",
          "--vid-sampling-start=0.1",
          "--vid-sampling-width=0.2",
          "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
          "--event-data-provider=dataProviders/1",
          "--event-filter=abcd",
          "--event-group=dataProviders/1/eventGroups/1",
          "--event-start-time=$TIME_STRING_1",
          "--event-end-time=$TIME_STRING_2",
        )
    callCli(args)

    val request: CreateMeasurementRequest = captureFirst {
      runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
    }

    val measurement = request.measurement
    val measurementSpec: MeasurementSpec = measurement.measurementSpec.unpack()
    assertThat(measurementSpec)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        measurementSpec {
          impression =
            MeasurementSpecKt.impression {
              privacyParams = differentialPrivacyParams {
                epsilon = 0.015
                delta = 0.0
              }
              maximumFrequencyPerUser = 1000
            }
          vidSamplingInterval =
            MeasurementSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.2f
            }
        }
      )
  }

  @Test
  fun `measurements create calls CreateMeasurement with correct duration params`() {
    val args =
      commonArgs +
        arrayOf(
          "measurements",
          "--api-key=$AUTHENTICATION_KEY",
          "create",
          "--measurement-consumer=measurementConsumers/777",
          "--duration",
          "--duration-privacy-epsilon=0.015",
          "--duration-privacy-delta=0.0",
          "--max-duration=5m20s",
          "--vid-sampling-start=0.1",
          "--vid-sampling-width=0.2",
          "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
          "--event-data-provider=dataProviders/1",
          "--event-filter=abcd",
          "--event-group=dataProviders/1/eventGroups/1",
          "--event-start-time=$TIME_STRING_1",
          "--event-end-time=$TIME_STRING_2",
        )
    callCli(args)

    val request: CreateMeasurementRequest = captureFirst {
      runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
    }

    val measurement = request.measurement
    val measurementSpec: MeasurementSpec = measurement.measurementSpec.unpack()
    assertThat(measurementSpec)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        measurementSpec {
          duration =
            MeasurementSpecKt.duration {
              privacyParams = differentialPrivacyParams {
                epsilon = 0.015
                delta = 0.0
              }
              maximumWatchDurationPerUser =
                Durations.add(Durations.fromMinutes(5), Durations.fromSeconds(20))
            }
          vidSamplingInterval =
            MeasurementSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.2f
            }
        }
      )
  }

  @Test
  fun `measurements create calls CreateMeasurement with correct population params`() {
    val args =
      commonArgs +
        arrayOf(
          "measurements",
          "--api-key=$AUTHENTICATION_KEY",
          "create",
          "--measurement-consumer=measurementConsumers/777",
          "--model-line=modelProviders/1/modelSuites/2/modelLines/3",
          "--population",
          "--private-key-der-file=$SECRETS_DIR/mc_cs_private.der",
          "--population-data-provider=dataProviders/1",
          "--population-filter=abcd",
          "--population-start-time=$TIME_STRING_1",
          "--population-end-time=$TIME_STRING_2",
        )
    callCli(args)

    val request: CreateMeasurementRequest = captureFirst {
      runBlocking { verify(measurementsServiceMock).createMeasurement(capture()) }
    }

    val measurement = request.measurement
    val measurementSpec: MeasurementSpec = measurement.measurementSpec.unpack()
    assertThat(measurementSpec)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        measurementSpec {
          population = MeasurementSpecKt.population {}
          modelLine = "modelProviders/1/modelSuites/2/modelLines/3"
        }
      )

    // Verify first RequisitionSpec.
    val signedRequisitionSpec =
      decryptRequisitionSpec(
        request.measurement.dataProvidersList.single().value.encryptedRequisitionSpec,
        DATA_PROVIDER_PRIVATE_KEY_HANDLE,
      )
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
    verifyRequisitionSpec(
      signedRequisitionSpec,
      requisitionSpec,
      measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )
    assertThat(requisitionSpec)
      .ignoringFields(RequisitionSpec.NONCE_FIELD_NUMBER)
      .isEqualTo(
        requisitionSpec {
          measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
          // field.
          serializedMeasurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message.value
          population =
            RequisitionSpecKt.population {
              filter = RequisitionSpecKt.eventFilter { expression = "abcd" }
              interval = interval {
                startTime = Instant.parse(TIME_STRING_1).toProtoTime()
                endTime = Instant.parse(TIME_STRING_2).toProtoTime()
              }
            }
        }
      )
  }

  @Test
  fun `measurements list calls ListMeasurements with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "measurements",
          "--api-key=$AUTHENTICATION_KEY",
          "list",
          "--measurement-consumer=$MEASUREMENT_CONSUMER_NAME",
        )
    callCli(args)

    // verify api key
    assertThat(
        headerInterceptor
          .captured(MeasurementsGrpcKt.listMeasurementsMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(AUTHENTICATION_KEY)

    val request: ListMeasurementsRequest = captureFirst {
      runBlocking { verify(measurementsServiceMock).listMeasurements(capture()) }
    }
    assertThat(request)
      .comparingExpectedFieldsOnly()
      .isEqualTo(listMeasurementsRequest { parent = MEASUREMENT_CONSUMER_NAME })
  }

  @Test
  fun `measurements get calls GetMeasurement with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "measurements",
          "--api-key=$AUTHENTICATION_KEY",
          "get",
          "--encryption-private-key-file=$SECRETS_DIR/mc_enc_private.tink",
          MEASUREMENT_NAME,
        )

    callCli(args)

    // verify api key
    assertThat(
        headerInterceptor
          .captured(MeasurementsGrpcKt.getMeasurementMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(AUTHENTICATION_KEY)

    val request: GetMeasurementRequest = captureFirst {
      runBlocking { verify(measurementsServiceMock).getMeasurement(capture()) }
    }
    assertThat(request)
      .comparingExpectedFieldsOnly()
      .isEqualTo(getMeasurementRequest { name = MEASUREMENT_NAME })
  }

  @Test
  fun `data-providers replace-required-duchies calls ReplaceDataProviderRequiredDuchies`() {
    val args =
      commonArgs +
        arrayOf(
          "data-providers",
          "--name=$DATA_PROVIDER_NAME",
          "replace-required-duchies",
          "--required-duchies=duchies/worker1",
        )
    callCli(args)

    val request: ReplaceDataProviderRequiredDuchiesRequest = captureFirst {
      runBlocking { verify(dataProvidersServiceMock).replaceDataProviderRequiredDuchies(capture()) }
    }

    assertThat(request)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        replaceDataProviderRequiredDuchiesRequest {
          name = DATA_PROVIDER_NAME
          requiredDuchies += DUCHY_NAMES
        }
      )
  }

  @Test
  fun `data-providers update-capabilities calls ReplaceDataProviderCapabilities`() {
    val args =
      commonArgs +
        arrayOf(
          "data-providers",
          "--name=$DATA_PROVIDER_NAME",
          "update-capabilities",
          "--hmss-supported=true",
        )

    callCli(args)

    val request = captureFirst {
      runBlocking { verify(dataProvidersServiceMock).replaceDataProviderCapabilities(capture()) }
    }
    assertThat(request)
      .isEqualTo(
        replaceDataProviderCapabilitiesRequest {
          name = DATA_PROVIDER_NAME
          capabilities =
            DATA_PROVIDER.capabilities.copy { honestMajorityShareShuffleSupported = true }
        }
      )
  }

  @Test
  fun `data-providers get calls GetDataProvider with correct params`() {
    val args = commonArgs + arrayOf("data-providers", "--name=dataProviders/777", "get")
    callCli(args)
    dataProvidersServiceMock.stub {
      onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
    }
    val request: GetDataProviderRequest = captureFirst {
      runBlocking { verify(dataProvidersServiceMock).getDataProvider(capture()) }
    }
    val dataProviderName = request.name
    assertThat(dataProviderName).isEqualTo("dataProviders/777")
  }

  @Test
  fun `create model line succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-lines",
          "create",
          "--parent=$MODEL_SUITE_NAME",
          "--display-name=Display name",
          "--description=Description",
          "--active-start-time=$MODEL_LINE_ACTIVE_START_TIME",
          "--active-end-time=$MODEL_LINE_ACTIVE_END_TIME",
          "--type=PROD",
          "--holdback-model-line=$HOLDBACK_MODEL_LINE_NAME",
        )
    callCli(args)

    val request: CreateModelLineRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).createModelLine(capture()) }
    }

    assertThat(request.modelLine)
      .ignoringFields(
        ModelLine.CREATE_TIME_FIELD_NUMBER,
        ModelLine.UPDATE_TIME_FIELD_NUMBER,
        ModelLine.NAME_FIELD_NUMBER,
      )
      .isEqualTo(MODEL_LINE)
  }

  @Test
  fun `create model line succeeds omitting optional params`() {
    val args =
      commonArgs +
        arrayOf(
          "model-lines",
          "create",
          "--parent=$MODEL_SUITE_NAME",
          "--active-start-time=$MODEL_LINE_ACTIVE_START_TIME",
          "--type=PROD",
        )
    callCli(args)

    val request: CreateModelLineRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).createModelLine(capture()) }
    }

    assertThat(request.modelLine)
      .ignoringFields(
        ModelLine.CREATE_TIME_FIELD_NUMBER,
        ModelLine.UPDATE_TIME_FIELD_NUMBER,
        ModelLine.NAME_FIELD_NUMBER,
      )
      .isEqualTo(
        MODEL_LINE.copy {
          description = ""
          displayName = ""
          holdbackModelLine = ""
          clearActiveEndTime()
        }
      )
  }

  @Test
  fun `set holdback model line succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-lines",
          "setHoldbackModelLine",
          "--name=$MODEL_LINE_NAME",
          "--holdback-model-line=$HOLDBACK_MODEL_LINE_NAME",
        )
    callCli(args)

    val request: SetModelLineHoldbackModelLineRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).setModelLineHoldbackModelLine(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        setModelLineHoldbackModelLineRequest {
          name = MODEL_LINE_NAME
          holdbackModelLine = HOLDBACK_MODEL_LINE_NAME
        }
      )
  }

  @Test
  fun `set active end time succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-lines",
          "setActiveEndTime",
          "--name=$MODEL_LINE_NAME",
          "--active-end-time=$MODEL_LINE_ACTIVE_END_TIME",
        )
    callCli(args)

    val request: SetModelLineActiveEndTimeRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).setModelLineActiveEndTime(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        setModelLineActiveEndTimeRequest {
          name = MODEL_LINE_NAME
          activeEndTime = Instant.parse(MODEL_LINE_ACTIVE_END_TIME).toProtoTime()
        }
      )
  }

  @Test
  fun `list model lines succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-lines",
          "list",
          "--parent=$MODEL_SUITE_NAME",
          "--page-size=$LIST_PAGE_SIZE",
          "--page-token=$LIST_PAGE_TOKEN",
          "--types=PROD",
        )
    callCli(args)

    val request: ListModelLinesRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).listModelLines(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelLinesRequest {
          parent = MODEL_SUITE_NAME
          pageSize = LIST_PAGE_SIZE
          pageToken = LIST_PAGE_TOKEN
          filter = filter { typeIn += ModelLine.Type.PROD }
        }
      )
  }

  @Test
  fun `list model lines succeeds omitting optional params`() {
    val args = commonArgs + arrayOf("model-lines", "list", "--parent=$MODEL_SUITE_NAME")
    callCli(args)

    val request: ListModelLinesRequest = captureFirst {
      runBlocking { verify(modelLinesServiceMock).listModelLines(capture()) }
    }

    assertThat(request).isEqualTo(listModelLinesRequest { parent = MODEL_SUITE_NAME })
  }

  @Test
  fun `create model release succeeds`() {
    val args = commonArgs + arrayOf("model-releases", "create", "--parent=$MODEL_SUITE_NAME")
    callCli(args)

    val request: CreateModelReleaseRequest = captureFirst {
      runBlocking { verify(modelReleasesServiceMock).createModelRelease(capture()) }
    }

    assertThat(request.modelRelease)
      .ignoringFields(ModelRelease.CREATE_TIME_FIELD_NUMBER, ModelRelease.NAME_FIELD_NUMBER)
      .isEqualTo(MODEL_RELEASE)
  }

  @Test
  fun `get model release succeeds`() {
    val args = commonArgs + arrayOf("model-releases", "get", "--name=$MODEL_RELEASE_NAME")
    callCli(args)

    val request: GetModelReleaseRequest = captureFirst {
      runBlocking { verify(modelReleasesServiceMock).getModelRelease(capture()) }
    }

    assertThat(request).isEqualTo(getModelReleaseRequest { name = MODEL_RELEASE_NAME })
  }

  @Test
  fun `list model releases succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-releases",
          "list",
          "--parent=$MODEL_SUITE_NAME",
          "--page-size=$LIST_PAGE_SIZE",
          "--page-token=$LIST_PAGE_TOKEN",
        )
    callCli(args)

    val request: ListModelReleasesRequest = captureFirst {
      runBlocking { verify(modelReleasesServiceMock).listModelReleases(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelReleasesRequest {
          parent = MODEL_SUITE_NAME
          pageSize = LIST_PAGE_SIZE
          pageToken = LIST_PAGE_TOKEN
        }
      )
  }

  @Test
  fun `list model releases succeeds omitting optional params`() {
    val args = commonArgs + arrayOf("model-releases", "list", "--parent=$MODEL_SUITE_NAME")
    callCli(args)

    val request: ListModelReleasesRequest = captureFirst {
      runBlocking { verify(modelReleasesServiceMock).listModelReleases(capture()) }
    }

    assertThat(request).isEqualTo(listModelReleasesRequest { parent = MODEL_SUITE_NAME })
  }

  @Test
  fun `create model outage succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-outages",
          "create",
          "--parent=$MODEL_LINE_NAME",
          "--outage-start-time=$MODEL_OUTAGE_ACTIVE_START_TIME",
          "--outage-end-time=$MODEL_OUTAGE_ACTIVE_END_TIME",
        )
    callCli(args)

    val request: CreateModelOutageRequest = captureFirst {
      runBlocking { verify(modelOutagesServiceMock).createModelOutage(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createModelOutageRequest {
          parent = MODEL_LINE_NAME
          modelOutage = modelOutage {
            outageInterval = interval {
              startTime = Instant.parse(MODEL_OUTAGE_ACTIVE_START_TIME).toProtoTime()
              endTime = Instant.parse(MODEL_OUTAGE_ACTIVE_END_TIME).toProtoTime()
            }
          }
        }
      )
  }

  @Test
  fun `list model outages succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-outages",
          "list",
          "--parent=$MODEL_LINE_NAME",
          "--page-size=$LIST_PAGE_SIZE",
          "--page-token=$LIST_PAGE_TOKEN",
          "--show-deleted=true",
          "--interval-start-time=$MODEL_OUTAGE_ACTIVE_START_TIME",
          "--interval-end-time=$MODEL_OUTAGE_ACTIVE_END_TIME",
        )
    callCli(args)

    val request: ListModelOutagesRequest = captureFirst {
      runBlocking { verify(modelOutagesServiceMock).listModelOutages(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelOutagesRequest {
          parent = MODEL_LINE_NAME
          pageSize = LIST_PAGE_SIZE
          pageToken = LIST_PAGE_TOKEN
          showDeleted = true
          filter =
            ListModelOutagesRequestKt.filter {
              outageIntervalOverlapping = interval {
                startTime = Instant.parse(MODEL_OUTAGE_ACTIVE_START_TIME).toProtoTime()
                endTime = Instant.parse(MODEL_OUTAGE_ACTIVE_END_TIME).toProtoTime()
              }
            }
        }
      )
  }

  @Test
  fun `list model outages succeeds omitting optional params`() {
    val args = commonArgs + arrayOf("model-outages", "list", "--parent=$MODEL_LINE_NAME")
    callCli(args)

    val request: ListModelOutagesRequest = captureFirst {
      runBlocking { verify(modelOutagesServiceMock).listModelOutages(capture()) }
    }

    assertThat(request).isEqualTo(listModelOutagesRequest { parent = MODEL_LINE_NAME })
  }

  @Test
  fun `delete model outage succeeds`() {
    val args = commonArgs + arrayOf("model-outages", "delete", "--name=$MODEL_OUTAGE_NAME")
    callCli(args)

    val request: DeleteModelOutageRequest = captureFirst {
      runBlocking { verify(modelOutagesServiceMock).deleteModelOutage(capture()) }
    }

    assertThat(request.name).isEqualTo(MODEL_OUTAGE_NAME)
  }

  @Test
  fun `create model shard succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-shards",
          "create",
          "--parent=$DATA_PROVIDER_NAME",
          "--model-release=$MODEL_RELEASE_NAME",
          "--model-blob-path=$MODEL_BLOB_PATH",
        )
    callCli(args)

    val request: CreateModelShardRequest = captureFirst {
      runBlocking { verify(modelShardsServiceMock).createModelShard(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createModelShardRequest {
          parent = DATA_PROVIDER_NAME
          modelShard = modelShard {
            modelRelease = MODEL_RELEASE_NAME
            modelBlob = modelBlob { modelBlobPath = MODEL_BLOB_PATH }
          }
        }
      )
  }

  @Test
  fun `list model shards succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-shards",
          "list",
          "--parent=$DATA_PROVIDER_NAME",
          "--page-size=$LIST_PAGE_SIZE",
          "--page-token=$LIST_PAGE_TOKEN",
        )
    callCli(args)

    val request: ListModelShardsRequest = captureFirst {
      runBlocking { verify(modelShardsServiceMock).listModelShards(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelShardsRequest {
          parent = DATA_PROVIDER_NAME
          pageSize = LIST_PAGE_SIZE
          pageToken = LIST_PAGE_TOKEN
        }
      )
  }

  @Test
  fun `list model shards succeeds omitting optional params`() {
    val args = commonArgs + arrayOf("model-shards", "list", "--parent=$DATA_PROVIDER_NAME")
    callCli(args)

    val request: ListModelShardsRequest = captureFirst {
      runBlocking { verify(modelShardsServiceMock).listModelShards(capture()) }
    }

    assertThat(request).isEqualTo(listModelShardsRequest { parent = DATA_PROVIDER_NAME })
  }

  @Test
  fun `delete model shard succeeds`() {
    val args = commonArgs + arrayOf("model-shards", "delete", "--name=$MODEL_SHARD_NAME")
    callCli(args)

    val request: DeleteModelShardRequest = captureFirst {
      runBlocking { verify(modelShardsServiceMock).deleteModelShard(capture()) }
    }

    assertThat(request.name).isEqualTo(MODEL_SHARD_NAME)
  }

  @Test
  fun `create model rollout succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-rollouts",
          "create",
          "--parent=$MODEL_LINE_NAME",
          "--rollout-start-date=2026-05-24",
          "--rollout-end-date=2026-09-24",
          "--model-release=$MODEL_RELEASE_NAME",
        )
    callCli(args)

    val request: CreateModelRolloutRequest = captureFirst {
      runBlocking { verify(modelRolloutsServiceMock).createModelRollout(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createModelRolloutRequest {
          parent = MODEL_LINE_NAME
          modelRollout = modelRollout {
            gradualRolloutPeriod = dateInterval {
              startDate = MODEL_ROLLOUT_ACTIVE_START_DATE
              endDate = MODEL_ROLLOUT_ACTIVE_END_DATE
            }
            modelRelease = MODEL_RELEASE_NAME
          }
        }
      )
  }

  @Test
  fun `list model rollouts succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-rollouts",
          "list",
          "--parent=$MODEL_LINE_NAME",
          "--page-size=10",
          "--page-token=token",
          "--rollout-period-overlapping-start-date=2026-05-24",
          "--rollout-period-overlapping-end-date=2026-09-24",
        )
    callCli(args)

    val request: ListModelRolloutsRequest = captureFirst {
      runBlocking { verify(modelRolloutsServiceMock).listModelRollouts(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelRolloutsRequest {
          parent = MODEL_LINE_NAME
          pageSize = 10
          pageToken = "token"
          filter =
            ListModelRolloutsRequestKt.filter {
              rolloutPeriodOverlapping = dateInterval {
                startDate = MODEL_ROLLOUT_ACTIVE_START_DATE
                endDate = MODEL_ROLLOUT_ACTIVE_END_DATE
              }
            }
        }
      )
  }

  @Test
  fun `schedule model rollouts freeze time succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-rollouts",
          "schedule",
          "--name=$MODEL_ROLLOUT_NAME",
          "--freeze-time=2026-07-24",
        )
    callCli(args)

    val request: ScheduleModelRolloutFreezeRequest = captureFirst {
      runBlocking { verify(modelRolloutsServiceMock).scheduleModelRolloutFreeze(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        scheduleModelRolloutFreezeRequest {
          name = MODEL_ROLLOUT_NAME
          rolloutFreezeDate = MODEL_ROLLOUT_FREEZE_DATE
        }
      )
  }

  @Test
  fun `delete model rollouts succeeds`() {
    val args = commonArgs + arrayOf("model-rollouts", "delete", "--name=$MODEL_ROLLOUT_NAME")
    callCli(args)

    val request: DeleteModelRolloutRequest = captureFirst {
      runBlocking { verify(modelRolloutsServiceMock).deleteModelRollout(capture()) }
    }

    assertThat(request).isEqualTo(deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME })
  }

  @Test
  fun `create model suite succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "create",
          "--parent=$MODEL_PROVIDER_NAME",
          "--display-name",
          "Display name",
          "--description",
          "Description",
        )
    callCli(args)

    val request: CreateModelSuiteRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).createModelSuite(capture()) }
    }

    assertThat(request.modelSuite)
      .ignoringFields(ModelSuite.CREATE_TIME_FIELD_NUMBER, ModelSuite.NAME_FIELD_NUMBER)
      .isEqualTo(MODEL_SUITE)
  }

  @Test
  fun `create model suite succeeds omitting optional params`() {
    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "create",
          "--parent=$MODEL_PROVIDER_NAME",
          "--display-name",
          "Display name",
        )
    callCli(args)

    val request: CreateModelSuiteRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).createModelSuite(capture()) }
    }

    assertThat(request.modelSuite)
      .ignoringFields(ModelSuite.CREATE_TIME_FIELD_NUMBER, ModelSuite.NAME_FIELD_NUMBER)
      .isEqualTo(MODEL_SUITE.copy { description = "" })
  }

  @Test
  fun `get model suite succeeds`() {
    val args = commonArgs + arrayOf("model-suites", "get", "--name", MODEL_SUITE_NAME)
    callCli(args)

    val request: GetModelSuiteRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).getModelSuite(capture()) }
    }

    assertThat(request).isEqualTo(getModelSuiteRequest { name = MODEL_SUITE_NAME })
  }

  @Test
  fun `list model suites succeeds`() {
    val args =
      commonArgs +
        arrayOf(
          "model-suites",
          "list",
          "--parent=$MODEL_PROVIDER_NAME",
          "--page-size",
          "10",
          "--page-token",
          "token",
        )
    callCli(args)

    val request: ListModelSuitesRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).listModelSuites(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelSuitesRequest {
          parent = MODEL_PROVIDER_NAME
          pageSize = 10
          pageToken = "token"
        }
      )
  }

  @Test
  fun `list model suites succeeds omitting optional params`() {
    val args = commonArgs + arrayOf("model-suites", "list", "--parent=$MODEL_PROVIDER_NAME")
    callCli(args)

    val request: ListModelSuitesRequest = captureFirst {
      runBlocking { verify(modelSuitesServiceMock).listModelSuites(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        listModelSuitesRequest {
          parent = MODEL_PROVIDER_NAME
          pageSize = 0
          pageToken = ""
        }
      )
  }

  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    private val MEASUREMENT_SPEC_FIELD: Descriptors.FieldDescriptor =
      Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER)
    private val ENCRYPTED_REQUISITION_SPEC_FIELD: Descriptors.FieldDescriptor =
      Measurement.DataProviderEntry.Value.getDescriptor()
        .findFieldByNumber(
          Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
        )
    private val NONCE_HASH_FIELD: Descriptors.FieldDescriptor =
      Measurement.DataProviderEntry.Value.getDescriptor()
        .findFieldByNumber(Measurement.DataProviderEntry.Value.NONCE_HASH_FIELD_NUMBER)

    private val MEASUREMENT_CONSUMER: MeasurementConsumer by lazy {
      measurementConsumer {
        name = MEASUREMENT_CONSUMER_NAME
        certificateDer = MEASUREMENT_CONSUMER_CERTIFICATE_FILE.readByteString()
        certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
        publicKey = signedMessage {
          setMessage(
            any {
              value = MEASUREMENT_CONSUMER_PUBLIC_KEY_FILE.readByteString()
              typeUrl = ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
            }
          )
          signature = MEASUREMENT_CONSUMER_PUBLIC_KEY_SIG_FILE.readByteString()
        }
        displayName = "MC Hammer"
      }
    }

    private val MEASUREMENT_CONSUMER_CERTIFICATE: X509Certificate by lazy {
      readCertificate(MEASUREMENT_CONSUMER.certificateDer)
    }
    private val TRUSTED_MEASUREMENT_CONSUMER_ISSUER: X509Certificate by lazy {
      readCertificate(MEASUREMENT_CONSUMER_ROOT_CERTIFICATE_FILE)
    }

    private val MEASUREMENT_CONSUMER_ENCRYPTION_PUBLIC_KEY: EncryptionPublicKey by lazy {
      MEASUREMENT_CONSUMER.publicKey.unpack()
    }

    /*
     * Contains a measurement result for each measurement type
     * TODO(@renjiezh) Have separate successful measurements for each measurement type
     */
    private val SUCCEEDED_MEASUREMENT: Measurement by lazy {
      val measurementPublicKey = MEASUREMENT_CONSUMER_ENCRYPTION_PUBLIC_KEY
      measurement {
        name = MEASUREMENT_NAME
        state = Measurement.State.SUCCEEDED

        results += resultOutput {
          val result = result { reach = reach { value = 4096 } }
          encryptedResult = getEncryptedResult(result, measurementPublicKey)
          certificate = DATA_PROVIDER_CERTIFICATE_NAME
        }
        results += resultOutput {
          val result = result {
            frequency = frequency {
              relativeFrequencyDistribution.put(1, 1.0 / 6)
              relativeFrequencyDistribution.put(2, 3.0 / 6)
              relativeFrequencyDistribution.put(3, 2.0 / 6)
            }
          }
          encryptedResult = getEncryptedResult(result, measurementPublicKey)
          certificate = DATA_PROVIDER_CERTIFICATE_NAME
        }
        results += resultOutput {
          val result = result { impression = impression { value = 4096 } }
          encryptedResult = getEncryptedResult(result, measurementPublicKey)
          certificate = DATA_PROVIDER_CERTIFICATE_NAME
        }
        results += resultOutput {
          val result = result {
            watchDuration = watchDuration {
              value = duration {
                seconds = 100
                nanos = 99
              }
            }
          }
          encryptedResult = getEncryptedResult(result, measurementPublicKey)
          certificate = DATA_PROVIDER_CERTIFICATE_NAME
        }
        results += resultOutput {
          val result = result { population = population { value = 100 } }
          encryptedResult = getEncryptedResult(result, measurementPublicKey)
          certificate = DATA_PROVIDER_CERTIFICATE_NAME
        }
      }
    }
  }
}

private fun getEncryptedResult(
  result: Measurement.Result,
  publicKey: EncryptionPublicKey,
): EncryptedMessage {
  val signedResult = signResult(result, AGGREGATOR_SIGNING_KEY)
  return encryptResult(signedResult, publicKey)
}
