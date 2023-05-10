/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.modelShard
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1
private const val EXTERNAL_DATA_PROVIDER_ID = 123L
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")
private val MODEL_RELEASE = 2L
private val MODEL_BLOB_PATH = "model_blob_path"
@RunWith(JUnit4::class)
abstract class ModelShardsServiceTest<T : ModelShardsCoroutineImplBase> {
  @get:Rule
  val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID)
    )

  protected data class Services<T>(
    val modelShardsService: T,
    val dataProvidersService: DataProvidersCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
//  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var modelShardsService: T
    private set

  protected abstract fun newServices(clock: Clock, idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(clock, idGenerator)
    modelShardsService = services.modelShardsService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `createModelShard succeeds`() = runBlocking {
    val dataProvider = dataProvider {
      certificate {
        notValidBefore = timestamp { seconds = 12345 }
        notValidAfter = timestamp { seconds = 23456 }
        details = CertificateKt.details { x509Der = CERTIFICATE_DER }
      }
      details =
        DataProviderKt.details {
          apiVersion = "v2alpha"
          publicKey = PUBLIC_KEY
          publicKeySignature = PUBLIC_KEY_SIGNATURE
        }
      requiredExternalDuchyIds += Population.DUCHIES.map { it.externalDuchyId }
    }
    val createdDataProvider = dataProvidersService.createDataProvider(dataProvider)

    val modelShard = modelShard {
      externalDataProviderId = createdDataProvider.externalDataProviderId
      externalModelReleaseId = MODEL_RELEASE
      modelBlobPath = "modelBlobPath"
    }
    // getting error here because of invalid externalModelReleaseId. we need to create a model release in the ModelReleases table
    val createdModelShard = modelShardsService.createModelShard(modelShard)

    assertThat(createdModelShard)
      .ignoringFields(
        ModelShard.CREATE_TIME_FIELD_NUMBER,
        ModelShard.EXTERNAL_MODEL_SHARD_ID_FIELD_NUMBER
      )
      .isEqualTo(
        modelShard {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalModelReleaseId = MODEL_RELEASE
          modelBlobPath = MODEL_BLOB_PATH
        }
      )

    //TODO(@jojijac0b): Add check to verify modelShard was written to Spanner table once ModelShardReader is done.
  }

  @Test
  fun `createModelShard fails with no externalDataProviderId`() = runBlocking {
    val modelShard = modelShard {
      externalModelReleaseId = MODEL_RELEASE
      modelBlobPath = MODEL_BLOB_PATH
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelShardsService.createModelShard(modelShard) }

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Truth.assertThat(exception).hasMessageThat().contains("DataProviderId field of ModelShard is missing.")
  }
}
