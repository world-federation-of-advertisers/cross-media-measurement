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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.modelShard

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ModelShardsServiceTest<T : ModelShardsCoroutineImplBase> {

  protected data class Services<T>(
    val modelShardsService: T,
    val dataProvidersService: DataProvidersCoroutineImplBase,
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
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
    println("joji made it here")

    val dataProvider = population.createDataProvider(dataProvidersService)
    println("joji made it here b")

    val modelShard = modelShard {
      externalDataProviderId = dataProvider.externalDataProviderId
      externalModelReleaseId = "modelRelease"
      modelBlobPath = "modelBlobPath"
    }
    println("joji made it here c")

    val createdModelShard = modelShardsService.createModelShard(modelShard)
    println("joji made it here d")

    assertThat(createdModelShard)
      .ignoringFields(
        ModelShard.CREATE_TIME_FIELD_NUMBER,
        ModelShard.EXTERNAL_MODEL_SHARD_ID_FIELD_NUMBER
      )
      .isEqualTo(
        modelShard {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalModelReleaseId = "modelRelease"
          modelBlobPath = "modelBlobPath"
        }
      )

    //TODO(@jojijac0b): Add check to verify modelShard was written to Spanner table once ModelShardReader is done.
  }

  @Test
  fun `createModelShard fails with no externalDataProviderId`() = runBlocking {
    val modelShard = modelShard {
      externalModelReleaseId = "modelRelease"
      modelBlobPath = "modelBlobPath"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { modelShardsService.createModelShard(modelShard) }

    Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Truth.assertThat(exception).hasMessageThat().contains("DataProviderId field of ModelShard is missing.")
  }
}
