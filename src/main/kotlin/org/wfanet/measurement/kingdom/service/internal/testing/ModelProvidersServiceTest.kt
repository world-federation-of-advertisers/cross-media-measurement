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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.GetModelProviderRequest
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelProvidersRequest
import org.wfanet.measurement.internal.kingdom.StreamModelProvidersRequestKt
import org.wfanet.measurement.internal.kingdom.getModelProviderRequest
import org.wfanet.measurement.internal.kingdom.streamModelProvidersRequest

private const val EXTERNAL_MODEL_PROVIDER_ID = 123L
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L

@RunWith(JUnit4::class)
abstract class ModelProvidersServiceTest {

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID),
    )

  protected lateinit var modelProvidersService: ModelProvidersCoroutineImplBase

  /** Creates a /ModelProviders service implementation using [idGenerator]. */
  protected abstract fun newService(idGenerator: IdGenerator): ModelProvidersCoroutineImplBase

  @Before
  fun initService() {
    modelProvidersService = newService(idGenerator)
  }

  @Test
  fun `getModelProvider fails for missing ModelProvider`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelProvidersService.getModelProvider(
          getModelProviderRequest { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("NOT_FOUND: ModelProvider not found")
  }

  @Test
  fun `createModelProvider succeeds`() = runBlocking {
    val modelProvider = ModelProvider.getDefaultInstance()
    val createdModelProvider = modelProvidersService.createModelProvider(modelProvider)

    assertThat(createdModelProvider)
      .isEqualTo(
        modelProvider
          .toBuilder()
          .apply { externalModelProviderId = FIXED_GENERATED_EXTERNAL_ID }
          .build()
      )
  }

  @Test
  fun `getModelProvider succeeds`() = runBlocking {
    val modelProvider = ModelProvider.getDefaultInstance()
    val createdModelProvider = modelProvidersService.createModelProvider(modelProvider)

    val modelProviderRead =
      modelProvidersService.getModelProvider(
        GetModelProviderRequest.newBuilder()
          .setExternalModelProviderId(createdModelProvider.externalModelProviderId)
          .build()
      )

    assertThat(modelProviderRead).isEqualTo(createdModelProvider)
  }

  @Test
  fun `streamModelProviders returns all model providers`(): Unit = runBlocking {
    val modelProvider1 = ModelProvider.getDefaultInstance()
    val createdModelProvider1 = modelProvidersService.createModelProvider(modelProvider1)

    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID + 1L)
    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID + 1L)
    val modelProvider2 = ModelProvider.getDefaultInstance()
    val createdModelProvider2 = modelProvidersService.createModelProvider(modelProvider2)

    val modelProviders: List<ModelProvider> =
      modelProvidersService
        .streamModelProviders(StreamModelProvidersRequest.getDefaultInstance())
        .toList()

    assertThat(modelProviders)
      .containsExactly(createdModelProvider1, createdModelProvider2)
      .inOrder()
  }

  @Test
  fun `streamModelProviders returns filtered model providers`(): Unit = runBlocking {
    val modelProvider1 = ModelProvider.getDefaultInstance()
    val createdModelProvider1 = modelProvidersService.createModelProvider(modelProvider1)

    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID + 1L)
    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID + 1L)
    val modelProvider2 = ModelProvider.getDefaultInstance()
    val createdModelProvider2 = modelProvidersService.createModelProvider(modelProvider2)

    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID + 2L)
    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID + 2L)
    val modelProvider3 = ModelProvider.getDefaultInstance()
    modelProvidersService.createModelProvider(modelProvider3)

    val modelProviders: List<ModelProvider> =
      modelProvidersService
        .streamModelProviders(
          streamModelProvidersRequest {
            limit = 1
            filter =
              StreamModelProvidersRequestKt.filter {
                after =
                  StreamModelProvidersRequestKt.afterFilter {
                    externalModelProviderId = createdModelProvider1.externalModelProviderId
                  }
              }
          }
        )
        .toList()

    assertThat(modelProviders).containsExactly(createdModelProvider2)
  }

  @Test
  fun `streamModelProviders fails when limit is less than 0`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelProvidersService.streamModelProviders(streamModelProvidersRequest { limit = -1 })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Limit")
  }

  @Test
  fun `streamModelProviders fails for missing after filter fields`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelProvidersService.streamModelProviders(
          streamModelProvidersRequest {
            filter =
              StreamModelProvidersRequestKt.filter {
                after = StreamModelProvidersRequest.AfterFilter.getDefaultInstance()
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
