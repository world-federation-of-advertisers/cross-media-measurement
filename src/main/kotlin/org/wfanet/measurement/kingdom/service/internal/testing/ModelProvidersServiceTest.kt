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
import org.wfanet.measurement.internal.kingdom.ListModelProvidersPageTokenKt
import org.wfanet.measurement.internal.kingdom.ListModelProvidersRequest
import org.wfanet.measurement.internal.kingdom.ListModelProvidersResponse
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.getModelProviderRequest
import org.wfanet.measurement.internal.kingdom.listModelProvidersPageToken
import org.wfanet.measurement.internal.kingdom.listModelProvidersRequest
import org.wfanet.measurement.internal.kingdom.listModelProvidersResponse

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
  fun `listModelProviders returns all model providers ordered by external ID`(): Unit =
    runBlocking {
      val modelProvider1 = ModelProvider.getDefaultInstance()
      val createdModelProvider1 = modelProvidersService.createModelProvider(modelProvider1)

      idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID + 1L)
      idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID + 1L)
      val modelProvider2 = ModelProvider.getDefaultInstance()
      val createdModelProvider2 = modelProvidersService.createModelProvider(modelProvider2)

      val response: ListModelProvidersResponse =
        modelProvidersService.listModelProviders(ListModelProvidersRequest.getDefaultInstance())

      assertThat(response)
        .isEqualTo(
          listModelProvidersResponse {
            modelProviders += createdModelProvider1
            modelProviders += createdModelProvider2
          }
        )
    }

  @Test
  fun `listModelProviders returns page size number of model providers after page token, and next page token when there are more results`():
    Unit = runBlocking {
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

    val response: ListModelProvidersResponse =
      modelProvidersService.listModelProviders(
        listModelProvidersRequest {
          pageSize = 1
          pageToken = listModelProvidersPageToken {
            after =
              ListModelProvidersPageTokenKt.after {
                externalModelProviderId = createdModelProvider1.externalModelProviderId
              }
          }
        }
      )

    assertThat(response)
      .isEqualTo(
        listModelProvidersResponse {
          modelProviders += createdModelProvider2
          nextPageToken = listModelProvidersPageToken {
            after =
              ListModelProvidersPageTokenKt.after {
                externalModelProviderId = createdModelProvider2.externalModelProviderId
              }
          }
        }
      )
  }

  @Test
  fun `listModelProviders fails when page size is less than 0`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        modelProvidersService.listModelProviders(listModelProvidersRequest { pageSize = -1 })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("less than 0")
  }
}
