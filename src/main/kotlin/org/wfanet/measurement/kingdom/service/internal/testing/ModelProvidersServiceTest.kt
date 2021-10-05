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
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.getModelProviderRequest

private const val EXTERNAL_MODEL_PROVIDER_ID = 123L
private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L

@RunWith(JUnit4::class)
abstract class ModelProvidersServiceTest {

  protected val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID)
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
}
