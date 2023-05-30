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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Empty
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelRollout
import org.wfanet.measurement.api.v2alpha.ModelRolloutKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.deleteModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsPageToken
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.scheduleModelRolloutFreezeRequest
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelRollout as InternalModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequest
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.filter as internalFilter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.rolloutPeriod
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteModelRolloutRequest as internalDeleteModelRolloutRequest
import org.wfanet.measurement.internal.kingdom.modelRollout as internalModelRollout
import org.wfanet.measurement.internal.kingdom.scheduleModelRolloutFreezeRequest as internalScheduleModelRolloutFreezeRequest
import org.wfanet.measurement.internal.kingdom.streamModelRolloutsRequest as internalStreamModelRolloutsRequest

private const val DEFAULT_LIMIT = 50

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME_2 = "modelProviders/AAAAAAAAAJs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME_2/modelSuites/AAAAAAAAAJs"
private const val MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAHs"
private const val MODEL_LINE_NAME_2 = "$MODEL_SUITE_NAME_2/modelLines/AAAAAAAAAJs"
private const val MODEL_ROLLOUT_NAME = "$MODEL_LINE_NAME/modelRollouts/AAAAAAAAAHs"
private const val MODEL_ROLLOUT_NAME_2 = "$MODEL_LINE_NAME/modelRollouts/AAAAAAAAAJs"
private const val MODEL_ROLLOUT_NAME_3 = "$MODEL_LINE_NAME/modelRollouts/AAAAAAAAAKs"
private const val MODEL_ROLLOUT_NAME_4 = "$MODEL_LINE_NAME_2/modelRollouts/AAAAAAAAAhs"
private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
private const val MODEL_RELEASE_NAME_2 = "$MODEL_SUITE_NAME_2/modelReleases/AAAAAAAAAJs"
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_LINE_ID =
  apiIdToExternalId(ModelLineKey.fromName(MODEL_LINE_NAME)!!.modelLineId)
private val EXTERNAL_MODEL_ROLLOUT_ID =
  apiIdToExternalId(ModelRolloutKey.fromName(MODEL_ROLLOUT_NAME)!!.modelRolloutId)
private val EXTERNAL_MODEL_ROLLOUT_ID_2 =
  apiIdToExternalId(ModelRolloutKey.fromName(MODEL_ROLLOUT_NAME_2)!!.modelRolloutId)
private val EXTERNAL_MODEL_ROLLOUT_ID_3 =
  apiIdToExternalId(ModelRolloutKey.fromName(MODEL_ROLLOUT_NAME_3)!!.modelRolloutId)
private val EXTERNAL_MODEL_RELEASE_ID =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME)!!.modelReleaseId)

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
private val ROLLOUT_PERIOD_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
private val ROLLOUT_PERIOD_END_TIME: Timestamp = Instant.ofEpochSecond(789).toProtoTime()
private val ROLLOUT_FREEZE_TIME: Timestamp = Instant.ofEpochSecond(678).toProtoTime()

private val INTERNAL_MODEL_ROLLOUT: InternalModelRollout = internalModelRollout {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelLineId = EXTERNAL_MODEL_LINE_ID
  externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_2
  rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
  rolloutPeriodEndTime = ROLLOUT_PERIOD_END_TIME
  rolloutFreezeTime = ROLLOUT_FREEZE_TIME
  externalPreviousModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
  externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

private val MODEL_ROLLOUT: ModelRollout = modelRollout {
  name = MODEL_ROLLOUT_NAME_2
  rolloutPeriod = timeInterval {
    startTime = ROLLOUT_PERIOD_START_TIME
    endTime = ROLLOUT_PERIOD_END_TIME
  }
  rolloutFreezeTime = ROLLOUT_FREEZE_TIME
  previousModelRollout = MODEL_ROLLOUT_NAME
  modelRelease = MODEL_RELEASE_NAME
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

private val MODEL_ROLLOUT_2: ModelRollout = modelRollout {
  name = MODEL_ROLLOUT_NAME_2
  rolloutPeriod = timeInterval {
    startTime = ROLLOUT_PERIOD_START_TIME
    endTime = ROLLOUT_PERIOD_END_TIME
  }
  rolloutFreezeTime = ROLLOUT_FREEZE_TIME
  previousModelRollout = MODEL_ROLLOUT_NAME
  modelRelease = MODEL_RELEASE_NAME_2
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

@RunWith(JUnit4::class)
class ModelRolloutsServiceTest {

  private val internalModelRolloutsMock: ModelRolloutsCoroutineImplBase =
    mockService() {
      onBlocking { createModelRollout(any()) }
        .thenAnswer {
          val request = it.getArgument<InternalModelRollout>(0)
          if (request.externalModelLineId != EXTERNAL_MODEL_LINE_ID) {
            failGrpc(Status.NOT_FOUND) { "ModelLine not found" }
          } else {
            INTERNAL_MODEL_ROLLOUT
          }
        }
      onBlocking { scheduleModelRolloutFreeze(any()) }
        .thenReturn(INTERNAL_MODEL_ROLLOUT.copy { rolloutFreezeTime = ROLLOUT_FREEZE_TIME })
      onBlocking { deleteModelRollout(any()) }.thenReturn(INTERNAL_MODEL_ROLLOUT)
      onBlocking { streamModelRollouts(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MODEL_ROLLOUT,
            INTERNAL_MODEL_ROLLOUT.copy { externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_2 },
            INTERNAL_MODEL_ROLLOUT.copy { externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_3 }
          )
        )
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelRolloutsMock) }

  private lateinit var service: ModelRolloutsService

  @Before
  fun initService() {
    service =
      ModelRolloutsService(
        ModelRolloutsCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `createModelRollout returns model rollout`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelRollout(request) }
      }

    val expected = MODEL_ROLLOUT

    verifyProtoArgument(
        internalModelRolloutsMock,
        ModelRolloutsCoroutineImplBase::createModelRollout
      )
      .isEqualTo(
        INTERNAL_MODEL_ROLLOUT.copy {
          clearCreateTime()
          clearUpdateTime()
          clearExternalModelRolloutId()
          clearExternalPreviousModelRolloutId()
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createModelRollout throws UNAUTHENTICATED when no principal is found`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createModelRollout(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createModelRollout throws PERMISSION_DENIED when ModelRelease is owned by a different ModelProvider`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.createModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRollout throws PERMISSION_DENIED when principal is data provider`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRollout throws PERMISSION_DENIED when principal is duchy`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.createModelRollout(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRollout throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRollout throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelRollout(createModelRolloutRequest { modelRollout = MODEL_ROLLOUT })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createModelRollout throws INVALID_ARGUMENT when model release is missing`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT.copy { clearModelRelease() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.createModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createModelRollout throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME
      modelRollout = MODEL_ROLLOUT
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelRollout throws NOT_FOUND if model line is not found`() {
    val request = createModelRolloutRequest {
      parent = MODEL_LINE_NAME_2
      modelRollout =
        MODEL_ROLLOUT.copy {
          name = MODEL_ROLLOUT_NAME_4
          modelRelease = MODEL_RELEASE_NAME_2
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `scheduleModelRolloutFreeze returns model rollout with rollout freeze time`() {
    val request = scheduleModelRolloutFreezeRequest {
      name = MODEL_ROLLOUT_NAME
      rolloutFreezeTime = ROLLOUT_FREEZE_TIME
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.scheduleModelRolloutFreeze(request) }
      }

    val expected = MODEL_ROLLOUT.copy { rolloutFreezeTime = ROLLOUT_FREEZE_TIME }

    verifyProtoArgument(
        internalModelRolloutsMock,
        ModelRolloutsCoroutineImplBase::scheduleModelRolloutFreeze
      )
      .isEqualTo(
        internalScheduleModelRolloutFreezeRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
          rolloutFreezeTime = ROLLOUT_FREEZE_TIME
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.scheduleModelRolloutFreeze(
              scheduleModelRolloutFreezeRequest { rolloutFreezeTime = ROLLOUT_FREEZE_TIME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws UNAUTHENTICATED when no principal is found`() {
    val request = scheduleModelRolloutFreezeRequest {
      name = MODEL_ROLLOUT_NAME
      rolloutFreezeTime = ROLLOUT_FREEZE_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.scheduleModelRolloutFreeze(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws PERMISSION_DENIED when principal is data provider`() {
    val request = scheduleModelRolloutFreezeRequest {
      name = MODEL_ROLLOUT_NAME
      rolloutFreezeTime = ROLLOUT_FREEZE_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.scheduleModelRolloutFreeze(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws PERMISSION_DENIED when principal is duchy`() {
    val request = scheduleModelRolloutFreezeRequest {
      name = MODEL_ROLLOUT_NAME
      rolloutFreezeTime = ROLLOUT_FREEZE_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) {
          runBlocking { service.scheduleModelRolloutFreeze(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = scheduleModelRolloutFreezeRequest {
      name = MODEL_ROLLOUT_NAME
      rolloutFreezeTime = ROLLOUT_FREEZE_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.scheduleModelRolloutFreeze(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = scheduleModelRolloutFreezeRequest {
      name = MODEL_ROLLOUT_NAME
      rolloutFreezeTime = ROLLOUT_FREEZE_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.scheduleModelRolloutFreeze(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelRollout succeeds`() {
    val request = deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.deleteModelRollout(request) }
      }

    val expected = Empty.getDefaultInstance()

    verifyProtoArgument(
        internalModelRolloutsMock,
        ModelRolloutsCoroutineImplBase::deleteModelRollout
      )
      .isEqualTo(
        internalDeleteModelRolloutRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `deleteModelRollout throws UNAUTHENTICATED when no principal is found`() {
    val request = deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.deleteModelRollout(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteModelRollout throws PERMISSION_DENIED when principal is data provider`() {
    val request = deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelRollout throws PERMISSION_DENIED when principal is duchy`() {
    val request = deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.deleteModelRollout(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelRollout throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.deleteModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteModelRollout throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.deleteModelRollout(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelRollouts with parent succeeds for model provider caller`() {
    val request = listModelRolloutsRequest { parent = MODEL_LINE_NAME }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listModelRollouts(request) }
      }

    val expected = listModelRolloutsResponse {
      modelRollout += MODEL_ROLLOUT
      modelRollout += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_2 }
      modelRollout += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_3 }
    }

    val streamModelRolloutsRequest =
      captureFirst<StreamModelRolloutsRequest> {
        verify(internalModelRolloutsMock).streamModelRollouts(capture())
      }

    assertThat(streamModelRolloutsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelRolloutsRequest {
          limit = DEFAULT_LIMIT + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            rolloutPeriod = rolloutPeriod {
              rolloutPeriodStartTime = Timestamp.getDefaultInstance()
              rolloutPeriodEndTime = Timestamp.getDefaultInstance()
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelRollouts with parent succeeds for data provider caller`() {
    val request = listModelRolloutsRequest { parent = MODEL_LINE_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelRollouts(request) }
      }

    val expected = listModelRolloutsResponse {
      modelRollout += MODEL_ROLLOUT
      modelRollout += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_2 }
      modelRollout += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_3 }
    }

    val streamModelRolloutsRequest =
      captureFirst<StreamModelRolloutsRequest> {
        verify(internalModelRolloutsMock).streamModelRollouts(capture())
      }

    assertThat(streamModelRolloutsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelRolloutsRequest {
          limit = DEFAULT_LIMIT + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            rolloutPeriod = rolloutPeriod {
              rolloutPeriodStartTime = Timestamp.getDefaultInstance()
              rolloutPeriodEndTime = Timestamp.getDefaultInstance()
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelRollouts throws UNAUTHENTICATED when no principal is found`() {
    val request = listModelRolloutsRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listModelRollouts(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listModelRollouts throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = listModelRolloutsRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.listModelRollouts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelRollouts throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelRollouts(ListModelRolloutsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelRollouts throws PERMISSION_DENIED when principal is duchy`() {
    val request = listModelRolloutsRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.listModelRollouts(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelRollouts throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = listModelRolloutsRequest { parent = MODEL_LINE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listModelRollouts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelRollouts throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listModelRolloutsRequest {
      parent = MODEL_LINE_NAME
      pageSize = -1
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelRollouts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelRollouts throws invalid argument when parent doesn't match parent in page token`() {
    val request = listModelRolloutsRequest {
      parent = MODEL_LINE_NAME
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = 987L
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
        }
      }
      pageToken = listModelRolloutsPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelRollouts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelRollouts throws invalid argument when rolloutPeriodOverlapping doesn't match rolloutPeriodOverlapping in page token`() {
    val request = listModelRolloutsRequest {
      parent = MODEL_LINE_NAME
      filter = filter {
        rolloutPeriodOverlapping = timeInterval {
          startTime = ROLLOUT_PERIOD_START_TIME
          endTime = ROLLOUT_PERIOD_END_TIME
        }
      }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = timeInterval {
          startTime = ROLLOUT_PERIOD_START_TIME
          endTime = ROLLOUT_FREEZE_TIME
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
        }
      }
      pageToken = listModelRolloutsPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelRollouts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelRollouts with page token gets the next page`() {
    val request = listModelRolloutsRequest {
      parent = MODEL_LINE_NAME
      pageSize = 2
      filter = filter {
        rolloutPeriodOverlapping = timeInterval {
          startTime = ROLLOUT_PERIOD_START_TIME
          endTime = ROLLOUT_PERIOD_END_TIME
        }
      }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = timeInterval {
          startTime = ROLLOUT_PERIOD_START_TIME
          endTime = ROLLOUT_PERIOD_END_TIME
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
        }
      }
      pageToken = listModelRolloutsPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelRollouts(request) }
      }

    val expected = listModelRolloutsResponse {
      modelRollout += MODEL_ROLLOUT
      modelRollout += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_2 }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = request.pageSize
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = timeInterval {
          startTime = ROLLOUT_PERIOD_START_TIME
          endTime = ROLLOUT_PERIOD_END_TIME
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_2
        }
      }
      nextPageToken = listModelRolloutsPageToken.toByteArray().base64UrlEncode()
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelRolloutsRequest> {
        verify(internalModelRolloutsMock).streamModelRollouts(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelRolloutsRequest {
          limit = request.pageSize + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            rolloutPeriod = rolloutPeriod {
              rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
              rolloutPeriodEndTime = ROLLOUT_PERIOD_END_TIME
            }
            after = afterFilter {
              rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelRollouts with new page size replaces page size in page token`() {
    val request = listModelRolloutsRequest {
      parent = MODEL_LINE_NAME
      pageSize = 4
      filter = filter {
        rolloutPeriodOverlapping = timeInterval {
          startTime = ROLLOUT_PERIOD_START_TIME
          endTime = ROLLOUT_PERIOD_END_TIME
        }
      }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = timeInterval {
          startTime = ROLLOUT_PERIOD_START_TIME
          endTime = ROLLOUT_PERIOD_END_TIME
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
        }
      }
      pageToken = listModelRolloutsPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelRollouts(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelRolloutsRequest> {
        verify(internalModelRolloutsMock).streamModelRollouts(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelRolloutsRequest {
          limit = request.pageSize + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            rolloutPeriod = rolloutPeriod {
              rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
              rolloutPeriodEndTime = ROLLOUT_PERIOD_END_TIME
            }
            after = afterFilter {
              rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelRollouts with no page size uses page size in page token`() {
    val request = listModelRolloutsRequest {
      parent = MODEL_LINE_NAME
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
        }
      }
      pageToken = listModelRolloutsPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelRollouts(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelRolloutsRequest> {
        verify(internalModelRolloutsMock).streamModelRollouts(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelRolloutsRequest {
          limit = 3
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            externalModelLineId = EXTERNAL_MODEL_LINE_ID
            after = afterFilter {
              rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
            }
          }
        }
      )
  }
}
