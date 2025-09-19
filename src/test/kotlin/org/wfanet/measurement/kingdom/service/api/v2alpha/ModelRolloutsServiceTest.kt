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
import com.google.type.date
import com.google.type.interval
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
import org.mockito.kotlin.stub
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
import org.wfanet.measurement.api.v2alpha.dateInterval
import org.wfanet.measurement.api.v2alpha.deleteModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsPageToken
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.scheduleModelRolloutFreezeRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelRollout as InternalModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequest
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.rolloutPeriod
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteModelRolloutRequest as internalDeleteModelRolloutRequest
import org.wfanet.measurement.internal.kingdom.modelRollout as internalModelRollout
import org.wfanet.measurement.internal.kingdom.scheduleModelRolloutFreezeRequest as internalScheduleModelRolloutFreezeRequest
import org.wfanet.measurement.internal.kingdom.streamModelRolloutsRequest as internalStreamModelRolloutsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutOlderThanPreviousException

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
private val ROLLOUT_PERIOD_START_TIME: Instant = Instant.ofEpochSecond(86400)
private val ROLLOUT_PERIOD_START_DATE = date {
  year = 1970
  month = 1
  day = 2
}
private val ROLLOUT_PERIOD_END_TIME: Instant = Instant.ofEpochSecond(172800)
private val ROLLOUT_PERIOD_END_DATE = date {
  year = 1970
  month = 1
  day = 3
}
private val ROLLOUT_FREEZE_TIME: Instant = Instant.ofEpochSecond(86400)
private val ROLLOUT_FREEZE_DATE = date {
  year = 1970
  month = 1
  day = 2
}

private val INTERNAL_MODEL_ROLLOUT: InternalModelRollout = internalModelRollout {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelLineId = EXTERNAL_MODEL_LINE_ID
  externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_2
  rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
  rolloutPeriodEndTime = ROLLOUT_PERIOD_END_TIME.toProtoTime()
  rolloutFreezeTime = ROLLOUT_FREEZE_TIME.toProtoTime()
  externalPreviousModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
  externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

private val MODEL_ROLLOUT: ModelRollout = modelRollout {
  name = MODEL_ROLLOUT_NAME_2
  gradualRolloutPeriod = dateInterval {
    startDate = ROLLOUT_PERIOD_START_DATE
    endDate = ROLLOUT_PERIOD_END_DATE
  }
  rolloutFreezeDate = ROLLOUT_FREEZE_DATE
  previousModelRollout = MODEL_ROLLOUT_NAME
  modelRelease = MODEL_RELEASE_NAME
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

private val MODEL_ROLLOUT_2: ModelRollout = modelRollout {
  name = MODEL_ROLLOUT_NAME_2
  instantRolloutDate = ROLLOUT_PERIOD_START_DATE
  rolloutFreezeDate = ROLLOUT_FREEZE_DATE
  previousModelRollout = MODEL_ROLLOUT_NAME
  modelRelease = MODEL_RELEASE_NAME_2
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

@RunWith(JUnit4::class)
class ModelRolloutsServiceTest {

  private val internalModelRolloutsMock: ModelRolloutsCoroutineImplBase = mockService {
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
      .thenReturn(
        INTERNAL_MODEL_ROLLOUT.copy { rolloutFreezeTime = ROLLOUT_FREEZE_TIME.toProtoTime() }
      )
    onBlocking { deleteModelRollout(any()) }.thenReturn(INTERNAL_MODEL_ROLLOUT)
    onBlocking { streamModelRollouts(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_MODEL_ROLLOUT,
          INTERNAL_MODEL_ROLLOUT.copy { externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_2 },
          INTERNAL_MODEL_ROLLOUT.copy { externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_3 },
        )
      )
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelRolloutsMock) }

  private lateinit var service: ModelRolloutsService

  @Before
  fun initService() {
    service = ModelRolloutsService(ModelRolloutsCoroutineStub(grpcTestServerRule.channel))
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
        ModelRolloutsCoroutineImplBase::createModelRollout,
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
      rolloutFreezeDate = ROLLOUT_FREEZE_DATE
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.scheduleModelRolloutFreeze(request) }
      }

    val expected = MODEL_ROLLOUT.copy { rolloutFreezeDate = ROLLOUT_FREEZE_DATE }

    verifyProtoArgument(
        internalModelRolloutsMock,
        ModelRolloutsCoroutineImplBase::scheduleModelRolloutFreeze,
      )
      .isEqualTo(
        internalScheduleModelRolloutFreezeRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID
          rolloutFreezeTime = ROLLOUT_FREEZE_TIME.toProtoTime()
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
              scheduleModelRolloutFreezeRequest { rolloutFreezeDate = ROLLOUT_FREEZE_DATE }
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
      rolloutFreezeDate = ROLLOUT_FREEZE_DATE
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
      rolloutFreezeDate = ROLLOUT_FREEZE_DATE
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
      rolloutFreezeDate = ROLLOUT_FREEZE_DATE
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
      rolloutFreezeDate = ROLLOUT_FREEZE_DATE
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
      rolloutFreezeDate = ROLLOUT_FREEZE_DATE
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
        ModelRolloutsCoroutineImplBase::deleteModelRollout,
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
      modelRollouts += MODEL_ROLLOUT
      modelRollouts += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_2 }
      modelRollouts += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_3 }
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
          filter =
            StreamModelRolloutsRequestKt.filter {
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
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
      modelRollouts += MODEL_ROLLOUT
      modelRollouts += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_2 }
      modelRollouts += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_3 }
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
          filter =
            StreamModelRolloutsRequestKt.filter {
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelRollouts succeeds when filtering by ModelRelease across ModelLines`() {
    val request = listModelRolloutsRequest {
      parent = "$MODEL_SUITE_NAME/modelLines/-"
      filter = filter { modelReleaseIn += MODEL_RELEASE_NAME }
    }

    runBlocking {
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) { service.listModelRollouts(request) }
    }

    val internalRequest: StreamModelRolloutsRequest =
      verifyAndCapture(
        internalModelRolloutsMock,
        ModelRolloutsCoroutineImplBase::streamModelRollouts,
      )
    assertThat(internalRequest)
      .isEqualTo(
        internalStreamModelRolloutsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamModelRolloutsRequestKt.filter {
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              // ModelLine not specified in order to list across them.

              externalModelReleaseIdIn += EXTERNAL_MODEL_RELEASE_ID
            }
        }
      )
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
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
        rolloutPeriodOverlapping = dateInterval {
          startDate = ROLLOUT_PERIOD_START_DATE
          endDate = ROLLOUT_PERIOD_END_DATE
        }
      }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = interval {
          startTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
          endTime = ROLLOUT_FREEZE_TIME.toProtoTime()
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
  fun `listModelRollouts throws invalid argument when rolloutPeriodOverlapping is missing from request and set in page token`() {
    val request = listModelRolloutsRequest {
      parent = MODEL_LINE_NAME

      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = interval {
          startTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
          endTime = ROLLOUT_PERIOD_END_TIME.toProtoTime()
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
        rolloutPeriodOverlapping = dateInterval {
          startDate = ROLLOUT_PERIOD_START_DATE
          endDate = ROLLOUT_PERIOD_END_DATE
        }
      }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = interval {
          startTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
          endTime = ROLLOUT_PERIOD_END_TIME.toProtoTime()
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
      modelRollouts += MODEL_ROLLOUT
      modelRollouts += MODEL_ROLLOUT.copy { name = MODEL_ROLLOUT_NAME_2 }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = request.pageSize
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = interval {
          startTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
          endTime = ROLLOUT_PERIOD_END_TIME.toProtoTime()
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
          filter =
            StreamModelRolloutsRequestKt.filter {
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              rolloutPeriod = rolloutPeriod {
                rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
                rolloutPeriodEndTime = ROLLOUT_PERIOD_END_TIME.toProtoTime()
              }
              after = afterFilter {
                rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
        rolloutPeriodOverlapping = dateInterval {
          startDate = ROLLOUT_PERIOD_START_DATE
          endDate = ROLLOUT_PERIOD_END_DATE
        }
      }
      val listModelRolloutsPageToken = listModelRolloutsPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        externalModelLineId = EXTERNAL_MODEL_LINE_ID
        rolloutPeriodOverlapping = interval {
          startTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
          endTime = ROLLOUT_PERIOD_END_TIME.toProtoTime()
        }
        lastModelRollout = previousPageEnd {
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
          filter =
            StreamModelRolloutsRequestKt.filter {
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              rolloutPeriod = rolloutPeriod {
                rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
                rolloutPeriodEndTime = ROLLOUT_PERIOD_END_TIME.toProtoTime()
              }
              after = afterFilter {
                rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
          rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
          filter =
            StreamModelRolloutsRequestKt.filter {
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
              after = afterFilter {
                rolloutPeriodStartTime = ROLLOUT_PERIOD_START_TIME.toProtoTime()
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
  fun `createModelRollout throws INVALID_ARGUMENT with model rollout name when model rollout argument invalid`() {
    internalModelRolloutsMock.stub {
      onBlocking { createModelRollout(any()) }
        .thenThrow(
          ModelRolloutOlderThanPreviousException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_ROLLOUT_ID),
            )
            .asStatusRuntimeException(
              Status.Code.INVALID_ARGUMENT,
              "ModelRollout invalid arguments.",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelRollout(
              createModelRolloutRequest {
                parent = MODEL_LINE_NAME
                modelRollout = MODEL_ROLLOUT
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRollout", MODEL_ROLLOUT_NAME)
  }

  @Test
  fun `createModelRollout throws NOT_FOUND with model line name when model line not found`() {
    internalModelRolloutsMock.stub {
      onBlocking { createModelRollout(any()) }
        .thenThrow(
          ModelLineNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelLine not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelRollout(
              createModelRolloutRequest {
                parent = MODEL_LINE_NAME
                modelRollout = MODEL_ROLLOUT
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLine", MODEL_LINE_NAME)
  }

  @Test
  fun `createModelRollout throws NOT_FOUND with model release name when model release not found`() {
    internalModelRolloutsMock.stub {
      onBlocking { createModelRollout(any()) }
        .thenThrow(
          ModelReleaseNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_RELEASE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelRelease not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelRollout(
              createModelRolloutRequest {
                parent = MODEL_LINE_NAME
                modelRollout = MODEL_ROLLOUT
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRelease", MODEL_RELEASE_NAME)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws INVALID_ARGUMENT with model rollout name when model rollout argument invalid`() {
    internalModelRolloutsMock.stub {
      onBlocking { scheduleModelRolloutFreeze(any()) }
        .thenThrow(
          ModelRolloutOlderThanPreviousException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_ROLLOUT_ID),
            )
            .asStatusRuntimeException(
              Status.Code.INVALID_ARGUMENT,
              "ModelRollout invalid arguments.",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.scheduleModelRolloutFreeze(
              scheduleModelRolloutFreezeRequest {
                name = MODEL_ROLLOUT_NAME
                rolloutFreezeDate = ROLLOUT_FREEZE_DATE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRollout", MODEL_ROLLOUT_NAME)
  }

  @Test
  fun `scheduleModelRolloutFreeze throws NOT_FOUND with model rollout name when model rollout not found`() {
    internalModelRolloutsMock.stub {
      onBlocking { scheduleModelRolloutFreeze(any()) }
        .thenThrow(
          ModelRolloutNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_ROLLOUT_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelRollout not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.scheduleModelRolloutFreeze(
              scheduleModelRolloutFreezeRequest {
                name = MODEL_ROLLOUT_NAME
                rolloutFreezeDate = ROLLOUT_FREEZE_DATE
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRollout", MODEL_ROLLOUT_NAME)
  }

  @Test
  fun `deleteModelRollout throws NOT_FOUND with model rollout name when model rollout not found`() {
    internalModelRolloutsMock.stub {
      onBlocking { deleteModelRollout(any()) }
        .thenThrow(
          ModelRolloutNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_ROLLOUT_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelRollout not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.deleteModelRollout(deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRollout", MODEL_ROLLOUT_NAME)
  }

  @Test
  fun `deleteModelRollout throws FAILED_PRECONDITION with model rollout name when model rollout invalid args`() {
    internalModelRolloutsMock.stub {
      onBlocking { deleteModelRollout(any()) }
        .thenThrow(
          ModelRolloutOlderThanPreviousException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              ExternalId(EXTERNAL_MODEL_ROLLOUT_ID),
            )
            .asStatusRuntimeException(
              Status.Code.FAILED_PRECONDITION,
              "ModelRollout invalid arguments.",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.deleteModelRollout(deleteModelRolloutRequest { name = MODEL_ROLLOUT_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelRollout", MODEL_ROLLOUT_NAME)
  }
}
