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

import io.grpc.Status
import kotlinx.coroutines.flow.flowOf
import org.junit.Before
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.internal.kingdom.ModelRollout as InternalModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.modelRollout as internalModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import com.google.protobuf.Timestamp
import java.time.Instant
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelRollout
import org.wfanet.measurement.api.v2alpha.ModelRolloutKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.copy

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAHs"
private const val MODEL_ROLLOUT_NAME = "$MODEL_LINE_NAME/modelRollouts/AAAAAAAAAHs"
private const val MODEL_ROLLOUT_NAME_2 = "$MODEL_LINE_NAME/modelRollouts/AAAAAAAAAJs"
private const val MODEL_ROLLOUT_NAME_3 = "$MODEL_LINE_NAME/modelRollouts/AAAAAAAAAKs"
private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
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
@RunWith(JUnit4::class)
class ModelRolloutsServiceTest {

  private val internalModelRolloutsMock: ModelRolloutsCoroutineImplBase =
    mockService() {
      onBlocking { createModelRollout(any()) }
        .thenAnswer {
          val request = it.getArgument<InternalModelRollout>(0)
          if (request.externalModelLineId != EXTERNAL_MODEL_LINE_ID) {
            failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
          } else {
            INTERNAL_MODEL_ROLLOUT
          }
        }
      onBlocking { scheduleModelRolloutFreeze(any()) }
        .thenReturn(INTERNAL_MODEL_ROLLOUT.copy { rolloutFreezeTime = ROLLOUT_FREEZE_TIME })
      onBlocking { deleteModelRollout(any()) }
        .thenReturn(INTERNAL_MODEL_ROLLOUT)
      onBlocking { streamModelRollouts(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MODEL_ROLLOUT,
            INTERNAL_MODEL_ROLLOUT.copy {
              externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_2
            },
            INTERNAL_MODEL_ROLLOUT.copy {
              externalModelRolloutId = EXTERNAL_MODEL_ROLLOUT_ID_3
            }
          )
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(internalModelRolloutsMock) }

  private lateinit var service: ModelRolloutsService

  @Before
  fun initService() {
    service =
      ModelRolloutsService(
        ModelRolloutsCoroutineStub(grpcTestServerRule.channel),
      )
  }

}
