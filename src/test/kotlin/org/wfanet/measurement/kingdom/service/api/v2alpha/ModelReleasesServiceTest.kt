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

import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import kotlinx.coroutines.flow.flowOf
import org.junit.Before
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelRelease as InternalModelRelease
import org.wfanet.measurement.internal.kingdom.modelRelease as internalModelRelease
import com.google.protobuf.Timestamp
import java.time.Instant
import org.junit.Rule
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.internal.kingdom.copy

private const val DEFAULT_LIMIT = 50


private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME_2 = "modelProviders/BBBBBBBBBHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME_2/modelSuites/AAAAAAAAAJs"
private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
private const val MODEL_RELEASE_NAME_2 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAJs"
private const val MODEL_RELEASE_NAME_3 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAKs"
private const val MODEL_RELEASE_NAME_4 = "$MODEL_SUITE_NAME_2/modelReleases/AAAAAAAAAHs"
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_RELEASE_ID =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME)!!.modelReleaseId)
private val EXTERNAL_MODEL_RELEASE_ID_2 =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME_2)!!.modelReleaseId)
private val EXTERNAL_MODEL_RELEASE_ID_3 =
  apiIdToExternalId(ModelReleaseKey.fromName(MODEL_RELEASE_NAME_3)!!.modelReleaseId)

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val INTERNAL_MODEL_RELEASE: InternalModelRelease = internalModelRelease {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID
  createTime = CREATE_TIME
}

private val MODEL_RELEASE: ModelRelease = modelRelease {
  name = MODEL_RELEASE_NAME
  createTime = CREATE_TIME
}

@RunWith(JUnit4::class)
class ModelReleasesServiceTest {

  private val internalModelLinesMock: ModelReleasesCoroutineImplBase =
    mockService() {
      onBlocking { createModelRelease(any()) }.thenReturn(INTERNAL_MODEL_RELEASE)
      onBlocking { getModelRelease(any()) }
        .thenReturn(INTERNAL_MODEL_RELEASE)
      onBlocking { streamModelReleases(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MODEL_RELEASE,
            INTERNAL_MODEL_RELEASE.copy {
              externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID_2
            },
            INTERNAL_MODEL_RELEASE.copy {
              externalModelReleaseId = EXTERNAL_MODEL_RELEASE_ID_3
            }
          )
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(internalModelLinesMock) }

  private lateinit var service: ModelReleasesService

  @Before
  fun initService() {
    service =
      ModelReleasesService(
        ModelReleasesCoroutineStub(grpcTestServerRule.channel),
      )
  }

}
