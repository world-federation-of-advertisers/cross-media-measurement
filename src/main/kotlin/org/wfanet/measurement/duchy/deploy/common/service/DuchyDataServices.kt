// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.service

import io.grpc.BindableService
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.full.declaredMemberProperties
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase

data class DuchyDataServices(
  val computationsService: ComputationsCoroutineImplBase,
  val computationStatsService: ComputationStatsCoroutineImplBase,
  val continuationTokensService: ContinuationTokensCoroutineImplBase,
)

fun DuchyDataServices.toList(): List<BindableService> {
  return DuchyDataServices::class.declaredMemberProperties.map { it.get(this) as BindableService }
}

interface DuchyDataServicesFactory {
  fun create(coroutineContext: CoroutineContext): DuchyDataServices
}
