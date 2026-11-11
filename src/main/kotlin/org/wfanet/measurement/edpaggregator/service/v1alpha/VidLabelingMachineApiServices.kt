/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import io.grpc.Channel
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineAuthorizer
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.GrpcHealingOperationStore
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub

/** Authenticated machine adapters using the system server's existing internal channel. */
object VidLabelingMachineApiServices {
  fun build(
    internalChannel: Channel,
    authorizer: VidLabelingMachineAuthorizer,
    rpcThrottlers: VidLabelingRpcThrottlers,
    coroutineContext: CoroutineContext = EmptyCoroutineContext,
  ): List<ServerServiceDefinition> {
    val candidates = RawImpressionUploadCorrectionCandidateServiceCoroutineStub(internalChannel)
    val operations = UploadHealingOperationServiceCoroutineStub(internalChannel)
    val operationStore =
      GrpcHealingOperationStore(
        UploadHealingOperationService(operations, coroutineContext),
        operations,
        rpcThrottlers,
      )
    return listOf(
        VidLabelingCorrectionDetectionService(
            candidates,
            authorizer,
            rpcThrottlers,
            coroutineContext,
          )
          .bindService(),
        VidLabelingHealingControllerService(
            operationStore,
            candidates,
            authorizer,
            rpcThrottlers,
            coroutineContext,
          )
          .bindService(),
      )
      .map { it.withInterceptor(VidLabelingMachineAuthorizer.Interceptor) }
  }

  internal suspend fun <T> authorized(
    authorizer: VidLabelingMachineAuthorizer,
    role: VidLabelingMachineAuthorizer.Role,
    block: suspend () -> T,
  ): T {
    authorizer.requireRole(role)
    return try {
      block()
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription(e.message ?: "Invalid machine request")
        .asRuntimeException()
    } catch (e: StatusException) {
      throw e.status.asRuntimeException(e.trailers)
    }
  }
}
