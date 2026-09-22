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

package org.wfanet.measurement.common.telemetry

import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.common.AttributeKey

/** Stable OpenTelemetry attributes shared by correlated XMM workflows. */
object XmmTraceAttributes {
  const val WORK_ITEM_NAME_STRING = "xmm.work_item.name"
  const val WORK_ITEM_ATTEMPT_NAME_STRING = "xmm.work_item_attempt.name"
  const val WORK_ITEM_GENERATION_STRING = "xmm.work_item.generation"
  const val LIFECYCLE_STAGE_STRING = "xmm.lifecycle.stage"
  const val OUTCOME_STRING = "xmm.outcome"
  const val ERROR_TYPE_STRING = "xmm.error.type"
  const val ERROR_CODE_STRING = "xmm.error.code"

  val WORK_ITEM_NAME: AttributeKey<String> = AttributeKey.stringKey(WORK_ITEM_NAME_STRING)
  val WORK_ITEM_ATTEMPT_NAME: AttributeKey<String> =
    AttributeKey.stringKey(WORK_ITEM_ATTEMPT_NAME_STRING)
  val WORK_ITEM_GENERATION: AttributeKey<Long> = AttributeKey.longKey(WORK_ITEM_GENERATION_STRING)
  val LIFECYCLE_STAGE: AttributeKey<String> = AttributeKey.stringKey(LIFECYCLE_STAGE_STRING)
  val OUTCOME: AttributeKey<String> = AttributeKey.stringKey(OUTCOME_STRING)
  val ERROR_TYPE: AttributeKey<String> = AttributeKey.stringKey(ERROR_TYPE_STRING)
  val ERROR_CODE: AttributeKey<String> = AttributeKey.stringKey(ERROR_CODE_STRING)

  /** Returns a bounded, human-readable exception class name suitable for a span attribute. */
  fun errorType(error: Throwable): String {
    return error::class.java.name.substringAfterLast('.').replace('$', '.').take(200)
  }

  /** Returns a stable gRPC status code from [error] or one of its wrapped causes. */
  fun errorCode(error: Throwable): String? {
    return generateSequence(error) { it.cause }
      .take(20)
      .mapNotNull {
        when (it) {
          is StatusException -> it.status.code.name
          is StatusRuntimeException -> it.status.code.name
          else -> null
        }
      }
      .firstOrNull()
      ?.let { "grpc.$it" }
  }
}
