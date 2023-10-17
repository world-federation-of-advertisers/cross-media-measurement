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

package org.wfanet.measurement.common.grpc

import io.grpc.Context

/**
 * Immediately calls [action] with [context] as the [current][Context.current] [Context].
 *
 * This is a coroutine-friendly version of [Context.call].
 *
 * TODO(@SanjayVas): Move this to common-jvm.
 */
inline fun <R> withContext(context: Context, action: () -> R): R {
  val previous: Context = context.attach()
  return try {
    action()
  } finally {
    context.detach(previous)
  }
}
