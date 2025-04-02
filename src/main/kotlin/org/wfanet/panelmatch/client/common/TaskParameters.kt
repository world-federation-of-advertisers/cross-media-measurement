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

package org.wfanet.panelmatch.client.common

import kotlin.reflect.KClass

/** Provides a map to access a task-specific context. */
class TaskParameters(parameters: Set<Any>) {
  private val underlyingMap: Map<KClass<*>, Any> =
    parameters.associateBy {
      require(it::class.isData) { "Task Parameters only store data classes" }
      it::class
    }

  fun <T : Any> get(key: KClass<T>): T? {
    require(key.isData) { "Task Parameters only store data classes" }
    @Suppress("UNCHECKED_CAST")
    return underlyingMap[key] as T?
  }
}
