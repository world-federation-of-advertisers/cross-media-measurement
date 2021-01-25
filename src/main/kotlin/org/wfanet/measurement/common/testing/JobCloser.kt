// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.testing

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 * Makes an AutoCloseable that cancels and joins a [Job].
 */
fun CoroutineScope.launchAsAutoCloseable(job: Job): AutoCloseable {
  return AutoCloseable { runBlocking { job.cancelAndJoin() } }
}

/**
 * Makes AutoCloseable that launches block in a new coroutine and then cancels and joins it.
 */
fun CoroutineScope.launchAsAutoCloseable(block: suspend () -> Unit): AutoCloseable {
  return launchAsAutoCloseable(launch { block() })
}
