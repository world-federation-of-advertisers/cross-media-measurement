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

package org.wfanet.panelmatch.client.logger

import java.util.Collections.synchronizedList
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.CoroutineName

@PublishedApi internal val taskLogs = ConcurrentHashMap<String, MutableList<String>>()

suspend inline fun Logger.addToTaskLog(logMessage: String, level: Level = Level.INFO) {
  val coroutineContextName: String = requireNotNull(coroutineContext[CoroutineName.Key]).name
  val message = "[$coroutineContextName] $logMessage"
  val logs = taskLogs.getOrPut(coroutineContextName) { synchronizedList(mutableListOf()) }
  logs.add(message)
  log(level, message)
}

suspend fun getAndClearTaskLog(): List<String> {
  val taskKey = requireNotNull(coroutineContext[CoroutineName.Key]).name
  val listCopy: MutableList<String>? = taskLogs.remove(taskKey)
  return listCopy ?: emptyList()
}

/**
 * Clears *all* the test logs. Should only be used if you really want to clear all the logs. In
 * general, used only during testing.
 */
internal fun clearLogs() {
  taskLogs.clear()
}
