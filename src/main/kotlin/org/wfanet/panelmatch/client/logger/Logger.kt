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
import java.util.logging.Logger
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.CoroutineName

private val taskLogs = ConcurrentHashMap<String, MutableList<String>>()

fun <R : Any> R.loggerFor(): Lazy<Logger> {
  return lazy { Logger.getLogger(this.javaClass.name) }
}

suspend fun Logger.addToTaskLog(logMessage: String) {
  val coroutineContextName: String = requireNotNull(coroutineContext[CoroutineName.Key]).toString()
  taskLogs.computeIfAbsent(coroutineContextName) { synchronizedList(mutableListOf<String>()) }
  requireNotNull(taskLogs[coroutineContextName]).add("$coroutineContextName:$logMessage")
  info("$coroutineContextName:$logMessage")
}

suspend fun getAndClearTaskLog(): List<String> {
  val taskKey = requireNotNull(coroutineContext[CoroutineName.Key]).toString()
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
