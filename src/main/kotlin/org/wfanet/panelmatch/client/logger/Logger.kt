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
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import org.wfanet.panelmatch.common.ExchangeDateKey

class TaskLog(val name: String) : CoroutineContext.Element {
  constructor(exchangeDateKey: ExchangeDateKey) : this(exchangeDateKey.path)

  override val key = Key
  val logs: MutableList<String> = synchronizedList(mutableListOf())

  object Key : CoroutineContext.Key<TaskLog>
}

suspend fun currentTaskLog(): TaskLog? = coroutineContext[TaskLog.Key]

suspend inline fun Logger.addToTaskLog(logMessage: String, level: Level = Level.INFO) {
  val taskLog = requireNotNull(currentTaskLog())
  val message = "[${taskLog.name}] $logMessage"
  taskLog.logs.add(message)
  log(level, message)
}

suspend inline fun Logger.addToTaskLog(throwable: Throwable, level: Level = Level.INFO) {
  val taskLog = requireNotNull(currentTaskLog())
  val message = "[${taskLog.name}] ${throwable.message}"
  taskLog.logs.add(message)
  log(level, taskLog.name, throwable)
}

suspend fun getAndClearTaskLog(): List<String> {
  val logs = requireNotNull(currentTaskLog()).logs
  return synchronized(logs) {
    val copy = logs.toMutableList() // Force a copy (toList() is not guaranteed to)
    logs.clear()
    copy
  }
}
