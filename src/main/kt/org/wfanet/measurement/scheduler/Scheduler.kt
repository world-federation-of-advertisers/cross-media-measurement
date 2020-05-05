// TODO(efoxepstein): add tests

package org.wfanet.measurement.scheduler

import kotlinx.coroutines.delay
import org.wfanet.measurement.common.AdaptiveThrottler
import java.time.Duration
import kotlin.random.Random

enum class TaskState {
  // Indicates that a task was executed and there may be more tasks to perform.
  TASK_EXECUTED,

  // Indicates that a task was not found. This is a signal for throttling backoff,
  // because no tasks are waiting to be executed.
  TASK_NOT_FOUND,
}

typealias TaskExecutor = suspend () -> TaskState

class Scheduler(private val pollInterval: Duration,
                private val throttler: AdaptiveThrottler,
                private val executor: TaskExecutor) {
  suspend fun runLoop() {
    loop@ while (true) {
      while (!throttler.attempt()) {
        delay(Random.nextLong(0, pollInterval.toMillis()))
      }

      when (executor()) {
        TaskState.TASK_NOT_FOUND -> throttler.reportThrottled()
        TaskState.TASK_EXECUTED -> continue@loop
      }
    }
  }
}
