// TODO(efoxepstein): add tests

package org.wfanet.measurement.scheduler

import kotlinx.coroutines.delay
import org.wfanet.measurement.common.AdaptiveThrottler
import java.time.Duration
import kotlin.random.Random

enum class TaskState { TASK_EXECUTED, TASK_NOT_FOUND }
typealias TaskExecutor = suspend () -> TaskState

class Scheduler(private val pollInterval: Duration,
                private val throttler: AdaptiveThrottler,
                private val executor: TaskExecutor) {
  suspend fun runLoop() {
    while (true) {
      while (!throttler.attempt()) {
        delay(Random.nextLong(0, pollInterval.toMillis()))
      }

      if (executor() == TaskState.TASK_NOT_FOUND) {
        throttler.reportThrottled()
      }
    }
  }
}
