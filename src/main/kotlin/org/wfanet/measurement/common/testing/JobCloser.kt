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
