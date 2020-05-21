package org.wfanet.measurement.common.testing

import org.mockito.Mockito

/**
 * Helper for using Mockito with Kotlin.
 *
 * Amazingly, this works. Without it, we get a [java.lang.IllegalStateException] from [Mockito.any].
 */
fun <T> any(): T = Mockito.any<T>()
