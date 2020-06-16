package org.wfanet.measurement.common.testing

import org.mockito.Mockito

// TODO: consider removing this file and using the mockito-kotlin 3rd party dep.

/**
 * Helper for using Mockito with Kotlin.
 *
 * Amazingly, this works. Without it, we get an [IllegalStateException] from [Mockito.any].
 */
fun <T> any(): T = Mockito.any<T>()

/** Helper for using Mockito with Kotlin. [Mockito.same] alone yields an [IllegalStateException]. */
fun <T> same(t: T): T = Mockito.same<T>(t)

/** Helper for using Mockito with Kotlin. [Mockito.eq] alone yields an [IllegalStateException]. */
fun <T> eq(t: T): T = Mockito.eq<T>(t)
