// Copyright 2020 The Measurement System Authors
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
