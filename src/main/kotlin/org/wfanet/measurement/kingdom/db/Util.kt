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

package org.wfanet.measurement.kingdom.db

import java.time.Instant

internal fun <T, V> List<T>?.ifNotNullOrEmpty(block: (List<T>) -> V): V? =
  this?.ifEmpty { null }?.let(block)

internal fun <V> Instant?.ifNotNullOrEpoch(block: (Instant) -> V): V? =
  this?.let { if (it == Instant.EPOCH) null else block(it) }
