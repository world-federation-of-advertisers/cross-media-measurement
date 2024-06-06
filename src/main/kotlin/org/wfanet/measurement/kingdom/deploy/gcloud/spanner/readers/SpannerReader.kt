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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Statement

/** Abstraction for reading rows from Spanner and translating into more expressive objects. */
abstract class SpannerReader<T : Any> : BaseSpannerReader<T>() {
  protected abstract val baseSql: String

  override val builder: Statement.Builder by lazy { Statement.newBuilder(baseSql) }

  open fun fillStatementBuilder(block: Statement.Builder.() -> Unit): SpannerReader<T> {
    builder.block()
    return this
  }
}
