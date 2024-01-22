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

package org.wfanet.panelmatch.common.beam.testing

import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.junit.Rule
import org.junit.rules.TemporaryFolder

/**
 * Base class for Apache Beam tests.
 *
 * This makes a [TestPipeline] available to subclasses and sets it to run automatically in each test
 * case. It also provides a convenience function for building [PCollection]s.
 */
open class BeamTestBase {
  @get:Rule val beamTemporaryFolder = TemporaryFolder()

  private val options by lazy {
    beamTemporaryFolder.create()
    TestPipeline.testingPipelineOptions().apply {
      stableUniqueNames = PipelineOptions.CheckEnabled.OFF
      tempLocation = beamTemporaryFolder.root.absolutePath
    }
  }

  @get:Rule
  val pipeline: TestPipeline =
    TestPipeline.fromOptions(options)
      .enableAbandonedNodeEnforcement(false)
      .enableAutoRunIfMissing(true)

  inline fun <reified T> pcollectionOf(
    name: String,
    vararg values: T,
    coder: Coder<T>? = null,
  ): PCollection<T> {
    return pcollectionOf(name, values.asIterable(), coder)
  }

  inline fun <reified T> pcollectionOf(
    name: String,
    values: Iterable<T>,
    coder: Coder<T>? = null,
  ): PCollection<T> {
    if (coder != null) {
      return pipeline.apply(name, Create.of(values).withCoder(coder))
    }
    return pipeline.apply(name, Create.of(values))
  }

  inline fun <reified T : Any> pcollectionViewOf(
    name: String,
    value: T,
    coder: Coder<T>? = null,
  ): PCollectionView<T> {
    return pcollectionOf("$name:Create", value, coder = coder)
      .apply("$name:View", View.asSingleton())
  }
}

fun <T> assertThat(pCollection: PCollection<T>): PAssert.IterableAssert<T> {
  return PAssert.that(pCollection)
}

inline fun <reified T> assertThat(view: PCollectionView<T>): PAssert.IterableAssert<T> {
  val parDo =
    object : DoFn<Int, T>() {
      @ProcessElement
      fun processElement(context: ProcessContext, out: OutputReceiver<T>) {
        out.output(context.sideInput(view))
      }
    }

  val extractedResult =
    view.pipeline
      .apply("Create Dummy PCollection", Create.of(1))
      .apply("Replace with Side Input", ParDo.of(parDo).withSideInputs(view))

  return assertThat(extractedResult)
}
