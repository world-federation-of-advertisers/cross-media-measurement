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

package org.wfanet.panelmatch.common.beam

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat

@RunWith(JUnit4::class)
class BeamTest : BeamTestBase() {
  private val collection: PCollection<KV<Int, String>> by lazy {
    pcollectionOf("collection", kvOf(1, "A"), kvOf(2, "B"), kvOf(3, "C"))
  }

  private val anotherCollection by lazy {
    pcollectionOf("right-hand side", kvOf(1, 'a'), kvOf(1, 'b'), kvOf(4, 'c'))
  }

  private val yetAnotherCollection by lazy {
    pcollectionOf("right-hand side", kvOf(1, 'a'), kvOf(2, 'b'), kvOf(3, 'c'))
  }

  private val listOfCollections by lazy {
    listOf(pcollectionOf("first", 1, 2, 3), pcollectionOf("second", 4, 5, 6))
  }

  private val collectionWithDuplicates by lazy { pcollectionOf("duplicates", "a", "a", "a") }

  @Test
  fun keys() {
    assertThat(collection.keys()).containsInAnyOrder(1, 2, 3)
  }

  @Test
  fun values() {
    assertThat(collection.values()).containsInAnyOrder("A", "B", "C")
  }

  @Test
  fun parDo() {
    val result: PCollection<Int> =
      collection.parDo(
        object : DoFn<KV<Int, String>, Int>() {
          @ProcessElement
          fun processElement(context: DoFn<KV<Int, String>, Int>.ProcessContext) {
            context.output(context.element().key + 10)
            context.output(context.element().key + 100)
          }
        }
      )

    assertThat(result).containsInAnyOrder(11, 101, 12, 102, 13, 103)
  }

  @Test
  fun parDoSequence() {
    val result: PCollection<Int> =
      collection.parDo {
        yield(it.key + 10)
        yield(it.key + 100)
      }

    assertThat(result).containsInAnyOrder(11, 101, 12, 102, 13, 103)
  }

  @Test
  fun map() {
    assertThat(collection.map { it.key + 10 }).containsInAnyOrder(11, 12, 13)
  }

  @Test
  fun filter() {
    assertThat(collection.filter { it.key > 1 }).containsInAnyOrder(kvOf(2, "B"), kvOf(3, "C"))
  }

  @Test
  fun flatMap() {
    assertThat(collection.flatMap { listOf(it.key + 10, it.key + 100) })
      .containsInAnyOrder(11, 12, 13, 101, 102, 103)
  }

  @Test
  fun keyBy() {
    assertThat(pcollectionOf("unkeyed-items", 1, 2, 3).keyBy { it + 10 })
      .containsInAnyOrder(kvOf(11, 1), kvOf(12, 2), kvOf(13, 3))
  }

  @Test
  fun mapKeys() {
    assertThat(collection.mapKeys { -it })
      .containsInAnyOrder(kvOf(-1, "A"), kvOf(-2, "B"), kvOf(-3, "C"))
  }

  @Test
  fun mapValues() {
    assertThat(collection.mapValues { it.lowercase() })
      .containsInAnyOrder(kvOf(1, "a"), kvOf(2, "b"), kvOf(3, "c"))
  }

  @Test
  fun partition() {
    val parts: PCollectionList<KV<Int, String>> = collection.partition(2) { it.key % 2 }
    assertThat(parts.size()).isEqualTo(2)
    assertThat(parts[0]).containsInAnyOrder(kvOf(2, "B"))
    assertThat(parts[1]).containsInAnyOrder(kvOf(1, "A"), kvOf(3, "C"))
  }

  @Test
  fun join() {
    val result: PCollection<KV<Int, String>> =
      collection.join(anotherCollection) { key, lefts, rights ->
        val leftString = lefts.sorted().joinToString(", ")
        val rightString = rights.sorted().joinToString(", ")
        yield(kvOf(key, "[$leftString] and [$rightString]"))
      }
    assertThat(result)
      .containsInAnyOrder(
        kvOf(1, "[A] and [a, b]"),
        kvOf(2, "[B] and []"),
        kvOf(3, "[C] and []"),
        kvOf(4, "[] and [c]"),
      )
  }

  @Test
  fun strictOneToOneJoin() {
    val result: PCollection<KV<String, Char>> = collection.strictOneToOneJoin(yetAnotherCollection)
    assertThat(result).containsInAnyOrder(kvOf("A", 'a'), kvOf("B", 'b'), kvOf("C", 'c'))
  }

  @Test
  fun joinIgnoringArguments() {
    val result: PCollection<Int> = collection.join(anotherCollection) { _, _, _ -> yield(1) }
    assertThat(result).containsInAnyOrder(1, 1, 1, 1)
  }

  @Test
  fun oneToOneJoinFailsWithManyOnOneSide() {
    collection.oneToOneJoin(anotherCollection)
    assertFails { pipeline.run() }
  }

  @Test
  fun oneToOneJoin() {
    collection.oneToOneJoin(yetAnotherCollection)
    val left = pcollectionOf("Left", kvOf(1, "only-on-left"), kvOf(2, "in-both:left"))
    val right = pcollectionOf("Right", kvOf(3, "only-on-right"), kvOf(2, "in-both:right"))
    val result = left.oneToOneJoin(right)
    assertThat(result)
      .containsInAnyOrder(
        kvOf("only-on-left", null),
        kvOf(null, "only-on-right"),
        kvOf("in-both:left", "in-both:right"),
      )
  }

  @Test
  fun count() {
    assertThat(collection.count()).containsInAnyOrder(3L)
  }

  @Test
  fun toPCollectionList() {
    val list = listOfCollections.toPCollectionList()
    assertThat(list.size()).isEqualTo(2)
    assertThat(list[0]).containsInAnyOrder(1, 2, 3)
    assertThat(list[1]).containsInAnyOrder(4, 5, 6)
  }

  @Test
  fun flatten() {
    val list = listOfCollections.toPCollectionList()
    assertThat(list.flatten()).containsInAnyOrder(1, 2, 3, 4, 5, 6)
  }

  @Test
  fun groupByKey() {
    assertThat(anotherCollection.groupByKey()).satisfies { elements ->
      assertThat(elements.map { kvOf(it.key, it.value.toList().sorted()) })
        .containsExactly(kvOf(1, listOf('a', 'b')), kvOf(4, listOf('c')))
      null
    }
  }

  @Test
  fun parDoWithSideInput() {
    val sideInput = collection.count()
    val result: PCollection<KV<Int, Long>> =
      collection.parDoWithSideInput(sideInput) { element, count -> yield(kvOf(element.key, count)) }
    assertThat(result).containsInAnyOrder(kvOf(1, 3L), kvOf(2, 3L), kvOf(3, 3L))
  }

  @Test
  fun breakFusionPreservesElements() {
    assertThat(collection.breakFusion())
      .containsInAnyOrder(kvOf(1, "A"), kvOf(2, "B"), kvOf(3, "C"))
  }

  @Test
  fun breakFusionPreservesElementMultiplicity() {
    assertThat(collectionWithDuplicates.breakFusion()).containsInAnyOrder("a", "a", "a")
  }

  @Test
  fun createSequence() {
    val sequence = pipeline.createSequence(10, 3)
    assertThat(sequence).containsInAnyOrder((0..9).toList())
    pipeline.run()
  }

  @Test
  fun minus() {
    val mainCollection = pcollectionOf("collection", "A", "B", "C")
    val extraCollection = pcollectionOf("extraCollection", "B", "C", "D", "E")
    assertThat(extraCollection.minus(mainCollection)).containsInAnyOrder("D", "E")
  }
}
