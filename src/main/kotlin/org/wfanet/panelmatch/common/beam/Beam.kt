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

import kotlin.math.ceil
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.coders.NullableCoder
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.Keys
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.Partition
import org.apache.beam.sdk.transforms.Values
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.common.singleOrNullIfEmpty

/** Kotlin convenience helper for making [KV]s. */
fun <KeyT, ValueT> kvOf(key: KeyT, value: ValueT): KV<KeyT, ValueT> {
  return KV.of(key, value)
}

/** Returns the keys of a [PCollection] of [KV]s. */
fun <KeyT, ValueT> PCollection<KV<KeyT, ValueT>>.keys(name: String = "Keys"): PCollection<KeyT> {
  return apply(name, Keys.create())
}

/** Returns the values of a [PCollection] of [KV]s. */
fun <KeyT, ValueT> PCollection<KV<KeyT, ValueT>>.values(
  name: String = "Values"
): PCollection<ValueT> {
  return apply(name, Values.create())
}

/** Kotlin convenience helper for [ParDo]. */
fun <InT, OutT> PCollection<InT>.parDo(
  doFn: DoFn<InT, OutT>,
  name: String = "ParDo",
): PCollection<OutT> {
  return apply(name, ParDo.of(doFn))
}

/** Kotlin convenience helper for [ParDo]. */
inline fun <InT, OutT> PCollection<InT>.parDo(
  name: String = "ParDo",
  crossinline processElement: suspend SequenceScope<OutT>.(InT) -> Unit,
): PCollection<OutT> {
  return parDo(
    object : DoFn<InT, OutT>() {
      @ProcessElement
      fun processElement(@Element element: InT, output: OutputReceiver<OutT>) {
        sequence<OutT> { processElement(element) }.forEach(output::output)
      }
    },
    name,
  )
}

/** Convenience helper for filtering a [PCollection] */
inline fun <reified T> PCollection<T>.filter(
  name: String = "Filter",
  crossinline predicate: (T) -> Boolean,
): PCollection<T> {
  return parDo(name) { if (predicate(it)) yield(it) }
}

/** Kotlin convenience helper for a [ParDo] that has a single output per input. */
inline fun <InT, reified OutT> PCollection<InT>.map(
  name: String = "Map",
  crossinline processElement: (InT) -> OutT,
): PCollection<OutT> {
  return parDo(name) { yield(processElement(it)) }
}

/** Kotlin convenience helper for a [ParDo] that yields an [Iterable] of outputs. */
inline fun <InT, reified OutT> PCollection<InT>.flatMap(
  name: String = "FlatMap",
  crossinline processElement: (InT) -> Iterable<OutT>,
): PCollection<OutT> {
  return parDo(name) { yieldAll(processElement(it)) }
}

/** Kotlin convenience helper for keying a [PCollection] by some function of the inputs. */
inline fun <InputT, reified KeyT> PCollection<InputT>.keyBy(
  name: String = "KeyBy",
  crossinline keySelector: (InputT) -> KeyT,
): PCollection<KV<KeyT, InputT>> {
  return map(name) { kvOf(keySelector(it), it) }
}

/** Kotlin convenience helper for transforming only the keys of a [PCollection] of [KV]s. */
inline fun <InKeyT, reified OutKeyT, reified ValueT> PCollection<KV<InKeyT, ValueT>>.mapKeys(
  name: String = "MapKeys",
  crossinline processKey: (InKeyT) -> OutKeyT,
): PCollection<KV<OutKeyT, ValueT>> {
  return map(name) { kvOf(processKey(it.key), it.value) }
}

/** Kotlin convenience helper for transforming only the keys of a [PCollection] of [KV]s. */
inline fun <KeyT, reified InValueT, reified OutValueT> PCollection<KV<KeyT, InValueT>>.mapValues(
  name: String = "MapValues",
  crossinline processValue: (InValueT) -> OutValueT,
): PCollection<KV<KeyT, OutValueT>> {
  return map(name) { kvOf(it.key, processValue(it.value)) }
}

/** Kotlin convenience helper for partitioning a [PCollection]. */
fun <T> PCollection<T>.partition(
  numParts: Int,
  name: String = "Partition",
  partitionBy: (T) -> Int,
): PCollectionList<T> {
  return apply(name, Partition.of(numParts) { value: T, _ -> partitionBy(value) })
}

/** Kotlin convenience helper for a join between two [PCollection]s. */
inline fun <KeyT, reified LeftT, reified RightT, OutT> PCollection<KV<KeyT, LeftT>>.join(
  right: PCollection<KV<KeyT, RightT>>,
  name: String = "Join",
  crossinline transform:
    suspend SequenceScope<OutT>.(KeyT, Iterable<LeftT>, Iterable<RightT>) -> Unit,
): PCollection<OutT> {
  val leftTag = object : TupleTag<LeftT>() {}
  val rightTag = object : TupleTag<RightT>() {}
  return KeyedPCollectionTuple.of(leftTag, this)
    .and(rightTag, right)
    .apply("$name/CoGroupByKey", CoGroupByKey.create())
    .parDo("$name/Transform") {
      transform(it.key, it.value.getAll(leftTag), it.value.getAll(rightTag))
    }
}

/**
 * Kotlin convenience helper for a join between two [PCollection]s where each joined list should
 * only contain a single element.
 */
inline fun <reified KeyT, reified LeftT, reified RightT> PCollection<KV<KeyT, LeftT>>
  .strictOneToOneJoin(
  right: PCollection<KV<KeyT, RightT>>,
  name: String = "Strict Join",
): PCollection<KV<LeftT, RightT>> {
  return oneToOneJoin(right, name = "$name/Join").map("$name/RequireNotNull") {
    kvOf(
      requireNotNull(it.key) { "Key is missing in strictOneToOneJoin '$name'" },
      requireNotNull(it.value) { "Value is missing in strictOneToOneJoin '$name'" },
    )
  }
}

/**
 * Kotlin convenience helper for a join between two [PCollection]s where for each key, each
 * PCollection contains at most one item.
 */
inline fun <reified KeyT, reified LeftT, reified RightT> PCollection<KV<KeyT, LeftT>>.oneToOneJoin(
  right: PCollection<KV<KeyT, RightT>>,
  name: String = "One-to-One Join",
): PCollection<KV<LeftT?, RightT?>> {
  return join<KeyT, LeftT, RightT, KV<LeftT?, RightT?>>(right, name) { _, lefts, rights ->
      yield(kvOf(lefts.singleOrNullIfEmpty(), rights.singleOrNullIfEmpty()))
    }
    .setCoder(
      KvCoder.of(
        NullableCoder.of((coder as KvCoder<KeyT, LeftT>).valueCoder),
        NullableCoder.of((right.coder as KvCoder<KeyT, RightT>).valueCoder),
      )
    )
}

/** Kotlin convenience helper for getting the size of a [PCollection]. */
fun <T> PCollection<T>.count(name: String = "Count"): PCollectionView<Long> {
  return apply(name, Combine.globally<T, Long>(Count.combineFn()).asSingletonView())
}

/** Kotlin convenience helper for building a [PCollectionList]. */
fun <T> Iterable<PCollection<T>>.toPCollectionList(): PCollectionList<T> {
  return PCollectionList.of(this)
}

/** Kotlin convenience helper for flattening [PCollection]s. */
fun <T> PCollectionList<T>.flatten(name: String = "Flatten"): PCollection<T> {
  return apply(name, Flatten.pCollections())
}

/** Kotlin convenience helper for [parDo] but with a single side input. */
inline fun <InT, reified SideT, reified OutT> PCollection<InT>.parDoWithSideInput(
  sideInput: PCollectionView<SideT>,
  name: String = "ParDoWithSideInput",
  crossinline processElement: suspend SequenceScope<OutT>.(InT, SideT) -> Unit,
): PCollection<OutT> {
  val doFn =
    object : DoFn<InT, OutT>() {
      @ProcessElement
      fun processElement(
        @Element element: InT,
        out: OutputReceiver<OutT>,
        context: ProcessContext,
      ) {
        sequence<OutT> { processElement(element, context.sideInput(sideInput)) }
          .forEach(out::output)
      }
    }
  return apply(name, ParDo.of(doFn).withSideInputs(sideInput))
}

inline fun <InT, reified SideT, reified OutT> PCollection<InT>.mapWithSideInput(
  sideInput: PCollectionView<SideT>,
  name: String = "MapWithSideInput",
  crossinline processElement: (InT, SideT) -> OutT,
): PCollection<OutT> {
  return parDoWithSideInput(sideInput, name) { e, side -> yield(processElement(e, side)) }
}

/** Kotlin convenience helper for applying GroupByKey. */
inline fun <KeyT, reified ValueT> PCollection<KV<KeyT, ValueT>>.groupByKey(
  name: String = "GroupByKey"
): PCollection<KV<KeyT, Iterable<ValueT>>> {
  return apply(name, GroupByKey.create())
}

fun <T> PCollection<T>.toSingletonView(name: String = "ToView"): PCollectionView<T> {
  return apply(name, View.asSingleton())
}

fun <K, V> PCollection<KV<K, V>>.toMapView(name: String = "ToMapView"): PCollectionView<Map<K, V>> {
  return apply(name, View.asMap())
}

/**
 * Kotlin convenience helper for combining a PCollection of items into a PCollection with a single
 * list of all items.
 */
fun <T> PCollection<T>.combineIntoList(name: String = "CombineIntoList"): PCollection<List<T>> {
  return map("$name/SingletonLists") { listOf(it) }
    .setCoder(ListCoder.of(coder))
    .apply("$name/Combine", Combine.globally { elements: Iterable<List<T>> -> elements.flatten() })
    .setCoder(ListCoder.of(coder))
}

/** Kotlin convenience helper for a fusion break on a PCollection. */
fun <T> PCollection<T>.breakFusion(name: String = "BreakFusion"): PCollection<T> {
  return apply(name, BreakFusion()).setCoder(coder)
}

/** Convenience function for creating a PCollection<Int> from 0 until n. */
fun Pipeline.createSequence(
  n: Int,
  parallelism: Int = 1000,
  name: String = "CreateSequence",
): PCollection<Int> {
  val numPerBatch: Int = ceil(n / parallelism.toFloat()).toInt()
  return apply("$name/Create", Create.of((0 until parallelism).toList())).parDo("$name/ParDo") {
    val start = it * numPerBatch
    val end = minOf(n, start + numPerBatch)
    yieldAll((start until end).toList())
  }
}

/** Convenience function for using Minus PTransform. */
inline fun <reified T : Any> PCollection<T>.minus(
  other: PCollection<T>,
  name: String = "Minus",
): PCollection<T> {
  return PCollectionList.of(this).and(other).apply(name, Minus()).setCoder(coder)
}
