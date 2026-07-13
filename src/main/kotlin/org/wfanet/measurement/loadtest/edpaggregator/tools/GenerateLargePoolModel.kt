/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * BENCHMARK-ONLY (throwaway). Remove before merge.
 *
 * Rewrites a compiled VID model's RankedPopulationNodes with large ranked pools so the memoized
 * stress test does not overflow. Reads a Riegeli CompiledNode blob (the same format the pipeline
 * reads via VirtualPeoplePoolEmitLabeler), enlarges each RankedPopulationNode's ranked_size and
 * re-spaces its pools' population_offset / total_population, then writes a new Riegeli blob
 * (single block, one uncompressed 0x72 simple-records chunk) that the same reader accepts.
 */
package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.highwayhash.HighwayHash
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.CodedOutputStream
import java.io.ByteArrayOutputStream
import java.io.File
import org.wfanet.measurement.common.riegeli.Riegeli
import org.wfanet.virtualpeople.common.CompiledNode

// HighwayHash key used by Riegeli ('Riegeli/', 'records\n', repeated).
private const val HH_K0 = 0x2f696c6567656952L
private const val HH_K1 = 0x0a7364726f636572L

private fun highwayHash64(data: ByteArray): Long {
  val h = HighwayHash(HH_K0, HH_K1, HH_K0, HH_K1)
  var pos = 0
  while (data.size - pos >= 32) {
    h.updatePacket(data, pos)
    pos += 32
  }
  if (data.size - pos > 0) h.updateRemainder(data, pos, data.size - pos)
  return h.finalize64()
}

private fun le(v: Long, n: Int): ByteArray {
  val b = ByteArray(n)
  for (i in 0 until n) b[i] = ((v ushr (8 * i)) and 0xff).toByte()
  return b
}

private fun varint(v: Long): ByteArray {
  val out = ByteArrayOutputStream()
  val c = CodedOutputStream.newInstance(out)
  c.writeUInt64NoTag(v)
  c.flush()
  return out.toByteArray()
}

/** Writes [records] as a minimal single-block Riegeli file: one uncompressed 0x72 records chunk. */
private fun writeRiegeli(records: List<ByteString>, out: File) {
  val sizesBuf =
    ByteArrayOutputStream().apply { records.forEach { write(varint(it.size().toLong())) } }.toByteArray()
  val valuesBuf = ByteArrayOutputStream().apply { records.forEach { it.writeTo(this) } }.toByteArray()
  val data =
    ByteArrayOutputStream()
      .apply {
        write(0x00) // compressionType = none
        write(varint(sizesBuf.size.toLong()))
        write(sizesBuf)
        write(valuesBuf)
      }
      .toByteArray()

  require(data.size < (1 shl 16)) { "chunk data ${data.size} exceeds one Riegeli block; writer is single-block only" }

  val dataSize = data.size.toLong()
  val decodedDataSize = (sizesBuf.size + valuesBuf.size).toLong()
  val numRecords = records.size.toLong()

  val headerFields =
    ByteArrayOutputStream()
      .apply {
        write(le(dataSize, 8))
        write(le(highwayHash64(data), 8))
        write(0x72) // chunkType = simple records
        write(le(numRecords, 7))
        write(le(decodedDataSize, 8))
      }
      .toByteArray() // exactly 32 bytes
  val chunkHeaderHash = highwayHash64(headerFields)

  val blockFields = ByteArray(16) // previousChunk = 0, nextChunk = 0 (reader validates hash, ignores values)
  val blockHeaderHash = highwayHash64(blockFields)

  out.outputStream().use { os ->
    os.write(le(blockHeaderHash, 8))
    os.write(blockFields)
    os.write(le(chunkHeaderHash, 8))
    os.write(headerFields)
    os.write(data)
  }
}

private class PoolSpacer(val rankedSize: Long, val poolSpan: Long) {
  var poolIdx = 0
  fun transform(node: CompiledNode): CompiledNode =
    when {
      node.hasRankedPopulationNode() -> {
        val rpn = node.rankedPopulationNode.toBuilder()
        rpn.rankedSize = rankedSize
        val pools =
          node.rankedPopulationNode.poolsList.map { p ->
            val off = 1L + poolIdx * poolSpan
            poolIdx++
            p.toBuilder().setPopulationOffset(off).setTotalPopulation(poolSpan).build()
          }
        rpn.clearPools()
        pools.forEach { rpn.addPools(it) }
        node.toBuilder().setRankedPopulationNode(rpn).build()
      }
      node.hasBranchNode() -> {
        val bn = node.branchNode.toBuilder()
        val branches =
          node.branchNode.branchesList.map { br ->
            if (br.hasNode()) br.toBuilder().setNode(transform(br.node)).build() else br
          }
        bn.clearBranches()
        branches.forEach { bn.addBranches(it) }
        node.toBuilder().setBranchNode(bn).build()
      }
      else -> node
    }
}

private fun printRankedSizes(nodes: List<CompiledNode>) {
  val stack = ArrayDeque<CompiledNode>().apply { nodes.forEach { addLast(it) } }
  while (stack.isNotEmpty()) {
    val node = stack.removeLast()
    when {
      node.hasRankedPopulationNode() -> {
        val rpn = node.rankedPopulationNode
        for (pool in rpn.poolsList) {
          println("  pool_offset=${pool.populationOffset} total_population=${pool.totalPopulation} ranked_size=${rpn.rankedSize}")
        }
      }
      node.hasBranchNode() -> node.branchNode.branchesList.forEach { if (it.hasNode()) stack.addLast(it.node) }
    }
  }
}

fun main(args: Array<String>) {
  require(args.size >= 2) { "usage: GenerateLargePoolModel <input> <output> [rankedSize=1000000000]" }
  val input = args[0]
  val output = args[1]
  val rankedSize = if (args.size > 2) args[2].toLong() else 1_000_000_000L
  val poolSpan = rankedSize * 2 // total_population per pool (> ranked_size, ranges non-overlapping)

  val nodes =
    Riegeli.readRecords(File(input)).map { ProtoAny.parseFrom(it).unpack(CompiledNode::class.java) }.toList()
  println("read ${nodes.size} record(s)")
  println("--- BEFORE ---")
  printRankedSizes(nodes)

  val spacer = PoolSpacer(rankedSize, poolSpan)
  val outNodes = nodes.map { spacer.transform(it) }
  println("rewrote ${spacer.poolIdx} pool(s) to ranked_size=$rankedSize")

  val records = outNodes.map { ProtoAny.pack(it).toByteString() }
  writeRiegeli(records, File(output))
  println("wrote ${File(output).length()} bytes to $output")

  val back =
    Riegeli.readRecords(File(output)).map { ProtoAny.parseFrom(it).unpack(CompiledNode::class.java) }.toList()
  println("read-back ${back.size} record(s)")
  println("--- AFTER ---")
  printRankedSizes(back)
}
