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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.StringValue
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.stringValue
import java.io.ByteArrayOutputStream
import java.io.File
import kotlin.test.assertNotNull
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.map
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.beam.testing.assertThat
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val LABEL = "some-label"
private const val BLOB_KEY = "some-blob-key"
private val BLOB_CONTENTS = "some-data".toByteStringUtf8()

@RunWith(JUnit4::class)
class ApacheBeamContextTest : BeamTestBase() {
  @get:Rule val temporaryFolder = TemporaryFolder()

  private lateinit var storageFactory: StorageFactory
  private lateinit var storageClient: StorageClient

  @Before
  fun setup() {
    val path = temporaryFolder.root.path
    storageFactory = StorageFactory { FileSystemStorageClient(File(path)) }
    storageClient = storageFactory.build()
  }

  @Test
  fun readBlob() = runBlockingTest {
    val inputLabels = mapOf(LABEL to BLOB_KEY)
    val inputBlobs = mapOf(LABEL to storageClient.writeBlob(BLOB_KEY, BLOB_CONTENTS))
    val context =
      ApacheBeamContext(pipeline, mapOf(), inputLabels, mapOf(), inputBlobs, storageFactory)

    assertThat(context.readBlob(LABEL)).isEqualTo(BLOB_CONTENTS)
  }

  @Test
  fun readBlobAsPCollection() = runBlockingTest {
    val inputLabels = mapOf(LABEL to BLOB_KEY)
    val inputBlobs = mapOf(LABEL to storageClient.writeBlob(BLOB_KEY, BLOB_CONTENTS))
    val context =
      ApacheBeamContext(pipeline, mapOf(), inputLabels, mapOf(), inputBlobs, storageFactory)

    assertThat(context.readBlobAsPCollection(LABEL)).containsInAnyOrder(BLOB_CONTENTS)
    assertThat(context.readBlobAsView(LABEL)).containsInAnyOrder(BLOB_CONTENTS)

    pipeline.run()
  }

  @Test
  fun readShardedFile() = runBlockingTest {
    val inputLabels = mapOf(LABEL to BLOB_KEY)
    val inputBlobs =
      mapOf(LABEL to storageClient.writeBlob(BLOB_KEY, "foo-*-of-3".toByteStringUtf8()))

    val shard0Contents = serializeStringsAsProtos("a", "bc", "def")
    val shard1Contents = serializeStringsAsProtos("xy")
    val shard2Contents = serializeStringsAsProtos("z")

    storageClient.writeBlob("foo-0-of-3", shard0Contents)
    storageClient.writeBlob("foo-1-of-3", shard1Contents)
    storageClient.writeBlob("foo-2-of-3", shard2Contents)

    val context =
      ApacheBeamContext(pipeline, mapOf(), inputLabels, mapOf(), inputBlobs, storageFactory)

    val stringValueProtos = context.readShardedPCollection(LABEL, stringValue {})
    assertThat(stringValueProtos.map { it.value }).containsInAnyOrder("a", "bc", "def", "xy", "z")

    pipeline.run()
  }

  @Test
  fun write() = runBlockingTest {
    val outputManifests = mapOf("some-output" to ShardedFileName("some-output-blobkey", 1))
    val context =
      ApacheBeamContext(pipeline, mapOf(), mapOf(), outputManifests, mapOf(), storageFactory)
    val items = pcollectionOf("Test Collection", stringValue { value = "some-item" })
    with(context) { items.writeShardedFiles("some-output") }

    pipeline.run()

    val blob = storageClient.getBlob("some-output-blobkey-0-of-1")
    assertNotNull(blob)

    val stringValues = blob.toByteString().parseDelimitedMessages(stringValue {})

    assertThat(stringValues).containsExactly(stringValue { value = "some-item" })
  }

  @Test
  fun writeSingleBlob() = runBlockingTest {
    val label = "some-label"
    val blobKey = "some-blob-key"
    val outputLabels = mapOf(label to blobKey)
    val context =
      ApacheBeamContext(pipeline, outputLabels, mapOf(), mapOf(), mapOf(), storageFactory)
    val item = stringValue { value = "some-item" }

    with(context) { pcollectionOf("Test Collection", item).writeSingleBlob(label) }

    pipeline.run()

    val blob = storageClient.getBlob(blobKey)
    assertNotNull(blob)

    assertThat(StringValue.parseFrom(blob.toByteString())).isEqualTo(item)
  }
}

private fun serializeStringsAsProtos(vararg items: String): ByteString {
  val output = ByteArrayOutputStream()
  items.forEach { stringValue { value = it }.writeDelimitedTo(output) }
  return output.toByteArray().toByteString()
}
