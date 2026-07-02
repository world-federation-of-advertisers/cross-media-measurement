/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.parquetRow
import org.wfanet.measurement.storage.parquetValue

@RunWith(JUnit4::class)
class BaseVidLabelingTeeAppRunnerTest {
  @get:Rule val tempDir = TemporaryFolder()

  /**
   * Minimal concrete [BaseVidLabelingTeeAppRunner] that injects a caller-supplied Hadoop
   * [Configuration] (here a local `file://` config) instead of the production GCS one, proving the
   * Parquet seam is exercisable without GCS and without mocking it.
   */
  private class TestRunner(rootUri: String, conf: Configuration) :
    BaseVidLabelingTeeAppRunner(storageRootUri = rootUri, hadoopConfigurationFor = { conf }) {
    override fun run() = error("not exercised by this test")

    fun parquetClient(kms: KmsClient): ParquetStorageClient =
      buildParquetStorageClient(StorageConfig(), kms)

    fun storageClient(cfg: StorageConfig): ConditionalOperationStorageClient =
      buildStorageClient(cfg)
  }

  @Test
  fun `buildParquetStorageClient round-trips an encrypted parquet via an injected file config`() =
    runBlocking {
      AeadConfig.register()
      val kekUri = "fake-kms://kek"
      val aead =
        KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM")).getPrimitive(Aead::class.java)
      val kms = FakeKmsClient().also { it.setAead(kekUri, aead) }

      // A plain local-filesystem Hadoop config (no fs.gs.impl) with parquet-mr PME keys; the
      // production runner would instead inject gcsHadoopConfiguration(projectId).
      val conf = Configuration().apply { set("parquet.encryption.uniform.key", kekUri) }
      val client = TestRunner(tempDir.root.absolutePath, conf).parquetClient(kms)

      val key = "model/sample.parquet"
      client.writeBlob(
        key,
        flow {
          emit(
            parquetRow { columns["event_id"] = parquetValue { stringValue = "e1" } }.toByteString()
          )
        },
      )

      val rows = client.getBlob(key)!!.readRows().toList()
      assertThat(rows).hasSize(1)
      assertThat(rows[0].getValue("event_id").stringValue).isEqualTo("e1")
    }

  @Test
  fun `buildStorageClient round-trips multiple relative keys under one bucket`() = runBlocking {
    // A file://-rooted multi-key store: the prefix's first path segment is the bucket, resolved
    // under the temp root. buildStorageClient must return a bucket-rooted (multi-key) client, NOT
    // a single-blob SelectedStorageClient, so several distinct keys round-trip. Rooting a
    // single-blob client at a bare "gs://" (the prior bug) would reject every real key.
    tempDir.newFolder("rank-maps")
    val cfg = StorageConfig(rootDirectory = tempDir.root, blobPrefix = "file:///rank-maps/prefix")
    val client: ConditionalOperationStorageClient =
      TestRunner(tempDir.root.absolutePath, Configuration()).storageClient(cfg)

    client.writeBlob("snapshot-1", flow { emit(ByteString.copyFromUtf8("one")) })
    client.writeBlob("snapshot-2", flow { emit(ByteString.copyFromUtf8("two")) })

    assertThat(client.getBlob("snapshot-1")!!.read().flatten().toStringUtf8()).isEqualTo("one")
    assertThat(client.getBlob("snapshot-2")!!.read().flatten().toStringUtf8()).isEqualTo("two")
  }
}
