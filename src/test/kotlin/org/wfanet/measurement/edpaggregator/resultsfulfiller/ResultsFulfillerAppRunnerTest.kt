/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import java.io.File
import java.nio.file.Files
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.kotlin.any

@RunWith(JUnit4::class)
class ResultsFulfillerAppRunnerTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `saveSecretToFile writes bytes to file`() {
    val testFile = tempFolder.newFile("test.pem")
    val data = "testdata".toByteArray()
    val runner = ResultsFulfillerAppRunner()

    runner.saveByteArrayToFile(data, testFile.absolutePath)

    assertThat(Files.exists(testFile.toPath())).isTrue()
    assertThat(data).isEqualTo(Files.readAllBytes(testFile.toPath()))
  }

  @Test
  fun `saveSecretToFile creates parent directories`() {
    val nestedFile = File(tempFolder.root, "nested/dir/file.pem")
    val data = "nested-data".toByteArray()
    val runner = ResultsFulfillerAppRunner()

    runner.saveByteArrayToFile(data, nestedFile.absolutePath)

    assertThat(nestedFile.exists()).isTrue()
    assertThat(data).isEqualTo(nestedFile.readBytes())
  }

  @Test
  fun `saveEdpsCerts calls saveSecretToFile five times for one edp`() {
    val runner = spy(ResultsFulfillerAppRunner())
    runner.javaClass.getDeclaredField("googleProjectId").apply {
      isAccessible = true
      set(runner, "testProject")
    }
    val edp =
      ResultsFulfillerAppRunner.EdpFlags().apply {
        certDerSecretId = "cert"
        privateDerSecretId = "priv"
        encPrivateSecretId = "enc"
        tlsKeySecretId = "tlsKey"
        tlsPemSecretId = "tlsPem"
        certDerFilePath = File(tempFolder.root, "testEdp_cs_cert.der").absolutePath
        privateDerFilePath = File(tempFolder.root, "testEdp_cs_private.der").absolutePath
        encPrivateFilePath = File(tempFolder.root, "testEdp_enc_private.tink").absolutePath
        tlsKeyFilePath = File(tempFolder.root, "testEdp_tls.key").absolutePath
        tlsPemFilePath = File(tempFolder.root, "testEdp_tls.pem").absolutePath
      }

    runner.javaClass.getDeclaredField("edpCerts").apply {
      isAccessible = true
      set(runner, listOf(edp))
    }

    doReturn(ByteArray(0)).`when`(runner).accessSecretBytes(anyString(), anyString(), anyString())
    doNothing().`when`(runner).saveByteArrayToFile(any(), anyString())
    runner.saveEdpsCerts()
    verify(runner, times(5)).saveByteArrayToFile(any(), anyString())
  }

  @Test
  fun `createKmsClients populates kmsClientsMap keys`() {
    val runner = ResultsFulfillerAppRunner()

    val edpCertsField = runner.javaClass.getDeclaredField("edpCerts").apply { isAccessible = true }

    val edp1 =
      ResultsFulfillerAppRunner.EdpFlags().apply {
        edpKmsAudience = "aud1"
        edpTargetServiceAccount = "sa1"
        edpResourceName = "res1"
      }
    val edp2 =
      ResultsFulfillerAppRunner.EdpFlags().apply {
        edpKmsAudience = "aud2"
        edpTargetServiceAccount = "sa2"
        edpResourceName = "res2"
      }

    edpCertsField.set(runner, listOf(edp1, edp2))

    runner.javaClass
      .getDeclaredMethod("createKmsClients")
      .apply { isAccessible = true }
      .invoke(runner)

    val kmsMapField =
      runner.javaClass.getDeclaredField("kmsClientsMap").apply { isAccessible = true }

    @Suppress("UNCHECKED_CAST") val map = kmsMapField.get(runner) as Map<String, *>

    // Verify that the map contains the right KmsClients
    assertThat(map.keys).containsExactly("res1", "res2")
  }
}
