/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.securecomputation

import java.time.Instant
import org.wfanet.measurement.securecomputation.v1alpha.impression
import org.wfanet.measurement.securecomputation.v1alpha.labeledImpression
import org.wfanet.measurement.securecomputation.v1alpha.LabeledImpression
import org.wfanet.measurement.securecomputation.v1alpha.Impression.Gender
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.Flow
import org.wfanet.virtualpeople.common.virtualPersonActivity
import org.wfanet.virtualpeople.common.personLabelAttributes
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.virtualpeople.common.Gender as VirtualGender
import java.io.File
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import java.nio.file.Files
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import org.wfanet.measurement.common.crypto.tink.StreamingAeadStorageClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.nio.channels.Channels
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import java.io.ByteArrayInputStream
import org.wfanet.measurement.common.flatten

class SyntheticImpressionGeneration(
  private val startInstant: Long = Instant.now().minus(90, ChronoUnit.DAYS).toEpochMilli(),
  private val endInstant: Long = Instant.now().toEpochMilli(),
) {

  private fun getRandomString(): String {
    val charPool = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    val length = Random.nextInt(5, 11) // upper bound is exclusive
    return (1..length).map { Random.nextInt(charPool.length) }.map { charPool[it] }.joinToString("")
  }

  fun generateImpressions(numImpressions: Int): Flow<ByteString> = flow {
    for (i in 1..numImpressions) {
      val data = labeledImpression {
        this.impression = impression {
          impressionUnixTimeStamp =
            (startInstant + (startInstant - endInstant) * Random.nextDouble()).toLong()
          gender = Gender.forNumber(Random.nextInt(3))
          age = Random.nextInt(83) + 18
          geo = Random.nextLong() % 100 + 1
          deviceTypeOs = listOf("Android", "iOS", "Windows", "MacOS", "Linux").random()
          campaignId = Random.nextLong() % 100 + 1
          watchDurationMs = (Random.nextInt(60000) + 1).toLong()
          adDurationMs = (Random.nextInt(60000) + 1000).toLong()
          bannerViewable = listOf(true, false).random()
          country = listOf("US", "CA", "GB", "AU", "DE").random()
          joinId = Random.nextLong()
          email = "${getRandomString()}@${getRandomString()}.${listOf("com", "net", "gov", "org").random()}"
          phoneNumber =
            "+1-${Random.nextInt(1000)}-${Random.nextInt(1000)}-${Random.nextInt(1000)}"
          placement = listOf("banner", "feed", "fullscreen", "popup").random()
        }
        people += virtualPersonActivity {
          virtualPersonId = Random.nextLong()
          label = personLabelAttributes {
            demo = demoBucket {
              gender = VirtualGender.forNumber(Random.nextInt(3))
              age = ageRange {
                val ageRangeStart = Random.nextInt(83) + 18
                minAge = ageRangeStart
                maxAge = ageRangeStart + 10
              }
            }
          }
        }
      }
      emit(data.toByteString())
    }
  }
}

fun getFileSizeInHumanReadableFormat(file: File): String {
  var fileSize = file.length().toDouble()
  val units = arrayOf("bytes", "KB", "MB", "GB", "TB")
  var index = 0
  while (fileSize >= 1024 && index < units.size - 1) {
    index++
    fileSize /= 1024
  }
  return "${String.format("%.2f", fileSize)} ${units[index]}"
}

fun Flow<ByteString>.gzip(): Flow<ByteString> = flow {
  val bos = ByteArrayOutputStream()
  val gos = GZIPOutputStream(bos)
  collect { byteString ->
    gos.write(byteString.toByteArray())
    emit(ByteString.copyFrom(bos.toByteArray()))
    bos.reset()
  }
  gos.close()
}

fun main(args: Array<String>) {
  val impressions = if ("gzip" in args) {
    SyntheticImpressionGeneration().generateImpressions(1_000_000).gzip()
  } else {
    SyntheticImpressionGeneration().generateImpressions(1_000_000)
  }
  val directory = Files.createTempDirectory(null).toFile()
  directory.deleteOnExit()
  val fileSystemStorageClient = FileSystemStorageClient(directory = directory)
  val mesosStorageClient = MesosRecordIoStorageClient(fileSystemStorageClient)
  val blobKey = "some-key"
  runBlocking {
    if ("aead" in args) {
      StreamingAeadConfig.register()
      val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM_HKDF_1MB")
      val KEY_ENCRYPTION_KEY = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
      val streamingAead = KEY_ENCRYPTION_KEY.getPrimitive(StreamingAead::class.java)
      val streamingAeadStorageClient = StreamingAeadStorageClient(mesosStorageClient, streamingAead)
      streamingAeadStorageClient.writeBlob(blobKey, impressions)
    } else {
      mesosStorageClient.writeBlob(blobKey, impressions)
    }
  }
  println("File size: ${getFileSizeInHumanReadableFormat(File(directory.path, blobKey))} bytes")
}
