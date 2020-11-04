// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.gcloud.common

import com.google.cloud.ByteArray as GcloudByteArray
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.Parser

fun ByteArray.toGcloudByteArray(): GcloudByteArray = GcloudByteArray.copyFrom(this)

fun ByteString.toGcloudByteArray(): GcloudByteArray =
  GcloudByteArray.copyFrom(asReadOnlyByteBuffer())

fun Message.toGcloudByteArray(): GcloudByteArray = toByteArray().toGcloudByteArray()

fun <T : Message> Parser<T>.parseFrom(gcloudByteArray: GcloudByteArray): T {
  return parseFrom(gcloudByteArray.toByteArray())
}
