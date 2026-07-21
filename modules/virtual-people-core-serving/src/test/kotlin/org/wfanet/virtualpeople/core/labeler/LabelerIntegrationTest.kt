// Copyright 2022 The Cross-Media Measurement Authors
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
// limitations under the \License.

package org.wfanet.virtualpeople.core.labeler

import com.google.common.truth.extensions.proto.ProtoTruth
import java.io.File
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.virtualpeople.common.compiledNode
import org.wfanet.virtualpeople.common.copy
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.labelerOutput

private const val TEXTPROTO_PATH = "src/main/resources/testing/labeler"

@RunWith(JUnit4::class)
class LabelerIntegrationTest {

  private fun applyAndValidate(
    labeler: Labeler,
    inputFileName: String,
    outputFileName: String,
  ) {
    val input =
      parseTextProto(File("$TEXTPROTO_PATH/$inputFileName").bufferedReader(), labelerInput {})
    val expectedOutput =
      parseTextProto(File("$TEXTPROTO_PATH/$outputFileName").bufferedReader(), labelerOutput {})
    val output = labeler.label(input).copy { clearSerializedDebugTrace() }

    ProtoTruth.assertThat(output).isEqualTo(expectedOutput)
  }

  @Test
  fun `build from root`() {
    val modelFileName = "toy_model.textproto"
    val rootNode =
      parseTextProto(File("$TEXTPROTO_PATH/$modelFileName").bufferedReader(), compiledNode {})
    val labeler = Labeler.build(rootNode)

    (1 until 19).forEach { index ->
      val indexString = "%02d".format(index)
      val inputFileName = "labeler_input_$indexString.textproto"
      val outputFileName = "labeler_output_$indexString.textproto"
      applyAndValidate(labeler, inputFileName, outputFileName)
    }
  }

  @Test
  fun `single id model`() {
    val modelFileName = "single_id_model.textproto"
    val rootNode =
      parseTextProto(File("$TEXTPROTO_PATH/$modelFileName").bufferedReader(), compiledNode {})
    val labeler = Labeler.build(rootNode)

    (1 until 19).forEach { index ->
      val indexString = "%02d".format(index)
      val inputFileName = "labeler_input_$indexString.textproto"
      val outputFileName = "single_id_labeler_output.textproto"
      applyAndValidate(labeler, inputFileName, outputFileName)
    }
  }
}
