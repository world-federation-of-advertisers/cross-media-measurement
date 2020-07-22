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

package org.wfanet.measurement.crypto;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.wfanet.anysketch.crypto.EncryptSketchRequest;
import org.wfanet.anysketch.crypto.EncryptSketchResponse;
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter;
import org.wfanet.measurement.internal.duchy.BlindLastLayerIndexThenJoinRegistersRequest;
import org.wfanet.measurement.internal.duchy.BlindLastLayerIndexThenJoinRegistersResponse;
import org.wfanet.measurement.internal.duchy.BlindOneLayerRegisterIndexRequest;
import org.wfanet.measurement.internal.duchy.BlindOneLayerRegisterIndexResponse;
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountRequest;
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountResponse;
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountResponse.FlagCount;
import org.wfanet.measurement.internal.duchy.DecryptOneLayerFlagAndCountRequest;
import org.wfanet.measurement.internal.duchy.DecryptOneLayerFlagAndCountResponse;
import org.wfanet.measurement.internal.duchy.ElGamalKeys;
import org.wfanet.measurement.internal.duchy.ElGamalPublicKeys;
import wfa.measurement.api.v1alpha.SketchOuterClass.Sketch;
import wfa.measurement.api.v1alpha.SketchOuterClass.Sketch.Register;
import wfa.measurement.api.v1alpha.SketchOuterClass.SketchConfig;
import wfa.measurement.api.v1alpha.SketchOuterClass.SketchConfig.ValueSpec;
import wfa.measurement.api.v1alpha.SketchOuterClass.SketchConfig.ValueSpec.Aggregator;

@RunWith(JUnit4.class)
public class ProtocolEncryptionUtilityTest {

  static {
    System.loadLibrary("protocol_encryption_utility");
    System.loadLibrary("sketch_encrypter_adapter");
  }

  private static final int CURVE_ID = 415; // NID_X9_62_prime256v1
  private static final int MAX_COUNTER_VALUE = 10;

  private static final String DUCHY_1_PK_G =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296";
  private static final String DUCHY_1_PK_Y =
      "02d1432ca007a6c6d739fce2d21feb56d9a2c35cf968265f9093c4b691e11386b3";
  private static final String DUCHY_1_SK =
      "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888";
  private static final String DUCHY_2_PK_G =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296";
  private static final String DUCHY_2_PK_Y =
      "039ef370ff4d216225401781d88a03f5a670a5040e6333492cb4e0cd991abbd5a3";
  private static final String DUCHY_2_SK =
      "31cc32e7cd53ff24f2b64ae8c531099af9867ebf5d9a659f742459947caa29b0";
  private static final String DUCHY_3_PK_G =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296";
  private static final String DUCHY_3_PK_Y =
      "02d0f25ab445fc9c29e7e2509adc93308430f432522ffa93c2ae737ceb480b66d7";
  private static final String DUCHY_3_SK =
      "338cce0306416b70e901436cb9eca5ac758e8ff41d7b58dabadf8726608ca6cc";
  private static final String CLIENT_PK_G =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296";
  private static final String CLIENT_PK_Y =
      "02505d7b3ac4c3c387c74132ab677a3421e883b90d4c83dc766e400fe67acc1f04";

  private static final ElGamalKeys DUCHY_1_EL_GAMAL_KEYS =
      ElGamalKeys.newBuilder()
          .setElGamalPk(
              ElGamalPublicKeys.newBuilder()
                  .setElGamalG(hexToByteString(DUCHY_1_PK_G))
                  .setElGamalY(hexToByteString(DUCHY_1_PK_Y)))
          .setElGamalSk(hexToByteString(DUCHY_1_SK))
          .build();
  private static final ElGamalKeys DUCHY_2_EL_GAMAL_KEYS =
      ElGamalKeys.newBuilder()
          .setElGamalPk(
              ElGamalPublicKeys.newBuilder()
                  .setElGamalG(hexToByteString(DUCHY_2_PK_G))
                  .setElGamalY(hexToByteString(DUCHY_2_PK_Y)))
          .setElGamalSk(hexToByteString(DUCHY_2_SK))
          .build();
  private static final ElGamalKeys DUCHY_3_EL_GAMAL_KEYS =
      ElGamalKeys.newBuilder()
          .setElGamalPk(
              ElGamalPublicKeys.newBuilder()
                  .setElGamalG(hexToByteString(DUCHY_3_PK_G))
                  .setElGamalY(hexToByteString(DUCHY_3_PK_Y)))
          .setElGamalSk(hexToByteString(DUCHY_3_SK))
          .build();
  private static final ElGamalPublicKeys CLIENT_EL_GAMAL_KEYS =
      ElGamalPublicKeys.newBuilder()
          .setElGamalG(hexToByteString(CLIENT_PK_G))
          .setElGamalY(hexToByteString(CLIENT_PK_Y))
          .build();
  private static final org.wfanet.anysketch.crypto.ElGamalPublicKeys SKETCH_ENCRYPTER_KEY =
      org.wfanet.anysketch.crypto.ElGamalPublicKeys.newBuilder()
          .setElGamalG(hexToByteString(CLIENT_PK_G))
          .setElGamalY(hexToByteString(CLIENT_PK_Y))
          .build();

  private static ByteString hexToByteString(String hexString) {
    checkArgument(hexString.length() % 2 == 0);
    byte[] result = new byte[hexString.length() / 2];
    for (int i = 0; i < result.length; i += 1) {
      int decimal = Integer.parseInt(hexString.substring(i * 2, i * 2 + 2), 16);
      result[i] = (byte) decimal;
    }
    return ByteString.copyFrom(result);
  }

  private void addRegister(Sketch.Builder sketch, int index, int key, int count) {
    sketch.addRegisters(Register.newBuilder().setIndex(index).addValues(key).addValues(count));
  }

  private Sketch.Builder createEmptyClceSketch() {
    return Sketch.newBuilder()
        .setConfig(
            SketchConfig.newBuilder()
                .addValues(ValueSpec.newBuilder().setAggregator(Aggregator.UNIQUE))
                .addValues(ValueSpec.newBuilder().setAggregator(Aggregator.SUM)));
  }

  // Helper function to go through the entire MPC protocol using the input data.
  // The final (flag, count) lists are returned.
  private DecryptLastLayerFlagAndCountResponse goThroughEntireMpcProtocol(
      ByteString encrypted_sketch) throws Exception {
    // Blind register indexes at duchy 1
    BlindOneLayerRegisterIndexRequest blind_one_layer_register_index_request_1 =
        BlindOneLayerRegisterIndexRequest.newBuilder()
            .setCurveId(CURVE_ID)
            .setLocalElGamalKeys(DUCHY_1_EL_GAMAL_KEYS)
            .setCompositeElGamalKeys(CLIENT_EL_GAMAL_KEYS)
            .setSketch(encrypted_sketch)
            .build();
    BlindOneLayerRegisterIndexResponse blind_one_layer_register_index_response_1 =
        BlindOneLayerRegisterIndexResponse.parseFrom(
            ProtocolEncryptionUtility.BlindOneLayerRegisterIndex(
                blind_one_layer_register_index_request_1.toByteArray()));

    // Blind register indexes at duchy 2
    BlindOneLayerRegisterIndexRequest blind_one_layer_register_index_request_2 =
        BlindOneLayerRegisterIndexRequest.newBuilder()
            .setCurveId(CURVE_ID)
            .setLocalElGamalKeys(DUCHY_2_EL_GAMAL_KEYS)
            .setCompositeElGamalKeys(CLIENT_EL_GAMAL_KEYS)
            .setSketch(blind_one_layer_register_index_response_1.getSketch())
            .build();
    BlindOneLayerRegisterIndexResponse blind_one_layer_register_index_response_2 =
        BlindOneLayerRegisterIndexResponse.parseFrom(
            ProtocolEncryptionUtility.BlindOneLayerRegisterIndex(
                blind_one_layer_register_index_request_2.toByteArray()));

    // Blind register indexes and join registers at duchy 3 (primary duchy)
    BlindLastLayerIndexThenJoinRegistersRequest blind_last_layer_index_then_join_registers_request =
        BlindLastLayerIndexThenJoinRegistersRequest.newBuilder()
            .setCurveId(CURVE_ID)
            .setCompositeElGamalKeys(CLIENT_EL_GAMAL_KEYS)
            .setLocalElGamalKeys(DUCHY_3_EL_GAMAL_KEYS)
            .setSketch(blind_one_layer_register_index_response_2.getSketch())
            .build();
    BlindLastLayerIndexThenJoinRegistersResponse
        blind_last_layer_index_then_join_registers_response =
            BlindLastLayerIndexThenJoinRegistersResponse.parseFrom(
                ProtocolEncryptionUtility.BlindLastLayerIndexThenJoinRegisters(
                    blind_last_layer_index_then_join_registers_request.toByteArray()));

    // Decrypt flags and counts at duchy 1
    DecryptOneLayerFlagAndCountRequest decrypt_one_layer_flag_and_count_request_1 =
        DecryptOneLayerFlagAndCountRequest.newBuilder()
            .setFlagCounts(blind_last_layer_index_then_join_registers_response.getFlagCounts())
            .setCurveId(CURVE_ID)
            .setLocalElGamalKeys(DUCHY_1_EL_GAMAL_KEYS)
            .build();
    DecryptOneLayerFlagAndCountResponse decrypt_one_layer_flag_and_count_response_1 =
        DecryptOneLayerFlagAndCountResponse.parseFrom(
            ProtocolEncryptionUtility.DecryptOneLayerFlagAndCount(
                decrypt_one_layer_flag_and_count_request_1.toByteArray()));

    // Decrypt flags and counts at duchy 2
    DecryptOneLayerFlagAndCountRequest decrypt_one_layer_flag_and_count_request_2 =
        DecryptOneLayerFlagAndCountRequest.newBuilder()
            .setFlagCounts(decrypt_one_layer_flag_and_count_response_1.getFlagCounts())
            .setCurveId(CURVE_ID)
            .setLocalElGamalKeys(DUCHY_2_EL_GAMAL_KEYS)
            .build();
    DecryptOneLayerFlagAndCountResponse decrypt_one_layer_flag_and_count_response_2 =
        DecryptOneLayerFlagAndCountResponse.parseFrom(
            ProtocolEncryptionUtility.DecryptOneLayerFlagAndCount(
                decrypt_one_layer_flag_and_count_request_2.toByteArray()));

    // Decrypt flags and counts at duchy 3 (primary duchy).
    DecryptLastLayerFlagAndCountRequest decrypt_last_layer_flag_and_count_request =
        DecryptLastLayerFlagAndCountRequest.newBuilder()
            .setFlagCounts(decrypt_one_layer_flag_and_count_response_2.getFlagCounts())
            .setCurveId(CURVE_ID)
            .setLocalElGamalKeys(DUCHY_3_EL_GAMAL_KEYS)
            .setMaximumFrequency(MAX_COUNTER_VALUE)
            .build();
    DecryptLastLayerFlagAndCountResponse final_response =
        DecryptLastLayerFlagAndCountResponse.parseFrom(
            ProtocolEncryptionUtility.DecryptLastLayerFlagAndCount(
                decrypt_last_layer_flag_and_count_request.toByteArray()));

    return final_response;
  }

  private FlagCount newFlagCount(boolean isNotDestoryed, int frequency) {
    return FlagCount.newBuilder().setIsNotDestroyed(isNotDestoryed).setFrequency(frequency).build();
  }

  @Test
  public void endToEnd_basicBehavior() throws Exception {
    Sketch.Builder rawSketch = createEmptyClceSketch();
    addRegister(rawSketch, /* index = */ 1, /* key = */ 111, /* count = */ 2);
    addRegister(rawSketch, /* index = */ 1, /* key = */ 111, /* count = */ 3);
    addRegister(rawSketch, /* index = */ 2, /* key = */ 222, /* count = */ 1);
    addRegister(rawSketch, /* index = */ 2, /* key = */ 333, /* count = */ 3);
    addRegister(rawSketch, /* index = */ 3, /* key = */ 444, /* count = */ 12);
    addRegister(rawSketch, /* index = */ 4, /* key = */ 555, /* count = */ 0);

    EncryptSketchRequest request =
        EncryptSketchRequest.newBuilder()
            .setSketch(rawSketch)
            .setCurveId(CURVE_ID)
            .setMaximumValue(MAX_COUNTER_VALUE)
            .setElGamalKeys(SKETCH_ENCRYPTER_KEY)
            .build();

    EncryptSketchResponse response =
        EncryptSketchResponse.parseFrom(
            SketchEncrypterAdapter.EncryptSketch(request.toByteArray()));

    ByteString encryptedSketch = response.getEncryptedSketch();

    List<FlagCount> result = goThroughEntireMpcProtocol(encryptedSketch).getFlagCountsList();

    assertThat(result)
        .containsExactly(
            newFlagCount(true, 5), // key 111, count 2+3=5.
            newFlagCount(true, 10), // key 444, capped by MAX_COUNTER_VALUE.
            newFlagCount(false, 10)); // key 222 and key 333 collide.
    // key 555 is ignored because the count is 0.
  }

  @Test
  public void blindOneLayerRegisterIndex_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.BlindOneLayerRegisterIndex(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }

  @Test
  public void blindLastLayerIndexThenJoinRegisters_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.BlindLastLayerIndexThenJoinRegisters(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }

  @Test
  public void decryptOneLayerFlagAndCount_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.DecryptOneLayerFlagAndCount(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }

  @Test
  public void decryptLastLayerFlagAndCount_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.DecryptLastLayerFlagAndCount(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }
}
