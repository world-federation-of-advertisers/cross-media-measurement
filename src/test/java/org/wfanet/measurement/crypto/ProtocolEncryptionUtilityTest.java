package org.wfanet.anysketch;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.wfanet.measurement.crypto.ProtocolEncryptionUtility;
import org.wfanet.measurement.internal.duchy.BlindLastLayerIndexThenJoinRegistersRequest;
import org.wfanet.measurement.internal.duchy.BlindOneLayerRegisterIndexRequest;
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountRequest;
import org.wfanet.measurement.internal.duchy.DecryptOneLayerFlagAndCountRequest;

@RunWith(JUnit4.class)
public class ProtocolEncryptionUtilityTest {

  static {
    try {
      System.loadLibrary("protocolencryptionutility");
    } catch (UnsatisfiedLinkError e) {
      if (e.getMessage().contains("grte")) {
        throw new RuntimeException(
            "This JNI SketchJavaEncrypter doesn't work with googlejdk.  Use another Java version.");
      } else {
        throw e;
      }
    }
  }

  @Test
  public void EndToEnd_basicBehavior() {
    // TODO(wangyaopw): add test for this
  }

  @Test
  public void BlindOneLayerRegisterIndex_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.BlindOneLayerRegisterIndex(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }

  @Test
  public void BlindLastLayerIndexThenJoinRegisters_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.BlindLastLayerIndexThenJoinRegisters(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }

  @Test
  public void DecryptOneLayerFlagAndCount_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.DecryptOneLayerFlagAndCount(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }

  @Test
  public void DecryptLastLayerFlagAndCount_invalidRequestProtoStringShouldFail() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                ProtocolEncryptionUtility.DecryptLastLayerFlagAndCount(
                    "something not a proto".getBytes()));
    assertThat(exception).hasMessageThat().contains("failed to parse");
  }
}
