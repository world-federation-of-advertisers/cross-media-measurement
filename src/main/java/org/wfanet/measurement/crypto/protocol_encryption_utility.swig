%include "exception.i"
%{
#include "src/main/cc/measurement/crypto/protocol_encryption_utility_wrapper.h"
%}

// Convert C++ ::private_join_and_compute::StatusOr<std::string> to a Java byte
// array.
%typemap(jni) ::private_join_and_compute::StatusOr<std::string> "jbyteArray"
%typemap(jtype) ::private_join_and_compute::StatusOr<std::string> "byte[]"
%typemap(jstype) ::private_join_and_compute::StatusOr<std::string> "byte[]"
%typemap(out) ::private_join_and_compute::StatusOr<std::string> {
  if ($1.ok()) {
    $result = JCALL1(NewByteArray, jenv, $1.value().length());
    JCALL4(SetByteArrayRegion, jenv, $result, 0,
           $1.value().length(), (const jbyte*) $1.value().data());
  } else {
    SWIG_exception(SWIG_RuntimeError, $1.status().ToString().c_str());
  }
}
%typemap(javaout) ::private_join_and_compute::StatusOr<std::string> {
  return $jnicall;
}

// Convert Java byte array to C++ const std::string& for any const std::string&
// input parameter.
%typemap(jni) const std::string& "jbyteArray"
%typemap(jtype) const std::string& "byte[]"
%typemap(jstype) const std::string& "byte[]"
%typemap(javain) const std::string& "$javainput"
%typemap(in) const std::string& {
  jsize temp_length = JCALL1(GetArrayLength, jenv, $input);
  jbyte* temp_bytes = JCALL2(GetByteArrayElements, jenv, $input, 0);
  $1 = new std::string((const char*) temp_bytes, temp_length);
  JCALL3(ReleaseByteArrayElements, jenv, $input, temp_bytes, JNI_ABORT);
}
// Convert C++ std::string to a Java byte array.
%clear std::string;
%typemap(jni) std::string "jbyteArray"
%typemap(jtype) std::string "byte[]"
%typemap(jstype) std::string "byte[]"
%typemap(out) std::string {
  $result = JCALL1(NewByteArray, jenv, $1.length());
  JCALL4(SetByteArrayRegion, jenv, $result, 0,
         $1.length(), (const jbyte*) $1.data());
}
%typemap(javaout) std::string {
  return $jnicall;
}


%include "src/main/cc/measurement/crypto/protocol_encryption_utility_wrapper.h"