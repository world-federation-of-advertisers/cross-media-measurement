// Copyright 2020 The Cross-Media Measurement Authors
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

%include "exception.i"
%{
#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility_wrapper.h"
%}

// Convert C++ ::absl::StatusOr<std::string> to a Java byte
// array.
%typemap(jni) ::absl::StatusOr<std::string> "jbyteArray"
%typemap(jtype) ::absl::StatusOr<std::string> "byte[]"
%typemap(jstype) ::absl::StatusOr<std::string> "byte[]"
%typemap(out) ::absl::StatusOr<std::string> {
  if ($1.ok()) {
    $result = JCALL1(NewByteArray, jenv, $1.value().length());
    JCALL4(SetByteArrayRegion, jenv, $result, 0,
           $1.value().length(), (const jbyte*) $1.value().data());
  } else {
    SWIG_exception(SWIG_RuntimeError, $1.status().ToString().c_str());
  }
}
%typemap(javaout) ::absl::StatusOr<std::string> {
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

// Follow Java style for function names.
%rename("%(lowercamelcase)s",%$isfunction) "";

%include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility_wrapper.h"
