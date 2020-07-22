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

#ifndef SRC_MAIN_CC_MEASUREMENT_CRYPTO_UTIL_MACROS_H_
#define SRC_MAIN_CC_MEASUREMENT_CRYPTO_UTIL_MACROS_H_

#define RETURN_IF_ERROR(status)        \
  do {                                 \
    Status _status = (status);         \
    if (!_status.ok()) return _status; \
  } while (0)

#endif  // SRC_MAIN_CC_MEASUREMENT_CRYPTO_UTIL_MACROS_H_
