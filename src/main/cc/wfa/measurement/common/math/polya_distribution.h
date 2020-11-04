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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_MATH_POLYA_DISTRIBUTION_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_MATH_POLYA_DISTRIBUTION_H_

#include <cstdint>

namespace wfa::measurement::common::math {

// Create a Polya random variable with distribution of Polya(a,r).
// S. Goryczka and L. Xiong, "A Comprehensive Comparison of Multiparty Secure
// Additions with Differential Privacy," in IEEE Transactions on Dependable and
// Secure Computing, vol. 14, no. 5, pp. 463-477, 1 Sept.-Oct. 2017,
// doi: 10.1109/TDSC.2015.2484326.
int64_t getPolyaRandomVariable(double alpha, double r);

// Create a Polya random variable with distribution of Polya(a,r)
int64_t getPolyaRandomVariable(double alpha, double r, int64_t seed);

}  // namespace wfa::measurement::common::math

#endif  // WFA_MEASUREMENT_COMMON_MATH_POLYA_DISTRIBUTION_H_
