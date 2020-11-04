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

#include "wfa/measurement/common/math/polya_distribution.h"

#include <chrono>
#include <random>

#include "absl/random/poisson_distribution.h"

namespace wfa::measurement::common::math {

int64_t getPolyaRandomVariable(double alpha, double r) {
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  return getPolyaRandomVariable(alpha, r, seed);
}

int64_t getPolyaRandomVariable(double alpha, double r, int64_t seed) {
  std::default_random_engine generator(seed);
  std::gamma_distribution<double> gamma_distribution(alpha, r / (1 - r));
  absl::poisson_distribution<int> poisson_distribution(
      gamma_distribution(generator));
  return poisson_distribution(generator);
}

}  // namespace wfa::measurement::common::math
