// Copyright 2023 The Cross-Media Measurement Authors
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

// In powers of 10
const MAGNITUDES = Object.freeze({
  3: 'K',
  6: 'M',
  9: 'B',
  12: 'T',
});

export const formatNumberWithMagnitude = (
  num: number,
  decimals: number,
  trailingZeros: boolean = false,
) => {
  let power = 0;
  let newNumber = num;
  while (newNumber >= 1000) {
      newNumber = newNumber / 1000;
      power += 3;
  }

  if (power === 0) {
      return num.toString();
  } else {
      const value = (newNumber.toFixed(decimals));

      return trailingZeros ?
          value.toString() + MAGNITUDES[power]
          : value.toString().replace(/\.0+$/, '') + MAGNITUDES[power]
  }
}
