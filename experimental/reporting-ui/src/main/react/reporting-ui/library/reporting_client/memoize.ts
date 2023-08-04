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

// Return a function that is wrapped with the memoization logic
export class Memoizer {
  cache: Map<string, any> = new Map();

  memoizePromiseFn = fn => {
    return (...args) => {
      const key = JSON.stringify(args);
  
      if (this.cache.has(key)) {
        return this.cache.get(key);
      }
  
      this.cache.set(
        key,
        fn(...args).catch(error => {
          // Delete cache entry if API call fails
          this.cache.delete(key);
          return Promise.reject(error);
        })
      );
  
      return this.cache.get(key);
    };
  };
};
