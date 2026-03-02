# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections.abc import MutableMapping
from dataclasses import dataclass


@dataclass
class Metric:
  value: float
  sigma: float
  name: str
  index: int = -1


@dataclass
class MetricSet:
  reach: Metric
  k_reach: dict[int, Metric]
  impression: Metric


class DataProviderMetricSetMap(MutableMapping):

  def __init__(self, *args, **kwargs):
    self.data = dict(*args, **kwargs)

  def __getitem__(self, key):
    return self.data[key]

  def __setitem__(self, key, value):
    self.data[key] = value

  def __delitem__(self, key):
    del self.data[key]

  def __iter__(self):
    return iter(self.data)

  def __len__(self):
    return len(self.data)


@dataclass
class Report:
  weekly_cumulatives: list[DataProviderMetricSetMap]
  weekly_non_cumulatives: list[DataProviderMetricSetMap]
  whole_campaign: DataProviderMetricSetMap


class FilterReportMap(MutableMapping):

  def __init__(self, *args, **kwargs):
    self.data = dict(*args, **kwargs)

  def __getitem__(self, key):
    return self.data[key]

  def __setitem__(self, key, value):
    self.data[key] = value

  def __delitem__(self, key):
    del self.data[key]

  def __iter__(self):
    return iter(self.data)

  def __len__(self):
    return len(self.data)
