#!/bin/bash
#
# Copyright 2022 The Cross-Media Measurement Authors
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

# Fetches ids created in the process of Kingdom setup
#

export HALO_DATAPROVIDERS=(`kubectl logs jobs/resource-setup-job | grep 'Successfully created data provider' | awk '{print $6;}'`)
export HALO_AGGREGATORCERT=`kubectl logs jobs/resource-setup-job | grep 'Successfully created certificate duchies/aggregator' | awk '{print $5;}'`
export HALO_WORKER1CERT=`kubectl logs jobs/resource-setup-job | grep 'Successfully created certificate duchies/worker1' | awk '{print $5;}'`
export HALO_WORKER2CERT=`kubectl logs jobs/resource-setup-job | grep 'Successfully created certificate duchies/worker2' | awk '{print $5;}'`
export HALO_MC=`kubectl logs jobs/resource-setup-job | grep 'Successfully created measurement consumer' | awk '{print $6;}'`
export HALO_MC_APIKEY=`kubectl logs jobs/resource-setup-job | grep 'API key for measurement consumer' | awk '{print $8;}'`
