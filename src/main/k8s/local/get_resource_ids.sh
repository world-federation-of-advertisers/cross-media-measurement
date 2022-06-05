#!/bin/bash
#
# Fetches id created in the process of Kingdom setup

export HALO_DATAPROVIDERS=(`kubectl logs jobs/resource-setup-job | grep 'Successfully created data provider' | awk '{print $6;}'`)
export HALO_AGGREGATORCERT=`kubectl logs jobs/resource-setup-job | grep 'Successfully created certificate duchies/aggregator' | awk '{print $5;}'`
export HALO_WORKER1CERT=`kubectl logs jobs/resource-setup-job | grep 'Successfully created certificate duchies/worker1' | awk '{print $5;}'`
export HALO_WORKER2CERT=`kubectl logs jobs/resource-setup-job | grep 'Successfully created certificate duchies/worker2' | awk '{print $5;}'`
export HALO_MC=`kubectl logs jobs/resource-setup-job | grep 'Successfully created measurement consumer' | awk '{print $6;}'`
export HALO_MC_APIKEY=`kubectl logs jobs/resource-setup-job | grep 'API key for measurement consumer' | awk '{print $8;}'`
