import json,base64,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent));import suite_control as s
phase=sys.argv[1];assert phase in ['before-main','after-main','after-delete'];out=s.P/('cleanup-final-'+phase);out.mkdir(exist_ok=True)
old=json.load(open(s.P/'cleanup-inventory-review/kingdom-owned-live-inventory.json'));ledger=s.load();edpa=json.load(open(out/'edpa-owned-live-inventory.json'))
def ext(name):return str(int.from_bytes(base64.urlsafe_b64decode(name+'='*((-len(name))%4)),'big',signed=True))
models=sorted({v['model_line'] for v in ledger['test11_fixtures'].values()}|{ledger['functional_model_line']});ids=','.join(ext(x.split('/')[-1]) for x in models);assert len(models)==19
mp='210907887594102900';suite='8890702293917058445';dp='8635770568665047672';mc='2778309677038900604';condition='ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND ExternalModelLineId IN ('+ids+')';ownline='SELECT ModelLineId FROM ModelLines WHERE '+condition
release=old['model_release'][0]['ModelReleaseId'];shard=old['model_shard'][0]['ModelShardId'];egids=','.join(x['EventGroupId'] for x in old['event_groups'])
anchors=json.load(open(s.P/'cleanup-inventory-review/root-reporting-ownership-anchors.json'));measnames={x['apiMeasurement'] for x in anchors['measurements']};assert all(n.startswith('measurementConsumers/VCTqwV_vFXw/measurements/') for n in measnames);mids=','.join(ext(n.split('/')[-1]) for n in sorted(measnames));requisitions={r['CmmsRequisition'] for r in edpa['requisitions']};assert all(n.startswith('dataProviders/T5RryPMNong/requisitions/') for n in requisitions);rids=','.join(ext(n.split('/')[-1]) for n in requisitions);meascondition='M.MeasurementConsumerId='+mc+' AND (M.ExternalMeasurementId IN ('+mids+') OR EXISTS (SELECT 1 FROM Requisitions R WHERE R.MeasurementConsumerId=M.MeasurementConsumerId AND R.MeasurementId=M.MeasurementId AND R.DataProviderId='+dp+' AND R.ExternalRequisitionId IN ('+rids+')))'
queries={
'model_lines':'SELECT ModelProviderId,ModelSuiteId,ModelLineId,ExternalModelLineId,DisplayName,HoldbackModelLineId,Type FROM ModelLines WHERE '+condition,
'availability':'SELECT * FROM DataProviderAvailabilityIntervals WHERE DataProviderId='+dp+' AND ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND ModelLineId IN ('+ownline+')',
'rollouts':'SELECT * FROM ModelRollouts WHERE ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND ModelLineId IN ('+ownline+')',
'model_shard':'SELECT * FROM ModelShards WHERE DataProviderId='+dp+' AND ModelShardId='+shard,
'model_release':'SELECT * FROM ModelReleases WHERE ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND ModelReleaseId='+release,
'external_holdback_refs':'SELECT ModelProviderId,ModelSuiteId,ModelLineId,HoldbackModelLineId FROM ModelLines WHERE ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND HoldbackModelLineId IN ('+ownline+') AND ModelLineId NOT IN ('+ownline+')',
'external_release_rollouts':'SELECT ModelProviderId,ModelSuiteId,ModelLineId,ModelRolloutId FROM ModelRollouts WHERE ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND ModelReleaseId='+release+' AND ModelLineId NOT IN ('+ownline+')',
'external_previous_rollout_refs':'SELECT ModelProviderId,ModelSuiteId,ModelLineId,ModelRolloutId,PreviousModelRolloutId FROM ModelRollouts WHERE ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND PreviousModelRolloutId IN (SELECT ModelRolloutId FROM ModelRollouts WHERE ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND ModelLineId IN ('+ownline+')) AND ModelLineId NOT IN ('+ownline+')',
'external_shard_release_refs':'SELECT DataProviderId,ModelShardId,ModelReleaseId FROM ModelShards WHERE ModelProviderId='+mp+' AND ModelSuiteId='+suite+' AND ModelReleaseId='+release+' AND NOT (DataProviderId='+dp+' AND ModelShardId='+shard+')',
'event_groups':'SELECT DataProviderId,EventGroupId,MeasurementConsumerId,ExternalEventGroupId,ProvidedEventGroupId,EntityType,EntityId,State FROM EventGroups WHERE DataProviderId='+dp+' AND EventGroupId IN ('+egids+')',
'activities':'SELECT * FROM EventGroupActivities WHERE DataProviderId='+dp+' AND EventGroupId IN ('+egids+')',
'measurements':'SELECT M.MeasurementConsumerId,M.MeasurementId,M.ExternalMeasurementId,M.ExternalComputationId,M.ProvidedMeasurementId,M.State,M.CreateRequestId FROM Measurements M WHERE '+meascondition,
'protected_shared_model_release':'SELECT ModelProviderId,ModelSuiteId,ModelReleaseId,ExternalModelReleaseId,PopulationDataProviderId,PopulationId FROM ModelReleases WHERE ExternalModelReleaseId='+ext('HG2qTCjS5WQ'),
'protected_shared_population':'SELECT DataProviderId,PopulationId,ExternalPopulationId FROM Populations WHERE ExternalPopulationId='+ext('a6OJ86CtSjw')}
a={'ownedModels':models,'ownedMeasurementsApi':sorted(measnames),'queries':queries}
for label,sql in queries.items():
 a[label]=s.query('cleanup-'+phase+'-kingdom-'+label,'kingdom',sql);(out/'kingdom-owned-live-inventory.json').write_text(json.dumps(a,indent=2)+'\n');print(label,len(a[label]),flush=True)
assert not a['external_holdback_refs'] and not a['external_release_rollouts'] and not a['external_shard_release_refs'] and not a['external_previous_rollout_refs'];assert all(x['ExternalComputationId'] is None for x in a['measurements']);print('KINGDOM_CAPTURE_COMPLETE',flush=True)
