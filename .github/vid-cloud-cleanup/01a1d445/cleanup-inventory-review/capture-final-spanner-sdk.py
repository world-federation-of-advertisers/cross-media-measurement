"""Read-only inventory from authoritative live fixture IDs. No deletion/state changes."""
import json,sys,re
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
import suite_control as s
if len(sys.argv)!=2 or sys.argv[1] not in ['before-main','after-main','after-delete']:raise SystemExit('Choose before-main|after-main|after-delete')
phase=sys.argv[1];out=s.P/('cleanup-final-'+phase);out.mkdir(exist_ok=True)
ledger=s.load();models=sorted({v['model_line'] for v in ledger['test11_fixtures'].values()}|{'modelProviders/Wt5MH8egH4w/modelSuites/NrAN9F9SunM/modelLines/GeGgPNU3bsI'})
assert len(models)==19 and all(x.startswith('modelProviders/Wt5MH8egH4w/modelSuites/NrAN9F9SunM/modelLines/') for x in models)
array='['+','.join("'"+x+"'" for x in models)+']';dp="DataProviderResourceId='T5RryPMNong'";raw="STARTS_WITH(DoneBlobUri,'gs://secure-computation-storage-dev-bucket/raw-impressions/edp/edp7/vid-cloud-20261113/')"
ownraw='SELECT RawImpressionUploadResourceId FROM RawImpressionUpload WHERE '+dp+' AND '+raw
queries={
 'raw_uploads':'SELECT DataProviderResourceId,RawImpressionUploadId,RawImpressionUploadResourceId,DoneBlobUri,DoneBlobGeneration,CAST(State AS INT64) State,RegistrationComplete,EvictionOperationId,CorrectionCandidateId FROM RawImpressionUpload WHERE '+dp+' AND '+raw,
 'raw_models':'SELECT DataProviderResourceId,RawImpressionUploadId,RawImpressionUploadModelLineId,RawImpressionUploadModelLineResourceId,CmmsModelLine,CAST(State AS INT64) State,PendingAvailabilityDates,SynchronizedAvailabilityDates,EvictionOperationId,RecoveryAction FROM RawImpressionUploadModelLine WHERE '+dp+' AND CmmsModelLine IN UNNEST('+array+')',
 'metadata':'SELECT DataProviderResourceId,ImpressionMetadataId,ImpressionMetadataResourceId,BlobUri,CmmsModelLine,RawImpressionUploadResourceId,OutputDoneBlobGeneration,MetadataBlobGeneration,ImpressionsBlobGeneration,CAST(State AS INT64) State FROM ImpressionMetadata WHERE '+dp+' AND CmmsModelLine IN UNNEST('+array+')',
 'requisitions':"SELECT DataProviderResourceId,RequisitionMetadataId,RequisitionMetadataResourceId,CmmsRequisition,BlobUri,GroupId,Report,WorkItem,CAST(State AS INT64) State FROM RequisitionMetadata WHERE "+dp+" AND STARTS_WITH(Report,'measurementConsumers/VCTqwV_vFXw/reports/vid-cloud-20261113-')",
 'routing':"SELECT * FROM ReportRoutingDecision WHERE "+dp+" AND STARTS_WITH(Report,'measurementConsumers/VCTqwV_vFXw/reports/vid-cloud-20261113-')",
 'corrections':'SELECT DataProviderResourceId,RawImpressionUploadCorrectionCandidateId,RawImpressionUploadResourceId,UploadHealingOperationId,CAST(State AS INT64) State FROM RawImpressionUploadCorrectionCandidate WHERE '+dp+' AND RawImpressionUploadResourceId IN ('+ownraw+')',
 'healing':"SELECT DataProviderResourceId,UploadHealingOperationId,LabeledImpressionsBlobPrefix,State,ResumeState,RawImpressionUploadCorrectionCandidateIds,MutationRequestIds FROM UploadHealingOperation WHERE "+dp+" AND STRPOS(LabeledImpressionsBlobPrefix,'vid-cloud-20261113')>0",
 'fences_review_only':'SELECT DataProviderResourceId,EvictionOperationId,State,Etag FROM VidLabelingEvictionFence WHERE '+dp,
 'leases_review_only':'SELECT DataProviderResourceId,SynchronizationAttemptId,CAST(State AS INT64) State,ExpireTime,MutationRequestIds FROM DataAvailabilitySyncLease WHERE '+dp,
 'jobs':"SELECT J.DataProviderResourceId,J.RawImpressionUploadId,J.VidLabelingJobId,J.VidLabelingJobResourceId,J.CmmsModelLines,J.RawImpressionUploadFiles,CAST(J.State AS INT64) State FROM VidLabelingJob J JOIN RawImpressionUpload U USING(DataProviderResourceId,RawImpressionUploadId) WHERE U."+dp+' AND '+raw,
}
inventory={'models':models}
for key,sql in queries.items():
 inventory[key]=s.query('cleanup-'+phase+'-edpa-'+key,'edp-aggregator',sql)
 (out/'edpa-owned-live-inventory.json').write_text(json.dumps(inventory,indent=2)+'\n');print(key,len(inventory[key]),flush=True)
# These are proven direct-source and RF roots only. Labeler params closure must add canonical job-linked WIs.
rf=sorted({x['WorkItem'].split('/')[-1] for x in inventory['requisitions'] if x.get('WorkItem')});rfarray='['+','.join("'"+x+"'" for x in rf)+']'
cp=s.query('cleanup-'+phase+'-cp-direct-roots','secure-computation',"SELECT WorkItemId,WorkItemResourceId,QueueId,State,Generation,SourceDataPath,SourceObjectGeneration,SourceObjectCreateTime,SerializationKey FROM WorkItems WHERE STRPOS(SourceDataPath,'vid-cloud-20261113')>0 OR WorkItemResourceId IN UNNEST("+rfarray+")")
(out/'cp-direct-source-RF-roots.json').write_text(json.dumps(cp,indent=2)+'\n')
(out/'CP-LABELER-CLOSURE-REQUIRED.txt').write_text('Decode stored WorkItemParams and match exact canonical jobs in edpa-owned-live-inventory.json before deletion; source-path roots do NOT cover labeler WIs.\n')
print('SDK scoped EDPA inventory captured. Additional labeler/Kingdom/R2DBC closure required; no deletion authorized.',flush=True)
