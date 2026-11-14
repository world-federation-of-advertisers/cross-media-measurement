"""Correlate captured Reporting rows with this task's existing request/API evidence; local reads only."""
import json,pathlib
P=pathlib.Path(__file__).parent;RUN=P.parent
x=json.load(open(P/'root-reporting-live-inventory-closure.json'));ledger=json.load(open(RUN/'ledger.json'))
request_index={};responses={}
for f in RUN.glob('*.json'):
 name=f.name
 if name.endswith(('create-report-request.json','create-report.json')):
  a=json.load(open(f))
  if isinstance(a,dict) and a.get('reportId') and a.get('requestId'):request_index.setdefault(a['reportId'],[]).append({'file':name,'requestId':a['requestId'],'parent':a.get('parent')})
 if 'get-report' in name or name.endswith(('report-current.json','report-create-result.json','report-executor.json')):
  a=json.load(open(f))
  if isinstance(a,dict) and a.get('name','').startswith('measurementConsumers/VCTqwV_vFXw/reports/vid-cloud-20261113-'):responses.setdefault(a['name'],[]).append((name,a))
# Test 9's actual API names are preserved in its completed execution ledger.
t9=ledger['tests']['9'];request_index.setdefault('vid-cloud-20261113-test9',[]).append({'file':'ledger.json/tests/9/report_intent','requestId':t9['report_intent']['request_id'],'parent':'measurementConsumers/VCTqwV_vFXw'})
report_by_id={r['reportid']:r for r in x['reports']};metric_by_id={r['metricid']:r for r in x['metrics']};measurement_by_id={r['measurementid']:r for r in x['measurements']}
anchors={'method':'Exact task CreateReport request UUID + actual API report/metric/measurement names + live internal link tuples; no timestamp ownership predicates','reports':[],'metrics':[],'measurements':[],'gaps':[]}
for r in x['reports']:
 api='measurementConsumers/VCTqwV_vFXw/reports/'+r['externalreportid'];matches=[m for m in request_index.get(r['externalreportid'],[]) if m['requestId']==r['createreportrequestid'] and m['parent']=='measurementConsumers/VCTqwV_vFXw']
 result_files=[f for f,a in responses.get(api,[])]
 if not matches or not result_files:anchors['gaps'].append({'report':api,'requestMatched':bool(matches),'responseMatched':bool(result_files)})
 anchors['reports'].append({'internalReportId':str(r['reportid']),'apiReport':api,'exactCreateReportRequestId':r['createreportrequestid'],'taskRequestEvidence':matches,'apiResponseEvidence':result_files})
for m in x['metrics']:
 api='measurementConsumers/VCTqwV_vFXw/metrics/'+m['externalmetricid'];links=[r for r in x['report_metric_links'] if r['metricid']==m['metricid']];files=[];public_measurements=[]
 for link in links:
  report=report_by_id[link['reportid']];reportapi='measurementConsumers/VCTqwV_vFXw/reports/'+report['externalreportid']
  for f,a in responses.get(reportapi,[]):
   for result in a.get('metricCalculationResults',[]):
    for attrs in result.get('resultAttributes',[]):
     if attrs.get('metric')==api:
      files.append(f);public_measurements+=attrs.get('metricResult',{}).get('cmmsMeasurements',[])
  if report['externalreportid']=='vid-cloud-20261113-test9' and api in t9['metric_names']:
   files.append('ledger.json/tests/9/metric_names');public_measurements+=t9['measurement_names']
 if not links or not files:anchors['gaps'].append({'metric':api,'liveOwnReportLink':bool(links),'actualApiNameMatched':bool(files)})
 anchors['metrics'].append({'internalMetricId':str(m['metricid']),'apiMetric':api,'liveReportLinkKeys':[{k:str(v) if isinstance(v,int) else v for k,v in r.items()} for r in links],'apiResponseEvidence':sorted(set(files))})
 for mm in x['metric_measurements']:
  if mm['metricid']!=m['metricid']:continue
  meas=measurement_by_id[mm['measurementid']];measapi='measurementConsumers/VCTqwV_vFXw/measurements/'+meas['cmmsmeasurementid'];matched=measapi in public_measurements
  if not matched:anchors['gaps'].append({'measurement':measapi,'actualApiNameMatched':False})
  anchors['measurements'].append({'internalMeasurementId':str(meas['measurementid']),'apiMeasurement':measapi,'cmmsCreateMeasurementRequestId':meas['cmmscreatemeasurementrequestid'],'internalMetricId':str(m['metricid']),'actualApiNameMatched':matched,'apiResponseEvidence':sorted(set(files))})
(P/'root-reporting-ownership-anchors.json').write_text(json.dumps(anchors,indent=2)+'\n');print('ANCHORS',len(anchors['reports']),len(anchors['metrics']),len(anchors['measurements']),'GAPS',anchors['gaps'])
assert anchors['gaps']==[]
