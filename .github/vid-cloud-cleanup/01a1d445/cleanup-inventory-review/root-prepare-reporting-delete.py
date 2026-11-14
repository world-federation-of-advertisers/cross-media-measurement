"""Prepare exact captured-key SQL only; this file makes no network or database calls."""
import hashlib,json,pathlib,re
P=pathlib.Path(__file__).parent
x=json.load(open(P/'root-reporting-live-inventory-closure.json'))
MC=1721932089996313930
assert x['read_only'] is True and x['scope']=='VCTqwV_vFXw/vid-cloud-20261113-'
import subprocess,sys
subprocess.run([sys.executable,str(P/'root-reporting-ownership-anchors.py')],check=True)
anchors=json.load(open(P/'root-reporting-ownership-anchors.json'));assert not anchors['gaps']
def q(v):
 if v is None:return 'NULL'
 if isinstance(v,bool):return 'TRUE' if v else 'FALSE'
 if isinstance(v,int):return str(v)
 return "'"+str(v).replace("'","''")+"'"
roots={'reports':('Reports','reportid',['externalreportid','createreportrequestid']),
 'metrics':('Metrics','metricid',['externalmetricid','reportingsetid']),
 'measurements':('Measurements','measurementid',['cmmscreatemeasurementrequestid','cmmsmeasurementid']),
 'metric_specs':('MetricCalculationSpecs','metriccalculationspecid',['externalmetriccalculationspecid']),
 'reporting_sets':('ReportingSets','reportingsetid',['externalreportingsetid']),
 'event_groups':('EventGroups','eventgroupid',['cmmsdataproviderid','cmmseventgroupid'])}
for key in roots:
 assert all(str(r['measurementconsumerid'])==str(MC) for r in x[key])
 assert x[key],key
sets={key:{int(r[col]) for r in x[key]} for key,(table,col,identity) in roots.items()}
def ids(key):return ','.join(map(str,sorted(sets[key])))
def pred(key,alias=''):
 col=roots[key][1];prefix=alias+'.' if alias else ''
 return f'{prefix}MeasurementConsumerId={MC} AND {prefix}{col} IN ({ids(key)})'
def within(key,col):return col+' IN ('+ids(key)+')'
def outside(key,col):return col+' NOT IN ('+ids(key)+')'
baseids=sorted({r['primitivereportingsetbasisid'] for r in x['primitive_bases']})
bases=','.join(map(str,baseids)) or 'NULL'
guards={
 'shared_metric_refs':f"SELECT 1 FROM MetricCalculationSpecReportingMetrics WHERE MeasurementConsumerId={MC} AND {within('metrics','MetricId')} AND {outside('reports','ReportId')}",
 'shared_spec_refs':f"SELECT 1 FROM MetricCalculationSpecReportingMetrics WHERE MeasurementConsumerId={MC} AND {within('metric_specs','MetricCalculationSpecId')} AND {outside('reports','ReportId')}",
 'unowned_metrics_using_own_sets':f"SELECT 1 FROM Metrics WHERE MeasurementConsumerId={MC} AND {within('reporting_sets','ReportingSetId')} AND {outside('metrics','MetricId')}",
 'shared_measurement_refs':f"SELECT 1 FROM MetricMeasurements WHERE MeasurementConsumerId={MC} AND {within('measurements','MeasurementId')} AND {outside('metrics','MetricId')}",
 'shared_set_report_metric_refs':f"SELECT 1 FROM MetricCalculationSpecReportingMetrics WHERE MeasurementConsumerId={MC} AND {within('reporting_sets','ReportingSetId')} AND {outside('reports','ReportId')}",
 'shared_set_spec_campaign_refs':f"SELECT 1 FROM MetricCalculationSpecs WHERE MeasurementConsumerId={MC} AND {within('reporting_sets','CampaignGroupId')} AND {outside('metric_specs','MetricCalculationSpecId')}",
 'shared_set_campaign_refs':f"SELECT 1 FROM ReportingSets WHERE MeasurementConsumerId={MC} AND {within('reporting_sets','CampaignGroupId')} AND {outside('reporting_sets','ReportingSetId')}",
 'shared_set_expression_refs':f"SELECT 1 FROM SetExpressions WHERE MeasurementConsumerId={MC} AND {outside('reporting_sets','ReportingSetId')} AND ({within('reporting_sets','LeftHandReportingSetId')} OR {within('reporting_sets','RightHandReportingSetId')})",
 'shared_basis_weighted_refs':f"SELECT 1 FROM WeightedSubsetUnionPrimitiveReportingSetBases WHERE MeasurementConsumerId={MC} AND PrimitiveReportingSetBasisId IN ({bases}) AND {outside('reporting_sets','ReportingSetId')}",
 'shared_basis_measurement_refs':f"SELECT 1 FROM MeasurementPrimitiveReportingSetBases WHERE MeasurementConsumerId={MC} AND PrimitiveReportingSetBasisId IN ({bases}) AND {outside('measurements','MeasurementId')}",
 'shared_cached_event_group_set_refs':f"SELECT 1 FROM ReportingSetEventGroups WHERE MeasurementConsumerId={MC} AND {within('event_groups','EventGroupId')} AND {outside('reporting_sets','ReportingSetId')}"}
assert all(k in x and x[k]==[] for k in guards),'Live shared reference guard is not empty'
assert x['owned_schedules']==[],'Unexpected owned schedule scope requires separate review'
assert x['report_schedule_links']==[],'Unexpected own report/schedule links require separate review'
children={
 'report_metric_links':('MetricCalculationSpecReportingMetrics',f'{pred("reports")}'),
 'metric_measurements':('MetricMeasurements',f'{pred("metrics")}'),
 'set_expressions':('SetExpressions',f'{pred("reporting_sets")}'),
 'primitive_bases':('PrimitiveReportingSetBases',f'MeasurementConsumerId={MC} AND PrimitiveReportingSetId IN ({ids("reporting_sets")})'),
 'basis_filters':('PrimitiveReportingSetBasisFilters',f'MeasurementConsumerId={MC} AND PrimitiveReportingSetBasisId IN ({bases})'),
 'weighted_subset_unions':('WeightedSubsetUnions',f'{pred("reporting_sets")}'),
 'weighted_union_basis_links':('WeightedSubsetUnionPrimitiveReportingSetBases',f'MeasurementConsumerId={MC} AND (ReportingSetId IN ({ids("reporting_sets")}) OR PrimitiveReportingSetBasisId IN ({bases}))'),
 'measurement_basis_links':('MeasurementPrimitiveReportingSetBases',f'MeasurementConsumerId={MC} AND (MeasurementId IN ({ids("measurements")}) OR PrimitiveReportingSetBasisId IN ({bases}))'),
 'set_event_group_links':('ReportingSetEventGroups',f'MeasurementConsumerId={MC} AND (ReportingSetId IN ({ids("reporting_sets")}) OR EventGroupId IN ({ids("event_groups")}))')}
primary={}
for c in x['constraints']:
 if c['constraint_type']=='p':
  m=re.fullmatch(r'PRIMARY KEY \(([^)]+)\)',c['definition']);assert m,c
  primary[c['table_name']]=[s.strip() for s in m.group(1).split(',')]
lines=['DO $owned_cleanup$','DECLARE changed bigint; preserved_before jsonb; preserved_after jsonb;','BEGIN',
 f"  IF current_database()<>'reporting-v2' OR current_user<>'reporting-v2-internal@halo-cmm-dev.iam' OR (SELECT MeasurementConsumerId FROM MeasurementConsumers WHERE CmmsMeasurementConsumerId='VCTqwV_vFXw') IS DISTINCT FROM {MC} THEN RAISE EXCEPTION 'Identity/database/MC mismatch'; END IF;"]
def fail_if(condition,label):lines.append(f"  IF {condition} THEN RAISE EXCEPTION {q(label)}; END IF;")
for key,(table,col,identity) in roots.items():
 lines.append(f'  PERFORM {col} FROM {table} WHERE {pred(key)} ORDER BY {col} FOR UPDATE;')
 fail_if(f'(SELECT count(*) FROM {table} WHERE {pred(key)})<>{len(x[key])}','Captured parent missing: '+key)
 expected=' OR '.join('('+ ' AND '.join(f'{c} IS NOT DISTINCT FROM {q(r[c])}' for c in [col]+identity)+')' for r in x[key])
 fail_if(f'EXISTS (SELECT 1 FROM {table} WHERE {pred(key)} AND NOT ({expected}))','Captured parent identity changed: '+key)
for key,col in [('reports','ExternalReportId'),('reporting_sets','ExternalReportingSetId'),('metric_specs','ExternalMetricCalculationSpecId')]:
 table=roots[key][0]
 fail_if(f"EXISTS (SELECT 1 FROM {table} WHERE MeasurementConsumerId={MC} AND STARTS_WITH({col},'vid-cloud-20261113-') AND {outside(key,roots[key][1])})",'Uncaptured owned parent: '+key)
fail_if(f"EXISTS (SELECT 1 FROM Metrics WHERE MeasurementConsumerId={MC} AND STARTS_WITH(ExternalMetricId,'vid-cloud-20261113-') AND {outside('metrics','MetricId')})",'Uncaptured owned direct metric')
fail_if(f'EXISTS (SELECT 1 FROM Metrics WHERE {pred("metrics")} AND State NOT IN (2,3))','Owned metric is not terminal')
fail_if(f'EXISTS (SELECT 1 FROM Measurements WHERE {pred("measurements")} AND State NOT IN (2,3))','Owned measurement is not terminal')
constraints=x['constraints'];expected=' OR '.join('('+ ' AND '.join(f'{c} IS NOT DISTINCT FROM {q(r[k])}' for c,k in [('conrelid::regclass::text','table_name'),('conname','constraint_name'),('contype::text','constraint_type'),('convalidated','convalidated'),('condeferrable','condeferrable'),('condeferred','condeferred'),('pg_get_constraintdef(oid)','definition')])+')' for r in constraints)
fail_if(f"(SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace)<>{len(constraints)} OR EXISTS (SELECT 1 FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT ({expected}))",'Runtime constraint catalog changed')
for name,sql in guards.items():fail_if('EXISTS ('+sql+')','Shared reference: '+name)
fail_if(f'EXISTS (SELECT 1 FROM ReportsReportSchedules WHERE {pred("reports")})','Unexpected own report schedule link')
fail_if(f"EXISTS (SELECT 1 FROM ReportSchedules WHERE MeasurementConsumerId={MC} AND STARTS_WITH(ExternalReportScheduleId,'vid-cloud-20261113-'))",'Uncaptured owned schedule')
for key,(table,where) in children.items():
 pk=primary[table.lower()];rows=x[key]
 fail_if(f'(SELECT count(*) FROM {table} WHERE {where})<>{len(rows)}','Captured child count changed: '+key)
 if rows:
  expected=' OR '.join('('+ ' AND '.join(f'{c} IS NOT DISTINCT FROM {q(r[c])}' for c in pk)+')' for r in rows)
  fail_if(f'EXISTS (SELECT 1 FROM {table} WHERE {where} AND NOT ({expected}))','Captured child keys changed: '+key)
def fingerprints():
 items=[]
 for key,(table,col,identity) in roots.items():
  items.append(f"SELECT {q(key)} AS entity,count(*) AS count,md5(COALESCE(string_agg(row_to_json(t)::text,E'\\n' ORDER BY {col}),'')) AS fingerprint FROM {table} t WHERE MeasurementConsumerId={MC} AND {outside(key,col)}")
 return 'SELECT jsonb_agg(to_jsonb(u) ORDER BY entity) FROM ('+' UNION ALL '.join(items)+') u'
lines.append('  SELECT result INTO preserved_before FROM ('+fingerprints()+') f(result);')
for key,(table,col,identity) in roots.items():
 lines.append(f'  DELETE FROM {table} WHERE {pred(key)};')
 lines.append('  GET DIAGNOSTICS changed = ROW_COUNT;')
 fail_if(f'changed<>{len(x[key])}','Delete count mismatch: '+key)
for key,(table,col,identity) in roots.items():fail_if(f'EXISTS (SELECT 1 FROM {table} WHERE {pred(key)})','Owned parent remains: '+key)
for key,(table,where) in children.items():fail_if(f'EXISTS (SELECT 1 FROM {table} WHERE {where})','Owned child remains: '+key)
lines.append('  SELECT result INTO preserved_after FROM ('+fingerprints()+') f(result);')
fail_if('preserved_before IS DISTINCT FROM preserved_after','Unowned parent fingerprint changed')
lines+=['END;','$owned_cleanup$;']
do='\n'.join(lines)
full="-- PREPARATION ONLY. Refresh/review after all tests and main before execution.\nBEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;\nSET LOCAL lock_timeout='10s';\nSET LOCAL statement_timeout='60s';\n"+do+'\nCOMMIT;\n'
(P/'root-reporting-scoped-delete.sql').write_text(full)
manifest={'scope':x['scope'],'measurementConsumerId':str(MC),'requiresRefreshAfterMainBeforeDelete':True,'inventorySha256':hashlib.sha256((P/'root-reporting-live-inventory-closure.json').read_bytes()).hexdigest(),'sqlSha256':hashlib.sha256(full.encode()).hexdigest(),'guards':{k:len(x[k]) for k in guards},'unownedReferenceHashes':{k:hashlib.sha256(json.dumps(x[k],sort_keys=True,separators=(',',':')).encode()).hexdigest() for k in guards},'parents':{},'children':{},'constraints':constraints}
for key in roots:manifest['parents'][key]=[{k:str(v) if k.endswith('id') and isinstance(v,int) else v for k,v in r.items()} for r in x[key]]
for key in children:manifest['children'][key]=[{k:str(v) if k.endswith('id') and isinstance(v,int) else v for k,v in r.items()} for r in x[key]]
(P/'root-reporting-owned-key-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
# Reuse the already verified SDK-free reflection connector boilerplate. All SQL is embedded.
s=(P/'ReportingCleanupInventory.java').read_text();s=s[:s.index('  private static void inventory(String[] args)')].replace('ReportingCleanupInventory','ReportingCleanupDelete')
snapshot_sha=manifest['inventorySha256'];sql_sha=manifest['sqlSha256']
execution_guard=(
    'if (args.length != 3 || !args[0].equals("--execute")\n'
    '          || !args[1].equals(EXPECTED_SNAPSHOT_SHA256)\n'
    '          || !args[2].equals(EXPECTED_SQL_SHA256)) {\n'
    '        throw new IllegalArgumentException("Required: --execute <reviewed snapshot SHA256> <reviewed SQL SHA256>");\n'
    '      }\n'
    '      cleanup(new String[0]);')
s=s.replace('inventory(args);',execution_guard)
s+='  private static final String EXPECTED_SNAPSHOT_SHA256 = '+json.dumps(snapshot_sha)+';\n'
s+='  private static final String EXPECTED_SQL_SHA256 = '+json.dumps(sql_sha)+';\n'
manifest['executionModeGuard']={'requiredArgs':['--execute',snapshot_sha,sql_sha],'connectionSetupAfterGuard':True,'arbitrarySqlOrKeysAccepted':False}
manifest['ownershipAnchors']={'artifact':'root-reporting-ownership-anchors.json','sha256':hashlib.sha256((P/'root-reporting-ownership-anchors.json').read_bytes()).hexdigest(),'method':anchors['method'],'reportCount':len(anchors['reports']),'metricCount':len(anchors['metrics']),'measurementCount':len(anchors['measurements'])}
(P/'root-reporting-owned-key-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
src=(P/'ReportingCleanupInventory.java').read_text();setup=src[src.index('    if(args.length != 0)'):src.index('    try {\n      query(connection,"BEGIN TRANSACTION')].replace('This helper only inventories fixed owned scope','This helper deletes only reviewed captured owned keys')
body='''    try {
      query(connection,"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
      query(connection,"SET LOCAL lock_timeout='10s'");
      query(connection,"SET LOCAL statement_timeout='60s'");
      query(connection,SQL);
      System.err.println("OWNED_REPORTING_COMMIT_INTENT");
      System.err.flush();
      query(connection,"COMMIT");
      System.out.println("{\\\"status\\\":\\\"COMMITTED\\\",\\\"scope\\\":\\\"VCTqwV_vFXw/vid-cloud-20261113-\\\"}");
    } catch (Throwable failure) {
      try { query(connection,"ROLLBACK"); } catch (Throwable rollback) { failure.addSuppressed(rollback); }
      throw failure;
    } finally { await(cls("io.r2dbc.spi.Connection").getMethod("close").invoke(connection)); }
  }
}
'''
s+='  private static final String SQL = String.join(\"\", new String[]{'+','.join(json.dumps(do[i:i+8000]) for i in range(0,len(do),8000))+'});\n  private static void cleanup(String[] args) throws Exception {\n'+setup+body
s=s.replace('/** Read-only inventory using the already deployed server\'s R2DBC Cloud SQL connector. */','/** PREPARATION ONLY: exact reviewed-key cleanup through the deployed R2DBC connector. */')
(P/'ReportingCleanupDelete.java').write_text(s)
print('PREPARED ONLY',len(full),'SQL bytes; guards',len(guards),'parents',sum(len(x[k]) for k in roots))
