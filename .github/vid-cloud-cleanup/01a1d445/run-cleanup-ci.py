"""Small dispatcher. Every destructive mode requires the separately reviewed plan hash."""
import json,sys,os,pathlib,subprocess,shutil,hashlib,urllib.parse,time,uuid,urllib.request
import suite_control as s
import ci_graph as graph
import ci_reporting as pg
P=s.P;R=pathlib.Path(s.R);MODE=os.environ['CI_CLEANUP_MODE'];OUT=P/'noncredential-receipts'
from ci_inventory import read,sha,child,snapshot,main_check
from ci_mutation import exact_delete_private,raw_barrier,plan,execute_outside
from ci_receipts import root_artifacts
R.mkdir(exist_ok=True);OUT.mkdir(exist_ok=True)
try:
 if MODE=='inventory':
  main_check();snapshot();pg.inventory();child('cleanup-inventory-review/root-prepare-reporting-delete.py');s.api('ci-config-bucket-policy','GET','https://storage.googleapis.com/storage/v1/b/edpa-configs-storage-dev-bucket?fields=name,versioning,softDeletePolicy');snap=graph.private_inventory('after-main');graph.prepare_private_manifests(snap);graph.outside_inventory();child('outside-gcs-cleanup-prep/prepare-outside-delete-helper.py');print('INVENTORY_COMPLETE_AWAIT_ROOT_FRESH_MANIFEST_REVIEW',flush=True)
 elif MODE=='gcs-reporting':
  value=plan();main_check();snapshot();exact_delete_private('raw-done-first.json');raw_barrier(graph.private_inventory('after-raw-done'));exact_delete_private('remaining-private.json');execute_outside();pg.execute(read(P/'cleanup-inventory-review/root-reporting-owned-key-manifest.json'))
  snap=graph.private_inventory('after-gcs');assert [r['name'] for r in snap['readable']]==[graph.BOOT];snapshot();child('cleanup-inventory-review/build-exact-spanner-manifests.py');print('GCS_REPORTING_COMPLETE_AWAIT_ROOT_ACTUAL_SPANNER_MANIFEST_REVIEW',flush=True)
 elif MODE=='spanner':
  value=plan();main_check();exact_delete_private('held-bootstrap.json');snap=graph.private_inventory('final');assert snap['readable']==[];assert read(P/'cleanup-raw-done-absence.json')['readableRawDoneVersions']==0
  for name in ['cp-delete-manifest.json','edpa-delete-manifest.json','kingdom-delete-manifest.json']:
   path=P/'cleanup-final-delete'/name;journal=R/('cleanup-spanner-'+name+'-'+uuid.uuid4().hex+'.json');
   try:print(s.remote(['java','@'+s.R+'/java-delete-exact-spanner.args',str(path),sha(path),'--execute',str(journal)]),flush=True)
   finally:
    if journal.exists():s.save('cleanup-final-delete/'+journal.name,read(journal))
  child('remove-owned-template.py');child('verify-final-owned-state.py');print('EXACT_SPANNER_CLEANUP_COMPLETE',flush=True)
 elif MODE=='verify':child('verify-final-owned-state.py')
 else:raise AssertionError('Unknown mode')
finally:root_artifacts()
