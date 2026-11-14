"""Existing pod child-JVM delivery; unchanged approved jar-key protocol, no credentials in artifacts."""
import json,pathlib,subprocess,sys,shutil,hashlib
import suite_control as s
P=s.P;R=pathlib.Path(s.R)
def prepare():
 R.mkdir(exist_ok=True)
 # Same known existing cluster. Read only public endpoint and CA fields.
 data=s.api('ci-existing-cluster-public','GET','https://container.googleapis.com/v1/projects/halo-cmm-dev/locations/-/clusters?fields=clusters(name,endpoint,masterAuth(clusterCaCertificate))')
 clusters=[c for c in data.get('clusters',[]) if c.get('endpoint')=='34.59.63.233'];assert len(clusters)==1
 c=clusters[0];plugin=R/'root-reporting-kube-adc-plugin.py';plugin.write_text("import json,subprocess\nt=subprocess.check_output(['gcloud','auth','print-access-token','--project=halo-cmm-dev'],text=True).strip()\nprint(json.dumps({'apiVersion':'client.authentication.k8s.io/v1beta1','kind':'ExecCredential','status':{'token':t}}))\n")
 config={'apiVersion':'v1','kind':'Config','clusters':[{'name':'fixed-dev','cluster':{'server':'https://'+c['endpoint'],'certificate-authority-data':c['masterAuth']['clusterCaCertificate']}}],'contexts':[{'name':'fixed-dev','context':{'cluster':'fixed-dev','user':'existing-wif','namespace':'default'}}],'current-context':'fixed-dev','users':[{'name':'existing-wif','user':{'exec':{'apiVersion':'client.authentication.k8s.io/v1beta1','command':sys.executable,'args':[str(plugin)],'interactiveMode':'Never'}}}]};(R/'root-reporting-kubeconfig.json').write_text(json.dumps(config));(R/'root-reporting-kubeconfig.json').chmod(0o600)
 sources=[P/'cleanup-inventory-review/ReportingCleanupInventory.java',P/'cleanup-inventory-review/ReportingCleanupProjection.java'];classes=R/'pg-inventory-classes';classes.mkdir(exist_ok=True);subprocess.run(['javac','--release','17','-d',str(classes),*[str(x) for x in sources]],check=True);subprocess.run(['jar','--create','--file',str(R/'root-reporting-cleanup-inventory.jar'),'-C',str(classes),'.'],check=True)
 for name in ['prepare-fresh-reporting-delivery.py','root-reporting-complete-delivery.py']:shutil.copyfile(P/'cleanup-inventory-review'/name,R/name)
 # Full ConfigMap values stay in memory; do not save or upload them.
 fresh=R/'prepare-fresh-reporting-delivery.py';text=fresh.read_text();text=text.replace(";(P/'cleanup-reporting-fresh-map-before.json').write_text(json.dumps(cm,indent=2))",'');fresh.write_text(text)
 subprocess.run([sys.executable,str(fresh)],check=True)
 return json.load(open(R/'root-reporting-complete-delivery-proof.json'))

def inventory():
 prepare()
 try:subprocess.run([sys.executable,str(R/'root-reporting-complete-delivery.py')],check=True)
 finally:
  for name in ['root-reporting-complete-delivery-proof.json','root-reporting-complete-delivery-journal.json']:
   if (R/name).exists():s.save('cleanup-inventory-review/'+name,json.load(open(R/name)))
 out=json.load(open(R/'root-reporting-live-inventory-complete.stdout.json'));assert out['read_only'] and out['scope']=='VCTqwV_vFXw/vid-cloud-20261113-'
 s.save('cleanup-inventory-review/root-reporting-live-inventory-closure.json',out)
 for name in ['root-reporting-complete-delivery-proof.json','root-reporting-complete-delivery-journal.json']:
  s.save('cleanup-inventory-review/'+name,json.load(open(R/name)))
 return out

def execute(manifest):
 # Only the reviewed static Java source embeds SQL. Same protocol as inventory, distinct key/class.
 source=P/'cleanup-inventory-review/ReportingCleanupDelete.java';classes=R/'pg-delete-classes';classes.mkdir(exist_ok=True);subprocess.run(['javac','--release','17','-d',str(classes),str(source)],check=True);jar=R/'root-reporting-cleanup-delete.jar';subprocess.run(['jar','--create','--file',str(jar),'-C',str(classes),'.'],check=True)
 proof=prepare();proof['jarSha256']=hashlib.sha256(jar.read_bytes()).hexdigest();(R/'root-reporting-delete-delivery-proof.json').write_text(json.dumps(proof))
 text=(P/'cleanup-inventory-review/root-reporting-complete-delivery.py').read_text();text=text.replace("'operation':'READ_ONLY_INVENTORY'","'operation':'EXACT_REVIEWED_DELETION'").replace(' and read-only existing pod child JVM',' and exact reviewed deletion in existing pod child JVM')
 text=text.replace('.vid-cloud-20261113-cleanup-inventory.jar','.vid-cloud-20261113-cleanup-delete.jar').replace('root-reporting-complete-delivery-proof.json','root-reporting-delete-delivery-proof.json').replace('root-reporting-complete-delivery-journal.json','root-reporting-delete-delivery-journal.json').replace('root-reporting-cleanup-inventory.jar','root-reporting-cleanup-delete.jar')
 # Projection probe class remains in the delete jar, preserving the known presence/absence check.
 subprocess.run(['javac','--release','17','-d',str(classes),str(P/'cleanup-inventory-review/ReportingCleanupProjection.java')],check=True);subprocess.run(['jar','--update','--file',str(jar),'-C',str(classes),'ReportingCleanupProjection.class'],check=True);proof['jarSha256']=hashlib.sha256(jar.read_bytes()).hexdigest();(R/'root-reporting-delete-delivery-proof.json').write_text(json.dumps(proof))
 command="'ReportingCleanupDelete','--execute',"+repr(manifest['inventorySha256'])+','+repr(manifest['sqlSha256'])
 text=text.replace("'ReportingCleanupInventory'],timeout=300)",command+'],timeout=300)')
 text=text.replace("inventory=json.loads(r.stdout);assert inventory['read_only'] is True and inventory['scope']=='VCTqwV_vFXw/vid-cloud-20261113-'","inventory=json.loads(r.stdout);assert inventory['status']=='COMMITTED' and inventory['scope']=='VCTqwV_vFXw/vid-cloud-20261113-'")
 text=text.replace("record('READ_ONLY_INVENTORY_INTENT',fixedScope='VCTqwV_vFXw/vid-cloud-20261113-',jvm='child-only',transaction='REPEATABLE READ READ ONLY; ROLLBACK; close; System.exit')","record('DELETION_COMMIT_INTENT',fixedScope='VCTqwV_vFXw/vid-cloud-20261113-',jvm='child-only',transaction='SERIALIZABLE; exact reviewed keys; COMMIT; no uncertain outcome auto-retry')")
 text=text.replace("record('INVENTORY_EXIT',returncode=r.returncode)","record('DELETION_EXIT',returncode=r.returncode,commitIntentObserved='OWNED_REPORTING_COMMIT_INTENT' in r.stderr);record('RECONCILE_REQUIRED_NO_AUTO_RETRY',reason='Nonzero exit may follow a committed transaction') if r.returncode else None")
 text=text.replace("print('INVENTORY_SUCCESS'","record('DELETION_COMMITTED_CONFIRMED');print('DELETION_SUCCESS'")
 text=text.replace('root-reporting-live-inventory-complete.stdout.json','root-reporting-delete.stdout.json').replace('root-reporting-live-inventory-complete.stderr.log','root-reporting-delete.stderr.log')
 script=R/'root-reporting-delete-delivery.py';script.write_text(text)
 try:subprocess.run([sys.executable,str(script)],check=True)
 finally:
  journal=R/'root-reporting-delete-delivery-journal.json'
  if journal.exists():s.save('cleanup-inventory-review/root-reporting-delete-delivery-journal.json',json.load(open(journal)))
 s.save('cleanup-inventory-review/root-reporting-delete-result.json',json.load(open(R/'root-reporting-delete.stdout.json')))
