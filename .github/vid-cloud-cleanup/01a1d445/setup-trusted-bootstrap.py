import pathlib,json,hashlib,subprocess,tarfile,os
P=pathlib.Path(__file__).resolve().parent;R=pathlib.Path('/tmp/vid-cloud-data-01a1d445');R.mkdir(exist_ok=True);b=json.load(open(P/'bootstrap-source.json'));archive=R/'bootstrap-bundle.tar.gz'
assert b=={'uri':'gs://secure-computation-storage-dev-bucket/test-operator-artifacts/vid-cloud-20261113/generator/bootstrap-bundle.tar.gz','generation':'1794622993228908','sha256':'96f779feb5a5d5a23523cb5ffa9dd9b34a45d3e0c9993d7f4a94c779b16a5c9a','bytes':225275187}
subprocess.run(['gcloud','storage','cp',b['uri']+'#'+b['generation'],str(archive),'--project=halo-cmm-dev'],check=True);data=archive.read_bytes();assert len(data)==b['bytes'] and hashlib.sha256(data).hexdigest()==b['sha256'];root=R/'bootstrap';root.mkdir(exist_ok=True)
with tarfile.open(archive) as tar:
 members=tar.getmembers();assert len(members)==460
 for m in members:
  assert (m.isfile() or m.isdir()) and not m.name.startswith('/') and '..' not in pathlib.PurePosixPath(m.name).parts
  assert not any(x in m.name for x in ['.ssh/','.config/gcloud/','application_default_credentials.json','credentials.db'])
 tar.extractall(root,members=members,filter='data')
for manifest in ['class-manifest.json','jar-manifest.json']:
 for row in json.load(open(root/manifest)):
  assert hashlib.sha256((root/row['relative']).read_bytes()).hexdigest()==row['sha256']
classpath=str(R)+':'+':'.join(str(root/x) for x in json.load(open(root/'classpath-relative.json')));classes=R/'compiled';classes.mkdir(exist_ok=True);classpath=str(classes)+':'+classpath
sources=[P/'CiGcpHttp.java',P/'prep-helpers/ReadScopedSql.java',P/'prep-helpers/CaptureOwnedGcsVersions.java',P/'prep-helpers/DeleteOwnedGcsVersions.java',P/'cleanup-inventory-review/DeleteExactOwnedSpannerGraph.java']
subprocess.run(['javac','--release','17','-cp',classpath,'-d',str(classes),*[str(p) for p in sources]],check=True)
for name,cls in [('java-gcp-http.args','CiGcpHttp'),('java-read-scoped-sql.args','ReadScopedSql'),('java-capture-owned-gcs.args','CaptureOwnedGcsVersions'),('java-delete-owned-gcs.args','DeleteOwnedGcsVersions'),('java-delete-exact-spanner.args','DeleteExactOwnedSpannerGraph')]:
 (R/name).write_text('-Xmx512m\n-cp\n"'+classpath+'"\n'+cls+'\n')
(R/'trusted-classpath.txt').write_text(classpath);print('TRUSTED_BOOTSTRAP_AND_REVIEWED_HELPERS_COMPILED',b['sha256'],flush=True)
