"""Preparation only: remove the exact old task template after all main reference checks."""
import json, time, uuid, urllib.request
import suite_control as s

NAME = 'vid-cloud-cutover-rf-20261113-01a1d445'
ID = '6208465045749902534'
BASE = 'https://compute.googleapis.com/compute/v1/projects/halo-cmm-dev/'
MIG = BASE + 'regions/us-central1/instanceGroupManagers/results-fulfiller-mig-v2'

manager = s.api('ci-owned-template-main-manager', 'GET', MIG)
assert NAME not in json.dumps(manager)
members = s.api('ci-owned-template-running-members', 'POST', MIG + '/listManagedInstances', {})
assert not members.get('nextPageToken'),'Uninspected member page preserved; template not deleted'
assert NAME not in json.dumps(members)
url = BASE + 'global/instanceTemplates/' + NAME
try:
    template = s.api('ci-owned-template-identity', 'GET', url + '?fields=name,id')
except RuntimeError as error:
    assert '404' in str(error)
    s.save('cleanup-owned-template-removal.json', {'status': 'ALREADY_ABSENT', 'name': NAME, 'id': ID})
else:
    assert template['name'] == NAME and template['id'] == ID
    request_id = str(uuid.uuid4())
    s.intent('remove-exact-owned-unused-template', {'name': NAME, 'id': ID, 'requestId': request_id})
    s.save('cleanup-owned-template-delete-intent.json', {'name': NAME, 'id': ID,
                                                      'requestId': request_id})
    request = urllib.request.Request(url + '?requestId=' + request_id, method='DELETE',
                                     headers={'Authorization': 'Bearer ' + s.token()})
    with urllib.request.urlopen(request, timeout=90) as response:
        operation = json.load(response)
    s.save('cleanup-owned-template-delete-operation.json', operation)
    while operation['status'] != 'DONE':
        time.sleep(5)
        operation = s.api('ci-owned-template-delete-operation', 'GET',
                         operation['selfLink'].replace('https://www.googleapis.com/compute/',
                                                       'https://compute.googleapis.com/compute/'))
    assert not operation.get('error')
    try:
        s.api('ci-owned-template-confirm-absent', 'GET', url + '?fields=name,id')
    except RuntimeError as error:
        assert '404' in str(error)
    else:
        raise AssertionError('Owned template remains')
    s.save('cleanup-owned-template-removal.json', {'status': 'PASS', 'name': NAME, 'id': ID,
                                                'noManagerOrRunningMemberReference': True})
